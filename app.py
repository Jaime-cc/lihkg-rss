from fastapi import FastAPI, Response
import requests
from feedgen.feed import FeedGenerator
from datetime import datetime, timezone
import time
import os

app = FastAPI()

BASE = "https://lihkg.com/api_v2"

# ===============================================================
# 只要改呢一行：
# 把 Chrome DevTools -> Cookies -> cf_clearance 的 Value 貼入
# （只貼 Value，不要加 cf_clearance=）
# ===============================================================
CF_CLEARANCE_VALUE = "Wyxf5yaiA1xJ2.z7V_YVbgFIayxqYe4bvk7wBr1Sav8-1770526290-1.2.1.1-xkAB9JFQGycUaozI1unshKil2lhXcmAS7lPG05eahDO_3YCDvuNthuXiskTsPR4ivm6AUoRCLBIEboFyWxnWAfzKitb_xMCm.qh8xXan0M13_JrQzx.MqzRvXQGTZ6KaGCghJJwfeIJEB1aYFroWqStm2gvvmRyL87dh7Mpbu4Ec07t_xpYdk3gAU7.BwEi9QYpczfPjG._EZygDWccQFcTECcxA4.mB.HsSqa7VC.c"

# ===============================================================
# 上線策略：
# - /rss 永遠只回「last_good/seed」，唔會主動打 LIHKG（避免 429）
# - 你要更新先用 /refresh 或 /refresh_all
# ===============================================================
MIN_CALL_INTERVAL_SEC = 8.0     # 全局節流（每次打上游最少相隔秒數）
REFRESH_GAP_SEC = 5.0           # refresh_all 每台間隔（避免 429）
DEFAULT_PAGE = 1
DEFAULT_COUNT = 60

_last_call_ts = 0.0
last_good_xml = {}   # (cat_id, page) -> xml
last_good_ts = {}    # (cat_id, page) -> unix ts


def _rate_limit():
    global _last_call_ts
    now = time.time()
    wait = MIN_CALL_INTERVAL_SEC - (now - _last_call_ts)
    if wait > 0:
        time.sleep(wait)
    _last_call_ts = time.time()


def thread_url(thread_id: str) -> str:
    return f"https://lihkg.com/thread/{thread_id}"


def to_rfc2822(ts: int) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%a, %d %b %Y %H:%M:%S %z")


def build_empty_rss(cat_id: int, page: int, reason: str) -> str:
    fg = FeedGenerator()
    fg.id(f"lihkg-cat-{cat_id}-p{page}")
    fg.title(f"LIHKG Category {cat_id} ({reason})")
    fg.link(href=f"https://lihkg.com/category/{cat_id}", rel="alternate")
    fg.description("This feed serves cached/seed content. Use /refresh or /refresh_all to update.")
    fg.lastBuildDate(datetime.now(timezone.utc))
    return fg.rss_str(pretty=True).decode("utf-8")


def fetch_threads(cat_id: int, page: int = 1, count: int = 60):
    """
    向 LIHKG 抓 threads（只喺 refresh 時用）：
    - 全局節流
    - 429 退避重試
    """
    if "PASTE_" in CF_CLEARANCE_VALUE or len(CF_CLEARANCE_VALUE) < 10:
        return None, "missing_cf_clearance"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://lihkg.com/category/{cat_id}",
        "Origin": "https://lihkg.com",
        "X-Requested-With": "XMLHttpRequest",
        "Cookie": f"cf_clearance={CF_CLEARANCE_VALUE}",
    }

    url = f"{BASE}/thread/category"
    params = {"cat_id": cat_id, "page": page, "count": count}

    backoff = 8
    for _ in range(3):
        _rate_limit()
        try:
            r = requests.get(url, params=params, headers=headers, timeout=25)
        except Exception:
            return None, "network_error"

        if r.status_code == 200:
            try:
                data = r.json()
            except Exception:
                return None, "json_parse_error"

            if data.get("success") != 1:
                return None, "upstream_success!=1"

            items = data.get("response", {}).get("items", [])
            return (items if isinstance(items, list) else []), "ok"

        if r.status_code == 429:
            time.sleep(backoff)
            backoff *= 2
            continue

        if r.status_code == 403:
            return None, "upstream_403_cookie_expired"

        return None, f"upstream_http_{r.status_code}"

    return None, "upstream_429_rate_limited"


def build_rss(cat_id: int, page: int, threads: list) -> str:
    fg = FeedGenerator()
    fg.id(f"lihkg-cat-{cat_id}-p{page}")
    fg.title(f"LIHKG Category {cat_id} (Latest)")
    fg.link(href=f"https://lihkg.com/category/{cat_id}", rel="alternate")
    fg.description("Unofficial RSS for LIHKG. Served from cache; use /refresh to update.")
    fg.lastBuildDate(datetime.now(timezone.utc))

    for t in threads:
        thread_id = str(t.get("thread_id", "")).strip()
        title = (t.get("title") or "").strip()
        if not thread_id or not title:
            continue

        link = thread_url(thread_id)
        fe = fg.add_entry()
        fe.id(thread_id)
        fe.title(title)
        fe.link(href=link)

        author = t.get("user_nickname") or t.get("user_name") or "unknown"
        fe.author({"name": str(author)})

        ts = t.get("create_time") or t.get("last_reply_time") or 0
        if isinstance(ts, int) and ts > 0:
            fe.pubDate(to_rfc2822(ts))

        reply_count = t.get("reply_count")
        like_count = t.get("like_count")
        dislike_count = t.get("dislike_count")
        fe.description(f"Replies: {reply_count} | Likes: {like_count} | Dislikes: {dislike_count}<br>{link}")

    return fg.rss_str(pretty=True).decode("utf-8")


def load_seed():
    """
    可選：用 seed_cat5.xml 先填入 cat_id=5，避免一開始無 last_good
    """
    seed_path = os.path.join(os.path.dirname(__file__), "seed_cat5.xml")
    if os.path.exists(seed_path):
        with open(seed_path, "r", encoding="utf-8") as f:
            xml = f.read().strip()
            if xml.startswith("<?xml") and "<rss" in xml:
                key = (5, 1)
                last_good_xml[key] = xml
                last_good_ts[key] = time.time()


load_seed()


@app.get("/")
def home():
    return {
        "ok": True,
        "mode": "cache_only_rss_manual_refresh",
        "usage": [
            "/rss?cat_id=5",
            "/status?cat_id=5",
            "/refresh?cat_id=5",
            "/refresh_all"
        ],
        "note": "RSS will NOT hit LIHKG. Use /refresh or /refresh_all manually to update and avoid 429."
    }


@app.get("/status")
def status(cat_id: int, page: int = 1):
    key = (cat_id, page)
    ts = last_good_ts.get(key)
    return {
        "cat_id": cat_id,
        "page": page,
        "has_last_good": key in last_good_xml,
        "last_good_utc": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None,
    }


@app.get("/rss")
def rss(cat_id: int, page: int = DEFAULT_PAGE):
    key = (cat_id, page)
    if key in last_good_xml:
        return Response(content=last_good_xml[key], media_type="application/rss+xml; charset=utf-8")

    # 從未成功/冇 seed：都回 XML（唔會 502）
    xml = build_empty_rss(cat_id, page, "no_cache_yet")
    return Response(content=xml, media_type="application/rss+xml; charset=utf-8")


@app.get("/refresh")
def refresh(cat_id: int, page: int = DEFAULT_PAGE, count: int = DEFAULT_COUNT):
    key = (cat_id, page)
    threads, reason = fetch_threads(cat_id=cat_id, page=page, count=count)

    if isinstance(threads, list):
        xml = build_rss(cat_id, page, threads)
        last_good_xml[key] = xml
        last_good_ts[key] = time.time()
        return {"ok": True, "reason": reason, "updated": True, "cat_id": cat_id}

    return {"ok": False, "reason": reason, "updated": False, "cat_id": cat_id}


@app.get("/refresh_all")
def refresh_all():
    """
    一鍵更新 OPML 內現有 10 台：
    1,4,5,6,7,8,9,10,11,12
    """
    cat_ids = [1, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    results = []

    for cid in cat_ids:
        threads, reason = fetch_threads(cat_id=cid, page=DEFAULT_PAGE, count=DEFAULT_COUNT)
        key = (cid, DEFAULT_PAGE)

        if isinstance(threads, list):
            xml = build_rss(cid, DEFAULT_PAGE, threads)
            last_good_xml[key] = xml
            last_good_ts[key] = time.time()
            results.append({"cat_id": cid, "ok": True})
        else:
            results.append({"cat_id": cid, "ok": False, "reason": reason})

        # 每台間隔一陣，避免連續打爆上游
        time.sleep(REFRESH_GAP_SEC)

    return {"updated": results}
