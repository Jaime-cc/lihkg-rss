import os
import json
import time
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

import httpx
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.responses import Response, JSONResponse
from feedgen.feed import FeedGenerator

APP_TITLE = "LIHKG RSS (cache-only RSS, manual refresh)"
APP_VERSION = "0.2.0"

# ===== Config =====
LIHKG_API_URL = "https://lihkg.com/api_v2/thread/category"
LIHKG_SITE_CAT_URL = "https://lihkg.com/category/{cat_id}"

CACHE_DIR = os.getenv("CACHE_DIR", "./cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# Only these categories are exposed/refreshable (your OPML list)
DEFAULT_ALLOWED_CATS = "1,4,5,6,7,8,9,10,11,12"
ALLOWED_CATS = [int(x.strip()) for x in os.getenv("ALLOWED_CATS", DEFAULT_ALLOWED_CATS).split(",") if x.strip().isdigit()]
ALLOWED_SET = set(ALLOWED_CATS)

# How many items to render into RSS
DEFAULT_RSS_COUNT = int(os.getenv("RSS_COUNT", "60"))

# Refresh behavior
REFRESH_DELAY_SECONDS = float(os.getenv("REFRESH_DELAY_SECONDS", "2.0"))  # delay between cats
REFRESH_TIMEOUT = float(os.getenv("REFRESH_TIMEOUT", "20.0"))
REFRESH_RETRIES = int(os.getenv("REFRESH_RETRIES", "3"))
REFRESH_BACKOFF_BASE = float(os.getenv("REFRESH_BACKOFF_BASE", "1.8"))

# Optional Cloudflare clearance cookie (set in Railway Variables)
CF_CLEARANCE = os.getenv("CF_CLEARANCE", "").strip()

# Concurrency limit (keep low to avoid rate-limits)
REFRESH_MAX_CONCURRENCY = int(os.getenv("REFRESH_MAX_CONCURRENCY", "1"))
_refresh_sem = asyncio.Semaphore(REFRESH_MAX_CONCURRENCY)

app = FastAPI(title=APP_TITLE, version=APP_VERSION)




# Force close connection on some endpoints to reduce Windows schannel/curl issues
def _json_close(content: dict, status_code: int = 200):
    return JSONResponse(content=content, status_code=status_code, headers={
        "Connection": "close",
        "Cache-Control": "no-store",
    })
# ===== In-memory index (last_good per cat/page) =====
# key: (cat_id, page) -> {"last_good_utc": "...", "path": "...", "etag": "..."}
INDEX: Dict[str, Dict[str, Any]] = {}


def _key(cat_id: int, page: int) -> str:
    return f"{cat_id}:{page}"


def _cache_path(cat_id: int, page: int) -> str:
    return os.path.join(CACHE_DIR, f"cat_{cat_id}_p_{page}.json")


def _meta_path(cat_id: int, page: int) -> str:
    return os.path.join(CACHE_DIR, f"cat_{cat_id}_p_{page}.meta.json")


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_from_disk_into_index() -> None:
    # best-effort load metadata files
    try:
        for fn in os.listdir(CACHE_DIR):
            if fn.endswith(".meta.json"):
                p = os.path.join(CACHE_DIR, fn)
                try:
                    meta = json.loads(open(p, "r", encoding="utf-8").read())
                    cat_id = int(meta.get("cat_id", -1))
                    page = int(meta.get("page", 1))
                    if cat_id > 0:
                        INDEX[_key(cat_id, page)] = meta
                except Exception:
                    continue
    except Exception:
        pass


_load_from_disk_into_index()


def _read_cache(cat_id: int, page: int) -> Optional[Dict[str, Any]]:
    p = _cache_path(cat_id, page)
    if not os.path.exists(p):
        return None
    try:
        return json.loads(open(p, "r", encoding="utf-8").read())
    except Exception:
        return None


def _write_cache(cat_id: int, page: int, payload: Dict[str, Any], etag: Optional[str] = None) -> None:
    p = _cache_path(cat_id, page)
    mp = _meta_path(cat_id, page)
    open(p, "w", encoding="utf-8").write(json.dumps(payload, ensure_ascii=False))
    meta = {
        "cat_id": cat_id,
        "page": page,
        "last_good_utc": _now_utc_iso(),
        "path": p,
        "etag": etag,
    }
    open(mp, "w", encoding="utf-8").write(json.dumps(meta, ensure_ascii=False))
    INDEX[_key(cat_id, page)] = meta


def _build_rss(cat_id: int, page: int, cached: Optional[Dict[str, Any]]) -> bytes:
    fg = FeedGenerator()
    fg.title(f"LIHKG Category {cat_id} ({'Latest' if cached else 'no_cache_yet'})")
    fg.link(href=LIHKG_SITE_CAT_URL.format(cat_id=cat_id), rel="alternate")
    if cached:
        fg.description("Unofficial RSS for LIHKG. Served from cache; use /refresh to update.")
    else:
        fg.description("This feed serves cached/seed content. Use /refresh or /refresh_all to update.")
    fg.docs("http://www.rssboard.org/rss-specification")
    fg.generator("python-feedgen")

    # lastBuildDate: if cache exists use meta last_good_utc else now
    meta = INDEX.get(_key(cat_id, page))
    last_good = meta.get("last_good_utc") if meta else None
    if last_good:
        try:
            dt = datetime.fromisoformat(last_good.replace("Z", "+00:00"))
        except Exception:
            dt = datetime.now(timezone.utc)
    else:
        dt = datetime.now(timezone.utc)
    fg.lastBuildDate(dt)

    if not cached:
        return fg.rss_str(pretty=True)

    # payload shape depends on LIHKG api; we defensively parse common structure
    items = []
    try:
        # likely: payload["response"]["items"]
        resp = cached.get("response") or cached.get("data") or cached
        items = resp.get("items") or resp.get("threads") or []
    except Exception:
        items = []

    for it in items[:DEFAULT_RSS_COUNT]:
        # LIHKG thread id fields could be "thread_id" or "id"
        thread_id = it.get("thread_id") or it.get("id")
        title = it.get("title") or f"Thread {thread_id}"
        url = f"https://lihkg.com/thread/{thread_id}" if thread_id else LIHKG_SITE_CAT_URL.format(cat_id=cat_id)

        # counts: "no_of_reply" / "like_count" / "dislike_count" are common
        replies = it.get("no_of_reply")
        likes = it.get("like_count")
        dislikes = it.get("dislike_count")
        desc_bits = []
        if replies is not None:
            desc_bits.append(f"Replies: {replies}")
        if likes is not None:
            desc_bits.append(f"Likes: {likes}")
        if dislikes is not None:
            desc_bits.append(f"Dislikes: {dislikes}")
        desc = " | ".join(desc_bits) + "<br>" + url if desc_bits else url

        fe = fg.add_entry()
        fe.title(title)
        fe.link(href=url)
        fe.guid(str(thread_id) if thread_id else url, permalink=False)
        fe.description(desc)

        # time: LIHKG sometimes uses unix ts or iso; we try a few keys
        ts = it.get("create_time") or it.get("create_time_utc") or it.get("timestamp")
        pub_dt = None
        if isinstance(ts, (int, float)):
            try:
                pub_dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
            except Exception:
                pub_dt = None
        elif isinstance(ts, str):
            try:
                pub_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except Exception:
                pub_dt = None
        if pub_dt:
            fe.pubDate(pub_dt)

    return fg.rss_str(pretty=True)


def _lihkg_headers() -> Dict[str, str]:
    h = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://lihkg.com/",
        "Origin": "https://lihkg.com",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if CF_CLEARANCE:
        # IMPORTANT: Do not f-string inject unquoted values; just build plain cookie string
        h["Cookie"] = f"cf_clearance={CF_CLEARANCE}"
    return h


async def _fetch_category(cat_id: int, page: int, count: int) -> Tuple[Dict[str, Any], Optional[str]]:
    params = {"cat_id": cat_id, "page": page, "count": count}
    async with httpx.AsyncClient(timeout=REFRESH_TIMEOUT, headers=_lihkg_headers(), follow_redirects=True) as client:
        r = await client.get(LIHKG_API_URL, params=params)
        if r.status_code == 429:
            raise HTTPException(status_code=429, detail="LIHKG API HTTP 429 (rate limited)")
        if r.status_code >= 400:
            raise HTTPException(status_code=502, detail=f"LIHKG API HTTP {r.status_code}")
        etag = r.headers.get("etag")
        return (r.json(), etag)


async def _refresh_one(cat_id: int, page: int, count: int) -> Dict[str, Any]:
    if cat_id not in ALLOWED_SET:
        raise HTTPException(status_code=400, detail=f"cat_id not allowed. allowed={sorted(ALLOWED_SET)}")

    async with _refresh_sem:
        last_err: Optional[str] = None
        for attempt in range(1, REFRESH_RETRIES + 1):
            try:
                payload, etag = await _fetch_category(cat_id, page, count)
                _write_cache(cat_id, page, payload, etag=etag)
                return {"ok": True, "reason": "ok", "updated": True, "cat_id": cat_id, "page": page, "attempt": attempt}
            except HTTPException as e:
                # rate limit: backoff + retry
                last_err = str(e.detail)
                if e.status_code == 429 and attempt < REFRESH_RETRIES:
                    await asyncio.sleep((REFRESH_BACKOFF_BASE ** (attempt - 1)) * REFRESH_DELAY_SECONDS)
                    continue
                raise
            except Exception as e:
                last_err = repr(e)
                if attempt < REFRESH_RETRIES:
                    await asyncio.sleep((REFRESH_BACKOFF_BASE ** (attempt - 1)) * REFRESH_DELAY_SECONDS)
                    continue
                raise HTTPException(status_code=502, detail=f"Refresh failed: {last_err}")

        raise HTTPException(status_code=502, detail=f"Refresh failed: {last_err}")


async def _refresh_all_bg() -> None:
    # background job: refresh all allowed cats sequentially with delay
    for cat_id in ALLOWED_CATS:
        try:
            await _refresh_one(cat_id, page=1, count=DEFAULT_RSS_COUNT)
        except Exception:
            # ignore one cat failure; continue
            pass
        await asyncio.sleep(REFRESH_DELAY_SECONDS)


@app.get("/")
def home():
    return _json_close({
        "ok": True,
        "mode": "cache_only_rss_manual_refresh",
        "allowed_cats": ALLOWED_CATS,
        "usage": ["/rss?cat_id=5", "/status?cat_id=5", "/refresh?cat_id=5", "/refresh_all"],
        "note": "RSS will NOT hit LIHKG. Use /refresh or /refresh_all manually to update and avoid 429.",
    })


@app.get("/status")
def status(cat_id: int = Query(...), page: int = Query(1)):
    if cat_id not in ALLOWED_SET:
        raise HTTPException(status_code=400, detail=f"cat_id not allowed. allowed={sorted(ALLOWED_SET)}")

    k = _key(cat_id, page)
    meta = INDEX.get(k)
    cached = _read_cache(cat_id, page)
    return {
        "cat_id": cat_id,
        "page": page,
        "has_last_good": bool(meta and meta.get("last_good_utc")),
        "last_good_utc": meta.get("last_good_utc") if meta else None,
        "cache_hit": cached is not None,
    }


@app.get("/rss")
def rss(cat_id: int = Query(...), page: int = Query(1)):
    if cat_id not in ALLOWED_SET:
        raise HTTPException(status_code=400, detail=f"cat_id not allowed. allowed={sorted(ALLOWED_SET)}")
    cached = _read_cache(cat_id, page)
    xml = _build_rss(cat_id, page, cached)
    return Response(content=xml, media_type="application/rss+xml; charset=utf-8")


@app.get("/refresh")
async def refresh(cat_id: int = Query(...), page: int = Query(1), count: int = Query(DEFAULT_RSS_COUNT)):
    # This endpoint DOES call LIHKG.
    result = await _refresh_one(cat_id, page, count)
    return JSONResponse(result)


@app.get("/refresh_all")
async def refresh_all(background_tasks: BackgroundTasks):
    """
    IMPORTANT:
    - Returns immediately (202) to avoid connection being closed by edge/TLS clients.
    - Actual refresh runs in background.
    """
    background_tasks.add_task(_refresh_all_bg)
    return JSONResponse(
        status_code=202,
        content={
            "ok": True,
            "queued": True,
            "mode": "background_refresh_all",
            "cats": ALLOWED_CATS,
            "note": "Refresh started in background. Check /status?cat_id=5 for last_good_utc updates.",
        },
    )
from fastapi import Request
from fastapi.responses import PlainTextResponse
import asyncio

@app.post("/wa")
async def wa(request: Request):
    form = await request.form()
    body = (form.get("Body") or "").strip().lower()

    # 你 WhatsApp 打 "all" → 觸發 refresh_all
    if body == "all":
        # 用背景方式做，避免 Twilio 等太耐
        asyncio.create_task(refresh_all_internal())
        return PlainTextResponse("OK")

    # 你 WhatsApp 打 "r 5" → 觸發 refresh cat 5
    if body.startswith("r "):
        try:
            cat = int(body.split()[1])
            asyncio.create_task(refresh_one_internal(cat))
            return PlainTextResponse("OK")
        except:
            return PlainTextResponse("OK")

    return PlainTextResponse("OK")

