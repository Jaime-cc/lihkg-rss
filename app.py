from __future__ import annotations

import asyncio
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from cachetools import TTLCache
from fastapi import BackgroundTasks, FastAPI, Query, Request, Response
from fastapi.responses import PlainTextResponse
from feedgen.feed import FeedGenerator

APP_NAME = "lihkg-rss"
DEFAULT_ALLOWED_CATS = [1, 4, 5, 6, 7, 8, 9, 10, 11, 12]

# ---- Tunables (env override) -------------------------------------------------
CACHE_DIR = os.getenv("CACHE_DIR", "/tmp/lihkg_cache")
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "1800"))  # RSS cache TTL
HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
REFRESH_COOLDOWN_SECONDS = int(os.getenv("REFRESH_COOLDOWN_SECONDS", "1800"))  # avoid 429
REFRESH_CONCURRENCY = int(os.getenv("REFRESH_CONCURRENCY", "1"))
REFRESH_ALL_DELAY_SECONDS = float(os.getenv("REFRESH_ALL_DELAY_SECONDS", "2.0"))

# For WhatsApp bot access control (optional)
# In Twilio Sandbox, only joined numbers can message you, but this adds a safety belt.
WA_ALLOWLIST = set(
    x.strip()
    for x in os.getenv("WA_ALLOWLIST", "").split(",")
    if x.strip()
)  # e.g. "whatsapp:+8529xxxxxxx"

# -----------------------------------------------------------------------------
os.makedirs(CACHE_DIR, exist_ok=True)

app = FastAPI(title="FastAPI", version="0.1.0")

# In-memory index: (cat_id, page) -> metadata about cache & refresh
INDEX: Dict[Tuple[int, int], Dict[str, Any]] = {}
INDEX_LOCK = asyncio.Lock()

# RSS XML cache: key -> xml
RSS_CACHE = TTLCache(maxsize=256, ttl=CACHE_TTL_SECONDS)

# Global refresh semaphore (avoid hammering LIHKG)
REFRESH_SEM = asyncio.Semaphore(REFRESH_CONCURRENCY)

# A single shared async client (HTTP/2 off to be conservative)
HTTP = httpx.AsyncClient(
    timeout=httpx.Timeout(HTTP_TIMEOUT_SECONDS),
    headers={"User-Agent": f"{APP_NAME}/1.0"},
    http2=False,
)

# -----------------------------------------------------------------------------
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None

def _http_headers_close() -> Dict[str, str]:
    # Helps Windows schannel + keep-alive weirdness; also avoid caching proxies.
    return {
        "Connection": "close",
        "Cache-Control": "no-store",
        "Pragma": "no-cache",
    }

def _json_close(data: Any, status_code: int = 200) -> Response:
    return Response(
        content=json.dumps(data, ensure_ascii=False),
        status_code=status_code,
        media_type="application/json",
        headers=_http_headers_close(),
    )

def _text_close(text: str, status_code: int = 200, media_type: str = "text/plain") -> Response:
    return Response(
        content=text,
        status_code=status_code,
        media_type=media_type,
        headers=_http_headers_close(),
    )

def _rss_close(xml: str) -> Response:
    return Response(
        content=xml,
        status_code=200,
        media_type="application/rss+xml; charset=utf-8",
        headers=_http_headers_close(),
    )

def _cache_path(cat_id: int, page: int) -> str:
    return os.path.join(CACHE_DIR, f"cat_{cat_id}_page_{page}.json")

def _read_cache(cat_id: int, page: int) -> Optional[Dict[str, Any]]:
    fp = _cache_path(cat_id, page)
    if not os.path.exists(fp):
        return None
    try:
        with open(fp, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _write_cache(cat_id: int, page: int, data: Dict[str, Any]) -> None:
    fp = _cache_path(cat_id, page)
    tmp = fp + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, fp)

async def _index_get(cat_id: int, page: int) -> Dict[str, Any]:
    async with INDEX_LOCK:
        return INDEX.get((cat_id, page), {}).copy()

async def _index_set(cat_id: int, page: int, patch: Dict[str, Any]) -> None:
    async with INDEX_LOCK:
        cur = INDEX.get((cat_id, page), {})
        cur.update(patch)
        INDEX[(cat_id, page)] = cur

def _allowed_cats() -> List[int]:
    env = os.getenv("ALLOWED_CATS")
    if not env:
        return DEFAULT_ALLOWED_CATS
    out = []
    for p in env.split(","):
        p = p.strip()
        if not p:
            continue
        try:
            out.append(int(p))
        except ValueError:
            pass
    return out or DEFAULT_ALLOWED_CATS

# -----------------------------------------------------------------------------
async def _fetch_lihkg_category(cat_id: int, page: int = 1, count: int = 60) -> Dict[str, Any]:
    """
    Fetch LIHKG category threads via their API v2.

    NOTE: This is unofficial and may change.
    """
    url = "https://lihkg.com/api_v2/thread/category"
    params = {"cat_id": cat_id, "page": page, "count": count, "type": "now"}
    async with REFRESH_SEM:
        r = await HTTP.get(url, params=params)
    r.raise_for_status()
    return r.json()

def _extract_items(api_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = []
    threads = (((api_json or {}).get("response") or {}).get("items")) or []
    for t in threads:
        try:
            thread_id = t.get("thread_id")
            title = t.get("title") or ""
            # LIHKG returns numeric strings sometimes
            reply_count = t.get("reply_count")
            like = t.get("like_count")
            dislike = t.get("dislike_count")
            create_time = t.get("create_time")  # unix seconds
            pub_dt = datetime.fromtimestamp(int(create_time), tz=timezone.utc) if create_time else _utcnow()
            link = f"https://lihkg.com/thread/{thread_id}"
            desc = f"Replies: {reply_count} | Likes: {like} | Dislikes: {dislike}<br>{link}"
            items.append(
                {
                    "title": title,
                    "link": link,
                    "guid": str(thread_id),
                    "pubDate": pub_dt,
                    "description": desc,
                }
            )
        except Exception:
            continue
    return items

def _build_rss(cat_id: int, page: int, items: List[Dict[str, Any]], title_suffix: str) -> str:
    fg = FeedGenerator()
    fg.title(f"LIHKG Category {cat_id} ({title_suffix})")
    fg.link(href=f"https://lihkg.com/category/{cat_id}", rel="alternate")
    fg.description("Unofficial RSS for LIHKG. Served from cache; use /refresh to update.")
    fg.generator("python-feedgen")

    fg.lastBuildDate(_utcnow())

    for it in items:
        fe = fg.add_entry()
        fe.title(it["title"])
        fe.link(href=it["link"])
        fe.guid(it["guid"], permalink=False)
        fe.description(it["description"])
        fe.pubDate(it["pubDate"])

    return fg.rss_str(pretty=True).decode("utf-8")

async def _refresh_one(cat_id: int, page: int = 1, count: int = 60) -> Dict[str, Any]:
    allowed = _allowed_cats()
    if cat_id not in allowed:
        return {"ok": False, "reason": "cat_not_allowed", "allowed_cats": allowed}

    # Cooldown guard (avoid LIHKG 429)
    meta = await _index_get(cat_id, page)
    last_attempt = meta.get("last_attempt_utc")
    if last_attempt:
        try:
            last_attempt_dt = datetime.fromisoformat(last_attempt)
            if (_utcnow() - last_attempt_dt).total_seconds() < REFRESH_COOLDOWN_SECONDS:
                return {
                    "ok": True,
                    "skipped": True,
                    "reason": "cooldown",
                    "cat_id": cat_id,
                    "page": page,
                    "cooldown_seconds": REFRESH_COOLDOWN_SECONDS,
                    "last_attempt_utc": last_attempt,
                }
        except Exception:
            pass

    await _index_set(cat_id, page, {"last_attempt_utc": _iso(_utcnow())})

    try:
        api_json = await _fetch_lihkg_category(cat_id=cat_id, page=page, count=count)
        items = _extract_items(api_json)
        payload = {
            "cat_id": cat_id,
            "page": page,
            "fetched_utc": _iso(_utcnow()),
            "items": [
                {
                    "title": x["title"],
                    "link": x["link"],
                    "guid": x["guid"],
                    "pubDate": _iso(x["pubDate"]),
                    "description": x["description"],
                }
                for x in items
            ],
        }
        _write_cache(cat_id, page, payload)
        await _index_set(
            cat_id,
            page,
            {
                "has_last_good": True,
                "last_good_utc": _iso(_utcnow()),
                "last_error": None,
            },
        )
        # Bust RSS cache
        RSS_CACHE.pop((cat_id, page), None)

        return {"ok": True, "reason": "ok", "updated": True, "cat_id": cat_id, "page": page, "count": count, "items": len(items)}
    except httpx.HTTPStatusError as e:
        # LIHKG may return 429
        status = e.response.status_code if e.response else None
        await _index_set(cat_id, page, {"last_error": f"http_status_{status}"})
        return {"ok": False, "reason": f"http_status_{status}", "status_code": status, "cat_id": cat_id, "page": page}
    except Exception as e:
        await _index_set(cat_id, page, {"last_error": str(e)})
        return {"ok": False, "reason": "exception", "error": str(e), "cat_id": cat_id, "page": page}

async def _background_refresh_one(cat_id: int, page: int, count: int) -> None:
    await _refresh_one(cat_id=cat_id, page=page, count=count)

async def _background_refresh_all() -> None:
    for c in _allowed_cats():
        await _refresh_one(cat_id=c, page=1, count=60)
        await asyncio.sleep(REFRESH_ALL_DELAY_SECONDS)

# -----------------------------------------------------------------------------
@app.on_event("shutdown")
async def _shutdown() -> None:
    await HTTP.aclose()

@app.get("/")
async def home() -> Response:
    return _json_close(
        {
            "ok": True,
            "mode": "cache_only_rss_manual_refresh",
            "allowed_cats": _allowed_cats(),
            "usage": ["/rss?cat_id=5", "/status?cat_id=5", "/refresh?cat_id=5", "/refresh_all", "/wa (Twilio WhatsApp webhook)"],
            "note": "RSS will NOT hit LIHKG. Use /refresh or /refresh_all manually to update and avoid 429.",
        }
    )

@app.get("/status")
async def status(cat_id: int = Query(...), page: int = Query(1)) -> Response:
    meta = await _index_get(cat_id, page)
    return _json_close(
        {
            "cat_id": cat_id,
            "page": page,
            "has_last_good": bool(meta.get("has_last_good")),
            "last_good_utc": meta.get("last_good_utc"),
            "last_attempt_utc": meta.get("last_attempt_utc"),
            "last_error": meta.get("last_error"),
        }
    )

@app.get("/rss")
async def rss(cat_id: int = Query(...), page: int = Query(1)) -> Response:
    key = (cat_id, page)
    cached = RSS_CACHE.get(key)
    if cached:
        return _rss_close(cached)

    payload = _read_cache(cat_id, page)
    if not payload:
        xml = _build_rss(cat_id, page, [], "no_cache_yet")
        RSS_CACHE[key] = xml
        return _rss_close(xml)

    items = []
    for it in payload.get("items", []):
        try:
            pub = it.get("pubDate")
            pub_dt = datetime.fromisoformat(pub) if pub else _utcnow()
            items.append(
                {
                    "title": it.get("title", ""),
                    "link": it.get("link", ""),
                    "guid": it.get("guid", ""),
                    "pubDate": pub_dt,
                    "description": it.get("description", ""),
                }
            )
        except Exception:
            continue

    xml = _build_rss(cat_id, page, items, "Latest")
    RSS_CACHE[key] = xml
    return _rss_close(xml)

@app.get("/refresh")
async def refresh(
    background_tasks: BackgroundTasks,
    cat_id: int = Query(...),
    page: int = Query(1),
    count: int = Query(60),
    wait: bool = Query(False, description="wait=true will block until refresh completes"),
) -> Response:
    """
    Default: return 202 immediately and refresh in background.
    wait=true: do the refresh synchronously and return result.
    """
    if wait:
        result = await _refresh_one(cat_id=cat_id, page=page, count=count)
        return _json_close(result, status_code=200 if result.get("ok") else 502)

    # queue background refresh
    background_tasks.add_task(_background_refresh_one, cat_id, page, count)
    return _json_close(
        {"ok": True, "queued": True, "mode": "background_refresh_one", "cat_id": cat_id, "page": page, "note": "Refresh started in background. Use /status to check last_good_utc."},
        status_code=202,
    )

@app.get("/refresh_all")
async def refresh_all(background_tasks: BackgroundTasks) -> Response:
    """
    One-click refresh for existing categories. Returns 202 immediately.
    """
    background_tasks.add_task(_background_refresh_all)
    return _json_close(
        {
            "ok": True,
            "queued": True,
            "mode": "background_refresh_all",
            "cats": _allowed_cats(),
            "note": "Refresh started in background. Check /status?cat_id=5 for last_good_utc updates.",
        },
        status_code=202,
    )

# -----------------------------------------------------------------------------
# WhatsApp Bot (Twilio Sandbox)
# Twilio will POST application/x-www-form-urlencoded to this endpoint.
# You configure the Sandbox 'When a message comes in' URL to:
#   https://<your-railway-domain>/wa
# -----------------------------------------------------------------------------
def _twiml_message(text: str) -> str:
    # Minimal TwiML
    # Twilio accepts UTF-8; ensure no XML-breaking chars.
    safe = (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
    return f'<?xml version="1.0" encoding="UTF-8"?><Response><Message>{safe}</Message></Response>'

def _parse_command(body: str) -> Tuple[str, Optional[int]]:
    b = (body or "").strip().lower()

    # common aliases
    if b in {"help", "?", "h"}:
        return ("help", None)
    if b in {"all", "refresh_all", "refresh all", "update all", "update_all"}:
        return ("refresh_all", None)

    # "refresh 5" / "update 5" / "cat 5"
    m = re.search(r"(refresh|update|cat)\s*[:=]?\s*(\d+)", b)
    if m:
        return ("refresh_one", int(m.group(2)))

    # just "5"
    if re.fullmatch(r"\d+", b):
        return ("refresh_one", int(b))

    return ("unknown", None)

@app.get("/wa")
async def wa_get() -> Response:
    # For browser sanity-check
    return _text_close(
        "OK. This is the Twilio WhatsApp webhook endpoint. Configure Twilio Sandbox to POST to /wa.\n"
        "Commands: all | refresh_all | 5 | refresh 5 | help\n"
    )

@app.post("/wa")
async def wa_post(request: Request, background_tasks: BackgroundTasks) -> Response:
    form = await request.form()
    body = str(form.get("Body") or "")
    from_ = str(form.get("From") or "")  # e.g. "whatsapp:+852..."
    profile = str(form.get("ProfileName") or "")

    # optional allowlist
    if WA_ALLOWLIST and from_ not in WA_ALLOWLIST:
        msg = f"Not allowed: {from_}\n(Ask admin to add you to WA_ALLOWLIST)"
        return Response(content=_twiml_message(msg), media_type="application/xml", headers=_http_headers_close())

    cmd, cat = _parse_command(body)

    if cmd == "help":
        msg = (
            "LIHKG RSS bot commands:\n"
            "• all  (or refresh_all)\n"
            "• 5    (refresh category 5)\n"
            "• refresh 5\n"
            "Then check Inoreader or /status?cat_id=5"
        )
        return Response(content=_twiml_message(msg), media_type="application/xml", headers=_http_headers_close())

    if cmd == "refresh_all":
        background_tasks.add_task(_background_refresh_all)
        msg = "✅ 已開始背景更新所有分類。\n可用 /status?cat_id=5 睇 last_good_utc 有冇變。"
        return Response(content=_twiml_message(msg), media_type="application/xml", headers=_http_headers_close())

    if cmd == "refresh_one" and cat is not None:
        if cat not in _allowed_cats():
            msg = f"❌ cat_id {cat} 不在 allowed。\nAllowed: {_allowed_cats()}"
            return Response(content=_twiml_message(msg), media_type="application/xml", headers=_http_headers_close())
        background_tasks.add_task(_background_refresh_one, cat, 1, 60)
        msg = f"✅ 已開始背景更新 cat_id={cat}。\n稍後用 /status?cat_id={cat} 睇 last_good_utc。"
        return Response(content=_twiml_message(msg), media_type="application/xml", headers=_http_headers_close())

    msg = (
        f"唔明你意思：{body!r}\n"
        "試下：all / refresh_all / 5 / refresh 5 / help"
    )
    return Response(content=_twiml_message(msg), media_type="application/xml", headers=_http_headers_close())
