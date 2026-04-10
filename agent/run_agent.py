from pathlib import Path
from datetime import datetime
import time

Path("SCHEDULER_PROOF.txt").write_text(
    f"Loaded run_agent.py at {datetime.now()}\n",
    encoding="utf-8"
)
from pathlib import Path
import datetime

with open("agent_start_log.txt", "a") as f:
    f.write(f"Started at {datetime.datetime.now()} | CWD={Path.cwd()}\n")

import os
import re
import csv
import atexit
import ctypes
from pathlib import Path
from datetime import datetime, timedelta, timezone
import sys

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import requests
import feedparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from zoneinfo import ZoneInfo
LOG_FILE = Path("agent_lifecycle.log")
# ---- Discord sender (must exist) ----
from src.notify_discord import send_discord

ENABLE_SCHEDULED = True
ENABLE_FEEDS = True
ENABLE_HEARTBEAT = True
# ============================================================
# CONFIG (safe-load agent/config.py but never crash if missing)
# ============================================================
def load_config():
    # Defaults (will work even without agent/config.py)
    cfg = {
        "DISCORD_WEBHOOK_URL": os.getenv("DISCORD_WEBHOOK_URL", "").strip(),
        "ENABLE_SCHEDULED": ENABLE_SCHEDULED,
        "ENABLE_FEEDS": ENABLE_FEEDS,
        "ENABLE_HEARTBEAT": ENABLE_HEARTBEAT,
        "HEARTBEAT_HOURS": 24,
        "LOCAL_ZIPS": {"60491", "60441", "60451", "60462"},
        "ALERT_SCORE_THRESHOLD": 3,

        # file paths relative to project root
        "PRODUCTS_CSV": "data/products.csv",
        "DROP_EVENTS_CSV": "data/drop_events.csv",
        "DROP_PRODUCTS_CSV": "data/drop_products.csv",
        "STORE_PATTERNS_CSV": "data/store_patterns.csv",
        "SCORED_DROPS_CSV": "data/scored_drops.csv",
        "ALERT_HISTORY_CSV": "data/alert_history.csv",

        # rotational + rss
        "SCHEDULED_DROPS_CSV": "data/scheduled_drops.csv",
        "SCHEDULE_HISTORY_CSV": "data/schedule_alert_history.csv",
        "FEEDS_CSV": "data/feeds.csv",
        "FEED_HISTORY_CSV": "data/feed_history.csv",
        "HEARTBEAT_HISTORY_CSV": "data/heartbeat_history.csv",
        "FEED_REQUIRED_KEYWORDS": [
            "elite trainer box",
            "etb",
            "pokemon center etb",
            "super premium collection",
            "premium collection",
            "booster bundle",
            "booster box",
            "collection box",
            "build and battle box",
            "mini tin",
            "tin",
            "spc",
            "upc",
        ],
    }

    try:
        import agent.config as c  # noqa
        # Use config.py values if present, but don't fail if missing
        cfg["DISCORD_WEBHOOK_URL"] = (getattr(c, "DISCORD_WEBHOOK_URL", cfg["DISCORD_WEBHOOK_URL"]) or "").strip()
        cfg["ENABLE_SCHEDULED"] = bool(getattr(c, "ENABLE_SCHEDULED", cfg["ENABLE_SCHEDULED"]))
        cfg["ENABLE_FEEDS"] = bool(getattr(c, "ENABLE_FEEDS", cfg["ENABLE_FEEDS"]))
        cfg["ENABLE_HEARTBEAT"] = bool(getattr(c, "ENABLE_HEARTBEAT", cfg["ENABLE_HEARTBEAT"]))
        cfg["HEARTBEAT_HOURS"] = float(getattr(c, "HEARTBEAT_HOURS", cfg["HEARTBEAT_HOURS"]))
        cfg["LOCAL_ZIPS"] = set(getattr(c, "LOCAL_ZIPS", cfg["LOCAL_ZIPS"]))
        cfg["ALERT_SCORE_THRESHOLD"] = float(getattr(c, "ALERT_SCORE_THRESHOLD", cfg["ALERT_SCORE_THRESHOLD"]))

        cfg["PRODUCTS_CSV"] = getattr(c, "PRODUCTS_CSV", cfg["PRODUCTS_CSV"])
        cfg["DROP_EVENTS_CSV"] = getattr(c, "DROP_EVENTS_CSV", cfg["DROP_EVENTS_CSV"])
        cfg["DROP_PRODUCTS_CSV"] = getattr(c, "DROP_PRODUCTS_CSV", cfg["DROP_PRODUCTS_CSV"])
        cfg["STORE_PATTERNS_CSV"] = getattr(c, "STORE_PATTERNS_CSV", cfg["STORE_PATTERNS_CSV"])
        cfg["SCORED_DROPS_CSV"] = getattr(c, "SCORED_DROPS_CSV", cfg["SCORED_DROPS_CSV"])
        cfg["ALERT_HISTORY_CSV"] = getattr(c, "ALERT_HISTORY_CSV", cfg["ALERT_HISTORY_CSV"])
        cfg["SCHEDULED_DROPS_CSV"] = getattr(c, "SCHEDULED_DROPS_CSV", cfg["SCHEDULED_DROPS_CSV"])
        cfg["SCHEDULE_HISTORY_CSV"] = getattr(c, "SCHEDULE_HISTORY_CSV", cfg["SCHEDULE_HISTORY_CSV"])
        cfg["FEEDS_CSV"] = getattr(c, "FEEDS_CSV", cfg["FEEDS_CSV"])
        cfg["FEED_HISTORY_CSV"] = getattr(c, "FEED_HISTORY_CSV", cfg["FEED_HISTORY_CSV"])
        cfg["HEARTBEAT_HISTORY_CSV"] = getattr(c, "HEARTBEAT_HISTORY_CSV", cfg["HEARTBEAT_HISTORY_CSV"])
        cfg["FEED_REQUIRED_KEYWORDS"] = list(getattr(c, "FEED_REQUIRED_KEYWORDS", cfg["FEED_REQUIRED_KEYWORDS"]))
    except Exception:
        # totally fine — defaults will be used
        pass

    return cfg


# ============================================================
# PATHS (always resolve relative to project root)
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCK_FILE = PROJECT_ROOT / "agent" / "run_agent.lock"


def p(rel):
    return str((PROJECT_ROOT / rel).resolve())


def _pid_is_alive(pid: int) -> bool:
    if not pid or pid <= 0:
        return False

    # Windows-safe check via OpenProcess/GetExitCodeProcess
    if os.name == "nt":
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        STILL_ACTIVE = 259
        handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
        if not handle:
            return False
        try:
            exit_code = ctypes.c_ulong()
            ok = ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
            return bool(ok) and exit_code.value == STILL_ACTIVE
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)

    # Fallback for non-Windows
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _release_lock():
    try:
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()
    except Exception:
        pass


def acquire_single_instance_lock() -> bool:
    """Return True if lock acquired, False if another live instance exists."""
    try:
        LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
        if LOCK_FILE.exists():
            try:
                existing_pid = int(LOCK_FILE.read_text(encoding="utf-8").strip())
            except Exception:
                existing_pid = 0

            if existing_pid and _pid_is_alive(existing_pid):
                print(f"[Agent] Another instance is already running (PID {existing_pid}). Exiting.")
                return False

            # stale lock
            _release_lock()

        LOCK_FILE.write_text(str(os.getpid()), encoding="utf-8")
        atexit.register(_release_lock)
        return True
    except Exception as e:
        print(f"[Agent] Lock setup warning: {e}")
        return True


# ============================================================
# UTIL: safe CSV read/write
# ============================================================
def read_csv_or_empty(path, columns=None):
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame(columns=columns or [])


def ensure_columns(df, cols):
    df = df.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df


def normalize_scalar_id(value):
    """Normalize IDs like 35.0 -> 35 and trim whitespace."""
    if pd.isna(value):
        return ""
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return ""
    m = re.fullmatch(r"(\d+)\.0+", s)
    if m:
        return m.group(1)
    return s


def expand_drop_products(drop_products: pd.DataFrame) -> pd.DataFrame:
    """Expand multi-product_id cells like '35&5' into separate rows."""
    dp = ensure_columns(drop_products, ["drop_id", "product_id"]).copy()
    rows = []
    for _, row in dp.iterrows():
        drop_id = normalize_scalar_id(row.get("drop_id"))
        raw_pid = str(row.get("product_id", "") or "").strip()
        parts = [normalize_scalar_id(p) for p in re.split(r"[&|,;/]+", raw_pid)] if raw_pid else [""]
        parts = [p for p in parts if p]
        if not parts:
            rows.append({"drop_id": drop_id, "product_id": ""})
            continue
        for pid in parts:
            rows.append({"drop_id": drop_id, "product_id": pid})
    return pd.DataFrame(rows, columns=["drop_id", "product_id"])


def canonical_item_key(item: dict) -> str:
    """Create stable key so same article from different feeds alerts once."""
    base = str(item.get("link") or item.get("id") or item.get("title") or "").strip().lower()
    if base.startswith("http://") or base.startswith("https://"):
        base = base.split("#", 1)[0]
        base = base.split("?", 1)[0]
    return base


# ============================================================
# ROTATIONAL / SCHEDULED ALERTS
# data/scheduled_drops.csv columns:
# event_id,event_name,retailer,day_of_week,time_local,timezone,lead_minutes,active,message
# ============================================================
def run_scheduled(now_utc, webhook_url, cfg):
    sched_path = p(cfg["SCHEDULED_DROPS_CSV"])
    hist_path = p(cfg["SCHEDULE_HISTORY_CSV"])

    sched = read_csv_or_empty(sched_path)
    if sched.empty:
        return 0

    # history: event_id, fire_date
    hist = read_csv_or_empty(hist_path, columns=["event_id", "fire_date", "fired_at"])

    sent = 0
    for _, row in sched.iterrows():
        if str(row.get("active", "TRUE")).upper() not in ("TRUE", "1", "YES", "Y"):
            continue

        try:
            tz = ZoneInfo(str(row["timezone"]))
            local_now = now_utc.astimezone(tz)

            # match weekday
            if local_now.strftime("%A") != str(row["day_of_week"]):
                continue

            hh, mm = map(int, str(row["time_local"]).split(":"))
            target = local_now.replace(hour=hh, minute=mm, second=0, microsecond=0)

            lead = int(row.get("lead_minutes", 0))
            window_start = target - timedelta(minutes=lead)
            window_end = target + timedelta(minutes=1)

            if not (window_start <= local_now <= window_end):
                continue

            fire_date = local_now.date().isoformat()
            already = ((hist["event_id"] == row["event_id"]) & (hist["fire_date"] == fire_date)).any()
            if already:
                continue

            title = f"⏰ Scheduled Drop: {row.get('event_name', row.get('event_id', 'Scheduled'))}"
            msg = str(row.get("message", ""))

            send_discord(webhook_url, title, msg)
            hist.loc[len(hist)] = [row["event_id"], fire_date, now_utc.isoformat(timespec="seconds")]
            sent += 1

        except Exception as e:
            # don't crash the agent because of one bad scheduled row
            print("Scheduled alert error:", e)

    hist.to_csv(hist_path, index=False)
    return sent


# ============================================================
# RSS/FEED ALERTS
# data/feeds.csv columns:
# feed_id,feed_name,feed_url,keywords,active
# keywords pipe-separated: "Perfect Order|Surging Sparks|..."
# ============================================================
def parse_rss_items(feed_text):
    parsed = feedparser.parse(feed_text)
    items = []
    for entry in parsed.entries:
        guid = (
            str(getattr(entry, "id", "") or "").strip()
            or str(getattr(entry, "link", "") or "").strip()
            or str(getattr(entry, "title", "") or "").strip()
        )
        if not guid:
            continue
        items.append(
            {
                "id": guid,
                "title": str(getattr(entry, "title", "") or "").strip(),
                "link": str(getattr(entry, "link", "") or "").strip(),
                "pubDate": str(getattr(entry, "published", "") or getattr(entry, "updated", "") or "").strip(),
                "summary": str(getattr(entry, "summary", "") or getattr(entry, "description", "") or "").strip(),
            }
        )
    return items


def create_resilient_session():
    """Create a requests Session with retry logic and proper headers."""
    session = requests.Session()
    
    # Set proper User-Agent to avoid being blocked
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    
    # Configure retry strategy with exponential backoff
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,  # 1s, 2s, 4s delays
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session


def run_feeds(webhook_url, cfg, session=None):
    if session is None:
        session = create_resilient_session()

    required_keywords = [
        str(k).strip().lower()
        for k in cfg.get("FEED_REQUIRED_KEYWORDS", [])
        if str(k).strip()
    ]
    
    feeds_path = p(cfg["FEEDS_CSV"])
    hist_path = p(cfg["FEED_HISTORY_CSV"])

    feeds = read_csv_or_empty(feeds_path)
    if feeds.empty:
        print("[Feeds] No feeds configured.")
        return 0

    hist = read_csv_or_empty(hist_path, columns=["feed_id", "item_id", "item_key"])
    hist = ensure_columns(hist, ["feed_id", "item_id", "item_key"])
    if hist["item_key"].isna().all():
        hist["item_key"] = hist["item_id"].astype(str).str.strip().str.lower()
    sent = 0

    fallback_urls = {
        "pokeguardian": [
            "https://news.google.com/rss/search?q=site:pokeguardian.com+pokemon+tcg&hl=en-US&gl=US&ceid=US:en"
        ],
        "tcgplayer": [
            "https://news.google.com/rss/search?q=site:tcgplayer.com+pokemon+tcg&hl=en-US&gl=US&ceid=US:en"
        ],
        "tcginsider": [
            "https://tcginsider.com/feed/"
        ],
        "pokenews": [
            "https://news.google.com/rss/search?q=site:pokemon.com+pokemon+news&hl=en-US&gl=US&ceid=US:en"
        ],
        "pocketmonsters": [
            "https://news.google.com/rss/search?q=site:pokemon.com+pokemon+news&hl=en-US&gl=US&ceid=US:en"
        ],
    }

    for idx, f in feeds.iterrows():
        if str(f.get("active", "TRUE")).upper() not in ("TRUE", "1", "YES", "Y"):
            continue

        feed_id = str(f.get("feed_id", "")).strip()
        primary_url = str(f.get("feed_url", "")).strip()
        urls_to_try = [primary_url] + fallback_urls.get(feed_id, [])
        urls_to_try = [u for i, u in enumerate(urls_to_try) if u and u not in urls_to_try[:i]]
        url = primary_url
        if not feed_id or not primary_url:
            continue

        keywords = []
        if pd.notna(f.get("keywords")) and str(f.get("keywords")).strip():
            keywords = [k.strip() for k in str(f.get("keywords")).split("|") if k.strip()]

        try:
            # Add slight delay between feed requests to be respectful
            if idx > 0:
                time.sleep(0.5)
            
            items = []
            last_http_status = None
            for try_url in urls_to_try:
                url = try_url
                print(f"[Feeds] Fetching {feed_id} from {url}")
                r = session.get(url, timeout=15)
                last_http_status = r.status_code
                if r.status_code >= 400:
                    print(f"[Feeds] HTTP {r.status_code} for {feed_id} ({url})")
                    continue
                items = parse_rss_items(r.text)
                if items:
                    break

            if not items and last_http_status and last_http_status >= 400:
                continue
            if not items:
                print(f"[Feeds] {feed_id}: No parsable items from configured sources")
            print(f"[Feeds] {feed_id}: Got {len(items)} items")

            for it in items[:25]:
                item_id = str(it.get("id", "") or "").strip()
                item_key = canonical_item_key(it)
                already_feed = ((hist["feed_id"] == feed_id) & (hist["item_id"] == item_id)).any()
                already_global = (hist["item_key"] == item_key).any()

                if already_feed:
                    continue

                # record seen regardless
                hist = pd.concat(
                    [
                        hist,
                        pd.DataFrame([
                            {
                                "feed_id": feed_id,
                                "item_id": item_id,
                                "item_key": item_key,
                            }
                        ]),
                    ],
                    ignore_index=True,
                )

                # suppress cross-feed duplicates (same article from another source)
                if already_global:
                    continue

                # optional keyword filter
                if keywords and not any(k.lower() in it["title"].lower() for k in keywords):
                    continue

                # Product-only gate so generic market/card news is skipped.
                item_text = " ".join(
                    [
                        str(it.get("title", "") or ""),
                        str(it.get("summary", "") or ""),
                    ]
                ).lower()
                if required_keywords and not any(k in item_text for k in required_keywords):
                    continue

                title = f"📰 News: {it['title']}"
                msg = f"{it['link']}\n{it.get('pubDate','')}"
                send_discord(webhook_url, title, msg)
                sent += 1
                print(f"[Feeds] Alert sent for {feed_id}: {it['title'][:50]}...")

        except requests.exceptions.Timeout:
            print(f"[Feeds] TIMEOUT: {feed_id} ({url})")
        except requests.exceptions.ConnectionError as e:
            print(f"[Feeds] CONNECTION ERROR: {feed_id} ({url}) - {e}")
        except requests.exceptions.HTTPError as e:
            print(f"[Feeds] HTTP ERROR: {feed_id} ({url}) - Status {r.status_code}")
        except Exception as e:
            print(f"[Feeds] ERROR: {feed_id} ({url}) - {type(e).__name__}: {e}")

    hist.to_csv(hist_path, index=False)
    return sent


def run_heartbeat(now_utc, webhook_url, cfg):
    """Send a low-noise liveness ping at most once per configured interval."""
    if not webhook_url:
        return 0

    history_path = p(cfg["HEARTBEAT_HISTORY_CSV"])
    hist = read_csv_or_empty(history_path, columns=["last_sent_at"])
    hist = ensure_columns(hist, ["last_sent_at"])

    interval_hours = max(float(cfg.get("HEARTBEAT_HOURS", 24)), 1.0)
    now_iso = now_utc.isoformat(timespec="seconds")
    should_send = True

    if not hist.empty:
        last_raw = str(hist.iloc[0].get("last_sent_at", "") or "").strip()
        if last_raw:
            try:
                last_dt = datetime.fromisoformat(last_raw.replace("Z", "+00:00"))
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=timezone.utc)
                elapsed_hours = (now_utc - last_dt).total_seconds() / 3600
                should_send = elapsed_hours >= interval_hours
            except Exception:
                should_send = True

    if not should_send:
        return 0

    title = "Heartbeat: PokeBot is alive"
    msg = (
        f"Agent runtime check-in.\n"
        f"UTC: {now_iso}\n"
        f"Loop interval: 5 minutes"
    )
    send_discord(webhook_url, title, msg)

    out = pd.DataFrame([{"last_sent_at": now_iso}])
    out.to_csv(history_path, index=False)
    return 1


# ============================================================
# STORE RELIABILITY LOOKUP (from 04 output)
# ============================================================
def build_store_reliability(store_patterns: pd.DataFrame) -> dict:
    if store_patterns is None or store_patterns.empty:
        return {}
    sp = store_patterns.copy()
    if "zip_code" in sp.columns:
        sp["zip_code"] = sp["zip_code"].astype(str)
    if "store_reliability_score" not in sp.columns:
        return {}

    out = {}
    for _, r in sp.iterrows():
        try:
            out[(r["retailer"], str(r["zip_code"]))] = float(r["store_reliability_score"])
        except Exception:
            continue
    return out


# ============================================================
# BUILD drop_detail (agent version of Book 03 merge)
# ============================================================
def make_drop_detail(products, drop_events, drop_products):
    # ensure required columns exist (so we don't crash)
    drop_events = ensure_columns(drop_events, ["drop_id","retailer","source","zip_code","price_observed","observed_at","notes"])
    drop_products = ensure_columns(drop_products, ["drop_id","product_id"])
    products = ensure_columns(products, ["product_id","product_name","product_type","exclusive_flag","msrp"])

    de = drop_events.copy()
    de["zip_code"] = de["zip_code"].astype(str)

    # Normalize IDs and expand multi-product rows for safe merging
    de["drop_id"] = de["drop_id"].map(normalize_scalar_id)
    dp = expand_drop_products(drop_products)

    products = products.copy()
    products["product_id"] = products["product_id"].map(normalize_scalar_id)

    dd = (
        de[["drop_id","retailer","source","zip_code","price_observed","observed_at","notes"]]
        .merge(dp[["drop_id","product_id"]], on="drop_id", how="left")
        .merge(products[["product_id","product_name","product_type","exclusive_flag","msrp"]], on="product_id", how="left")
    )

    missing_mask = dd["product_name"].isna() & dd["product_id"].notna() & (dd["product_id"].astype(str).str.strip() != "")
    if missing_mask.any():
        missing_ids = sorted(dd.loc[missing_mask, "product_id"].astype(str).unique().tolist())
        print(f"[Drops] WARNING: Unmapped product_id(s) in data/drop_products.csv: {', '.join(missing_ids)}")
        dd.loc[missing_mask, "product_name"] = dd.loc[missing_mask, "product_id"].map(lambda x: f"Unmapped product_id {x}")

    return dd


# ============================================================
# SCORE (ZIP+MSRP dominant + store reliability bias)
# ============================================================
def score_drops(drop_detail, local_zips, store_reliability):
    def score_row(row):
        score = 0

        if bool(row.get("exclusive_flag")):
            score += 3

        if row.get("product_type") in ["ETB", "Booster Bundle", "Pin Collection", "Poster Collection"]:
            score += 2

        if row.get("retailer") == "Pokemon Center":
            score += 2

        if row.get("source") in ["email", "app", "in_person", "community"]:
            score += 1

        # ZIP + MSRP dominance
        try:
            if (str(row.get("zip_code")) in local_zips) and (float(row.get("price_observed")) <= float(row.get("msrp"))):
                score += 5
        except Exception:
            pass

        # store reliability bias (cap at +3)
        try:
            bonus = store_reliability.get((row.get("retailer"), str(row.get("zip_code"))), 0)
            score += min(float(bonus), 3)
        except Exception:
            pass

        return score

    scored = drop_detail.copy()
    scored["drop_score"] = scored.apply(score_row, axis=1)
    return scored.sort_values("drop_score", ascending=False)


# ============================================================
# SANITIZE values for Discord (no NaN/None strings)
# ============================================================
def sanitize_field(val):
    """Convert value to string, replacing NaN/None with empty string."""
    if val is None or pd.isna(val):
        return ""
    return str(val).strip()


# ============================================================
# DROP ALERTS + DEDUPE
# ============================================================
def run_drop_alerts(scored, webhook_url, cfg):
    hist_path = p(cfg["ALERT_HISTORY_CSV"])
    hist = read_csv_or_empty(hist_path, columns=["drop_id","product_id","alerted_at","drop_score"])
    
    # Ensure consistent data types in history
    for col in ["drop_id", "product_id"]:
        if col in hist.columns:
            hist[col] = hist[col].map(normalize_scalar_id)
    if "drop_score" in hist.columns:
        hist["drop_score"] = pd.to_numeric(hist["drop_score"], errors="coerce").fillna(0.0)

    def already(did, pid):
        if hist.empty:
            return False
        return ((hist["drop_id"] == str(did)) & (hist["product_id"] == str(pid))).any()

    sent = 0
    threshold = float(cfg["ALERT_SCORE_THRESHOLD"])

    for _, row in scored.iterrows():
        try:
            if float(row.get("drop_score", 0)) < threshold:
                continue
            if pd.isna(row.get("drop_id")) or pd.isna(row.get("product_id")):
                continue

            did = normalize_scalar_id(row["drop_id"])
            pid = normalize_scalar_id(row["product_id"])
            if already(did, pid):
                continue

            title = f"🔥 Pokebot Drop Alert (score {row['drop_score']})"
            msg = (
                f"Product: {sanitize_field(row.get('product_name'))}\n"
                f"Retailer: {sanitize_field(row.get('retailer'))}\n"
                f"ZIP: {sanitize_field(row.get('zip_code'))}\n"
                f"Price: {sanitize_field(row.get('price_observed'))} | MSRP: {sanitize_field(row.get('msrp'))}\n"
                f"Source: {sanitize_field(row.get('source'))}\n"
                f"Observed: {sanitize_field(row.get('observed_at'))}\n"
                f"Notes: {sanitize_field(row.get('notes'))}"
            )

            send_discord(webhook_url, title, msg)
            new_row = pd.DataFrame([{
                "drop_id": did, 
                "product_id": pid, 
                "alerted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), 
                "drop_score": float(row.get("drop_score", 0))
            }])
            hist = pd.concat([hist, new_row], ignore_index=True)
            sent += 1

        except Exception as e:
            print("Drop alert error:", e)

    hist.to_csv(hist_path, index=False)
    return sent


# ============================================================
# MAIN
# ============================================================
def run_once(session=None):
    cfg = load_config()
    webhook_url = cfg["DISCORD_WEBHOOK_URL"]
    enable_scheduled = bool(cfg.get("ENABLE_SCHEDULED", True))
    enable_feeds = bool(cfg.get("ENABLE_FEEDS", True))
    enable_heartbeat = bool(cfg.get("ENABLE_HEARTBEAT", True))

    now_utc = datetime.now(timezone.utc)

    if webhook_url and enable_scheduled:
        sched_sent = run_scheduled(now_utc, webhook_url, cfg)
        print(f"[Agent] Scheduled alerts sent: {sched_sent}")

    if webhook_url and enable_feeds:
        feeds_sent = run_feeds(webhook_url, cfg, session)
        print(f"[Agent] Feed alerts sent: {feeds_sent}")

    if webhook_url and enable_heartbeat:
        heartbeat_sent = run_heartbeat(now_utc, webhook_url, cfg)
        print(f"[Agent] Heartbeat alerts sent: {heartbeat_sent}")

    # ... scoring + drop alerts continue below ...

    # --- Load core data ---
    products = pd.read_csv(p(cfg["PRODUCTS_CSV"]))
    drop_events = pd.read_csv(p(cfg["DROP_EVENTS_CSV"]))
    drop_products = pd.read_csv(p(cfg["DROP_PRODUCTS_CSV"]))

    store_patterns = read_csv_or_empty(p(cfg["STORE_PATTERNS_CSV"]))
    store_reliability = build_store_reliability(store_patterns)

    # --- Time-based alerts + RSS alerts (only if webhook exists) ---


    # --- Score drops & persist scored output ---
    drop_detail = make_drop_detail(products, drop_events, drop_products)
    scored = score_drops(drop_detail, cfg["LOCAL_ZIPS"], store_reliability)

    scored_out = p(cfg["SCORED_DROPS_CSV"])
    scored.to_csv(scored_out, index=False)
    print(f"Scored drops saved: {scored_out} (rows={len(scored)})")

    # --- Drop-based alerts (only if webhook exists) ---
    if webhook_url:
        drop_sent = run_drop_alerts(scored, webhook_url, cfg)
        print("Drop alerts sent:", drop_sent)

    print("Agent run complete.")


def main():
    SLEEP_SECONDS = 300  # 5 minutes

    if not acquire_single_instance_lock():
        return
    
    cfg = load_config()
    if not cfg["DISCORD_WEBHOOK_URL"]:
        print("[Agent] WARNING: DISCORD_WEBHOOK_URL not set. No alerts will be sent.")
        print("[Agent] Set environment variable: $env:DISCORD_WEBHOOK_URL='your_webhook_url'")
    
    print("[Agent] Pokebot agent started (persistent mode)")
    print("[Agent] Creating persistent session for best performance...")
    
    session = create_resilient_session()

    while True:
        try:
            print(f"[Agent] Run started at {datetime.now(timezone.utc).isoformat()}")
            run_once(session)
        except Exception as e:
            print(f"[Agent] FATAL ERROR: {type(e).__name__}: {e}")

        print(f"[Agent] Sleeping for {SLEEP_SECONDS}s until next run...")
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()