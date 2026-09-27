# ============================================================
# DHAN BROKER LOGIC LAYER
# Replaces Kite-specific auth, funds, ltp, historical, orders,
# positions, and leverage gating.
# Policy:
#   - trade only if Dhan leverage >= 5x
#   - qty sizing uses fixed 5x only
# FIX: Added client-id header to all raw requests.post calls
# ============================================================

from __future__ import annotations

import os, io, sys, time, math, random, urllib.parse, re
import logging, traceback, threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional

import requests
import pandas as pd
from dotenv import load_dotenv
from dhanhq import DhanContext, dhanhq

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
load_dotenv(override=True)

DHAN_CLIENT_ID       = os.getenv("DHAN_CLIENT_ID", "").strip()
DHAN_ACCESS_TOKEN    = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
TELEGRAM_TOKEN       = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "").strip()
DEBUG                = False
MIN_CAPITAL_SLOT     = float(os.getenv("MIN_CAPITAL_SLOT", 100))
CHARTINK_COOKIE_RAW  = os.getenv("CHARTINK_COOKIE_RAW", "")
CHARTINK_CSRF_TOKEN  = os.getenv("CHARTINK_CSRF_TOKEN", "")

IST = ZoneInfo("Asia/Kolkata")

MARKET_OPEN  = (8, 55)
MARKET_CLOSE = (15, 30)
ENTRY_CUTOFF = (14, 55)
SQUAREOFF_AT = (15, 8)

MAX_POSITIONS = 1
EXIT_PENDING_TIMEOUT = 60
DHAN_LEVERAGE_THRESHOLD = 5.0
DHAN_FIXED_POLICY_LEVERAGE = 5.0
DHAN_LEVERAGE_CACHE_TTL = 20

# ─────────────────────────────────────────────────────────────
# DHAN SDK
# ─────────────────────────────────────────────────────────────
dhan_context = DhanContext(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
dhan = dhanhq(dhan_context)

# ─────────────────────────────────────────────────────────────
# DHAN HEADERS HELPER  ← CENTRALISED FIX
# ─────────────────────────────────────────────────────────────
def _dhan_headers() -> dict:
    """Returns the required headers for ALL Dhan v2 raw HTTP calls.
    Both access-token AND client-id are mandatory for marketfeed/ltp,
    charts/intraday, and margincalculator endpoints.
    """
    return {
        "access-token": DHAN_ACCESS_TOKEN,
        "client-id": DHAN_CLIENT_ID,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

# ─────────────────────────────────────────────────────────────
# STATE
# ─────────────────────────────────────────────────────────────
telegram_sent: dict = {}
tick_sizes: dict = {}
bot_positions: dict = {}
exit_pending: dict = {}
security_id_map: dict = {}
security_id_to_symbol: dict = {}   # reverse of security_id_map — O(1) lookups instead of O(n) scans
instrument_type_map: dict = {}
_dhan_leverage_cache: dict = {}
_ltp_cache: dict = {}          # {symbol: (timestamp, price)} — 3-second TTL
LTP_CACHE_TTL = 3.0            # seconds — keeps rapid same-cycle calls from hitting the API
_shutdown_event = threading.Event()
_chartink_empty_streak = 0
_CHARTINK_REFRESH_AFTER = 3
LTP_FAIL_COOLDOWN = 2.5
_ltp_fail_cache: dict = {}
_ltp_batch_lock = None  # optional: replace with threading.Lock() in main.py
_orders_cache: dict = {"ts": 0.0, "data": []}
_positions_cache: dict = {"ts": 0.0, "data": []}
BROKER_CACHE_TTL = 3.0  # seconds — collapses repeated fetch+filter calls within one maintenance tick
_excluded_raw = os.getenv("EXCLUDED_SYMBOLS", "").strip()
EXCLUDED_SYMBOLS = {s.strip().upper() for s in _excluded_raw.split(",")} if _excluded_raw else set()

IST = ZoneInfo("Asia/Kolkata")

TERMINAL_STATUSES = {
    "CANCELLED",
    "TRADED",
    "REJECTED",
    "EXPIRED",
    "CLOSED",
}

OPEN_ORDER_STATUSES = {
    "TRANSIT",
    "PENDING",
    "PART_TRADED",
    "TRIGGER_PENDING",
    "OPEN",
}

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
def _safe_stream_handler() -> logging.StreamHandler:
    if sys.platform == "win32":
        stream = io.TextIOWrapper(
            sys.stdout.buffer,
            encoding="utf-8",
            errors="replace",
            line_buffering=True,
        )
    else:
        stream = sys.stdout
    h = logging.StreamHandler(stream)
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    return h

logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        _safe_stream_handler(),
        logging.FileHandler("Dhan_bot.log", encoding="utf-8"),
    ],
)

def log(msg: str):
    logging.info(msg)

# ─────────────────────────────────────────────────────────────
# RETRY
# ─────────────────────────────────────────────────────────────
def with_retry(fn, *args, attempts=3, delay=3, fallback=None, label="", **kwargs):
    for i in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            log(f"[retry:{label}] attempt {i}/{attempts} failed: {e}")
            if i < attempts:
                time.sleep(delay)
    log(f"[retry:{label}] all attempts exhausted — returning fallback={fallback}")
    return fallback

# ─────────────────────────────────────────────────────────────
# TIME HELPERS
# ─────────────────────────────────────────────────────────────
def now_ist() -> datetime:
    return datetime.now(IST)

def _ist_hm() -> tuple:
    n = now_ist()
    return (n.hour, n.minute)

def _is_market_open() -> bool:
    if now_ist().weekday() >= 5:
        return False
    t = _ist_hm()
    return MARKET_OPEN <= t <= MARKET_CLOSE

def _is_entry_allowed() -> bool:
    return _is_market_open() and _ist_hm() < ENTRY_CUTOFF

def _seconds_until_market_open() -> float:
    now = now_ist()
    target = now.replace(hour=9, minute=15, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    while target.weekday() >= 5:
        target += timedelta(days=1)
    return max(0.0, (target - now).total_seconds())

def seconds_until(target: datetime) -> float:
    return max(0.0, (target - now_ist()).total_seconds())

def next_trailing_run_time() -> datetime:
    now = now_ist()
    candle_start_min = (now.minute // 5) * 5
    candle_start = now.replace(minute=candle_start_min, second=0, microsecond=0)
    candidate = candle_start + timedelta(minutes=5, seconds=90)
    if candidate <= now:
        candidate += timedelta(minutes=5)
    return candidate

# ─────────────────────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────────────────────
def send_telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    def _send():
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=8)
        log(f"[telegram] sent: {msg}")

    with_retry(_send, label="telegram")

def send_signal_telegram(symbol: str, side: str, extra: str = ""):
    key = (symbol.upper(), side.upper())
    now = now_ist()
    last = telegram_sent.get(key)
    if last and (now - last).total_seconds() < 1200:
        log(f"[telegram-dedup] {symbol} {side} suppressed ({int((now-last).total_seconds())}s ago)")
        return
    telegram_sent[key] = now
    send_telegram(f"Dhan SIGNAL | {side} | {symbol} | {now.strftime('%H:%M:%S')}{' | ' + extra if extra else ''}")

# ─────────────────────────────────────────────────────────────
# DHAN AUTH / INSTRUMENTS
# ─────────────────────────────────────────────────────────────
def validate_dhan_auth():
    if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
        msg = "❌ Bot startup FAILED: DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN missing in .env"
        log(msg)
        send_telegram(msg)
        shutdown("Missing Dhan credentials")
        return

    def _check():
        return dhan.get_fund_limits()

    resp = with_retry(_check, label="dhan_auth", fallback=None)
    if not isinstance(resp, dict):
        msg = "❌ Dhan auth failed: invalid get_fund_limits response"
        log(msg)
        send_telegram(msg)
        shutdown("Dhan auth invalid")
        return

    if resp.get("status") != "success":
        msg = f"❌ Dhan auth failed: {resp}"
        log(msg)
        send_telegram(msg)
        shutdown("Dhan auth failed")
        return

    # Additional market-data auth check — fundlimit passing does NOT
    # guarantee marketfeed/ltp will work (different header requirements).
    def _ltp_probe():
        r = requests.post(
            "https://api.dhan.co/v2/marketfeed/ltp",
            headers=_dhan_headers(),
            json={"NSE_EQ": [1333]},
            timeout=8,
        )
        r.raise_for_status()
        return r.json()
        
    probe = with_retry(_ltp_probe, label="dhan_ltp_probe", fallback="FAILED")
    if probe == "FAILED":
        msg = "❌ Dhan marketfeed/ltp probe failed — check DHAN_CLIENT_ID or token scope"
        log(msg)
        send_telegram(msg)
        shutdown("Dhan LTP auth failed")
        return

    log("[startup] Dhan auth OK (fundlimit + ltp probe passed)")

def load_dhan_instrument_master():
    global security_id_map, instrument_type_map, tick_sizes

    url = "https://images.dhan.co/api-data/api-scrip-master.csv"

    def _fetch():
        return pd.read_csv(
            url,
            low_memory=False,
            dtype={
                "SEM_EXM_EXCH_ID": "string",
                "SEM_SEGMENT": "string",
                "SEM_SMST_SECURITY_ID": "string",
                "SEM_TRADING_SYMBOL": "string",
                "SEM_INSTRUMENT_NAME": "string",
                "SEM_TICK_SIZE": "string",
            }
        )

    df = with_retry(_fetch, label="dhan_instruments", fallback=None)
    if df is None or df.empty:
        raise RuntimeError("Dhan instrument master unavailable")

    cols = {c.lower(): c for c in df.columns}
    exchange_col   = cols.get("sem_exm_exch_id")
    segment_col    = cols.get("sem_segment")
    symbol_col     = cols.get("sem_trading_symbol")
    security_col   = cols.get("sem_smst_security_id")
    instrument_col = cols.get("sem_instrument_name")
    tick_col       = cols.get("sem_tick_size") or cols.get("sem_tic_size")

    if not all([exchange_col, segment_col, symbol_col, security_col]):
        raise RuntimeError(f"Unexpected Dhan instrument CSV schema. Found columns: {list(df.columns)}")

    x = df.copy()

    x[exchange_col] = x[exchange_col].astype(str).str.strip().str.upper()
    x[segment_col] = x[segment_col].astype(str).str.strip().str.upper()
    if instrument_col:
        x[instrument_col] = x[instrument_col].astype(str).str.strip().str.upper()

    eq_mask = (x[exchange_col] == "NSE")

    if segment_col:
        eq_mask &= x[segment_col].isin({"E", "EQ"})

    if instrument_col:
        eq_mask &= (x[instrument_col] == "EQUITY")

    x = x[eq_mask].copy()

    security_id_map.clear()
    security_id_to_symbol.clear()
    instrument_type_map.clear()

    # Vectorized column extraction — avoids iterrows(), which boxes every
    # row into a pandas Series and is 50-100x slower than working on the
    # underlying numpy arrays directly for a file this size.
    syms = x[symbol_col].astype(str).str.strip().str.upper().to_numpy()
    sec_ids = x[security_col].astype(str).str.strip().to_numpy()

    if tick_col:
        raw_ticks = pd.to_numeric(x[tick_col], errors="coerce").to_numpy()
    else:
        raw_ticks = None

    for i in range(len(x)):
        sym = syms[i]
        sec_id = sec_ids[i]

        if not sym or not sec_id or sym == "NAN" or sec_id == "NAN":
            continue

        security_id_map[sym] = sec_id
        security_id_to_symbol[sec_id] = sym
        instrument_type_map[sym] = "NSE_EQ"

        if raw_ticks is not None:
            raw_tick = raw_ticks[i]
            if raw_tick == raw_tick:  # not NaN
                tick = raw_tick / 100 if raw_tick >= 1 else raw_tick
                tick_sizes[sym] = tick
                if sym == "SYNGENE":
                    log(f"[tick-load] SYNGENE raw_tick={raw_tick} normalized_tick={tick} security_id={sec_id}")

    log(f"[startup] Dhan NSE_EQ instruments loaded: {len(security_id_map)} symbols")

def get_security_id(symbol: str) -> str:
    return security_id_map.get(symbol.upper(), "")

# ─────────────────────────────────────────────────────────────
# DHAN FUNDS / POSITIONS / ORDERS
# ─────────────────────────────────────────────────────────────
def get_available_capital() -> float:
    def _fetch():
        resp = dhan.get_fund_limits()
        if resp.get("status") != "success":
            raise RuntimeError(f"fund_limits failed: {resp}")
        data = resp.get("data", {})
        capital = float(data.get("availabelBalance", 0) or 0)
        log(f"[capital] availabelBalance={capital}")
        return capital

    return with_retry(_fetch, label="capital", fallback=0.0) or 0.0

def get_all_orders(max_age: float = BROKER_CACHE_TTL) -> list:
    now_ts = time.time()
    if max_age > 0 and (now_ts - _orders_cache["ts"]) <= max_age:
        return _orders_cache["data"]

    def _fetch():
        resp = dhan.get_order_list()
        if resp.get("status") != "success":
            raise RuntimeError(f"get_order_list failed: {resp}")
        return resp.get("data", []) or []

    data = with_retry(_fetch, label="order_list", fallback=[])
    _orders_cache["ts"] = now_ts
    _orders_cache["data"] = data
    return data

def get_open_intraday_positions(max_age: float = BROKER_CACHE_TTL) -> list:
    now_ts = time.time()
    if max_age > 0 and (now_ts - _positions_cache["ts"]) <= max_age:
        return _positions_cache["data"]

    def _fetch():
        resp = dhan.get_positions()
        if resp.get("status") != "success":
            raise RuntimeError(f"get_positions failed: {resp}")
        rows = resp.get("data", []) or []
        out = []
        for p in rows:
            product = str(p.get("productType", "")).upper()
            net_qty = int(float(p.get("netQty", 0) or 0))
            if product in {"INTRADAY", "INTRA", "TRADE"} and net_qty != 0:
                out.append(p)
        return out

    data = with_retry(_fetch, label="positions", fallback=[])
    _positions_cache["ts"] = now_ts
    _positions_cache["data"] = data
    return data

def get_active_slm_orders() -> list:
    out = []
    for o in get_all_orders():
        order_type = str(o.get("orderType", "")).upper()
        status = str(o.get("orderStatus", "")).upper()
        if order_type in {"SL", "STOP_LOSS"} and status not in TERMINAL_STATUSES:
            out.append(o)
    return out

def get_active_slm_for_symbol(symbol: str) -> list:
    symbol = symbol.upper()
    out = []
    for o in get_active_slm_orders():
        ts = str(o.get("tradingSymbol", "") or o.get("securityId", "")).upper()
        if ts == symbol or str(o.get("securityId", "")) == get_security_id(symbol):
            out.append(o)
    return out

# ─────────────────────────────────────────────────────────────
# DHAN MARKET DATA
# ─────────────────────────────────────────────────────────────
def get_ltp(symbol: str, max_age: float = LTP_CACHE_TTL) -> float:
    """Fetch LTP with short cache and brief cooldown after 429 failures."""
    sym = symbol.upper()
    now_ts = time.time()

    cached = _ltp_cache.get(sym)
    if cached and max_age > 0 and (now_ts - cached[0]) <= max_age:
        return cached[1]

    fail_ts = _ltp_fail_cache.get(sym)
    if fail_ts and (now_ts - fail_ts) < LTP_FAIL_COOLDOWN:
        return cached[1] if cached else 0.0

    security_id = get_security_id(sym)
    if not security_id:
        return 0.0

    payload = {"NSE_EQ": [int(security_id)]}

    def _fetch():
        r = requests.post(
            "https://api.dhan.co/v2/marketfeed/ltp",
            headers=_dhan_headers(),
            json=payload,
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        item = data.get("data", {}).get("NSE_EQ", {}).get(str(security_id), {})
        return float(item.get("last_price", 0) or 0)

    try:
        price = with_retry(_fetch, label=f"ltp:{sym}", fallback=0.0) or 0.0
    except Exception:
        price = 0.0

    if price > 0:
        _ltp_cache[sym] = (time.time(), price)
        _ltp_fail_cache.pop(sym, None)
        return price

    _ltp_fail_cache[sym] = now_ts
    return cached[1] if cached else 0.0


def _normalize_intraday_df(data: dict) -> pd.DataFrame:
    rows = data.get("data", data)

    if isinstance(rows, list):
        df = pd.DataFrame(rows)

        if "start_Time" in df.columns:
            df["date"] = pd.to_datetime(df["start_Time"], errors="coerce")
            if df["date"].dt.tz is None:
                df["date"] = df["date"].dt.tz_localize(IST)
            else:
                df["date"] = df["date"].dt.tz_convert(IST)
            return df

        if "timestamp" in df.columns:
            ts = pd.to_numeric(df["timestamp"], errors="coerce")
            df["date"] = pd.to_datetime(ts, unit="s", utc=True, errors="coerce").dt.tz_convert(IST)
            return df

        return pd.DataFrame()

    if isinstance(rows, dict):
        keys = {k.lower(): k for k in rows.keys()}
        ts_col = keys.get("timestamp") or keys.get("start_time")
        o_col = keys.get("open")
        h_col = keys.get("high")
        l_col = keys.get("low")
        c_col = keys.get("close")
        v_col = keys.get("volume")

        if ts_col and o_col and h_col and l_col and c_col:
            ts = pd.to_numeric(rows[ts_col], errors="coerce")
            dt = pd.to_datetime(ts, unit="s", utc=True, errors="coerce").tz_convert(IST)

            return pd.DataFrame({
                "date": dt,
                "open": pd.to_numeric(rows[o_col], errors="coerce"),
                "high": pd.to_numeric(rows[h_col], errors="coerce"),
                "low": pd.to_numeric(rows[l_col], errors="coerce"),
                "close": pd.to_numeric(rows[c_col], errors="coerce"),
                "volume": pd.to_numeric(rows.get(v_col, [0] * len(rows[o_col])), errors="coerce"),
            })

    return pd.DataFrame()


def get_ltp_batch(symbols: list[str], max_age: float = LTP_CACHE_TTL) -> dict[str, float]:
    """Batch LTP fetch to reduce 429 bursts. Returns {SYMBOL: price}."""
    now_ts = time.time()
    out: dict[str, float] = {}
    missing: list[tuple[str, str]] = []

    for symbol in symbols:
        sym = str(symbol).upper()
        cached = _ltp_cache.get(sym)
        if cached and max_age > 0 and (now_ts - cached[0]) <= max_age:
            out[sym] = cached[1]
            continue
        security_id = get_security_id(sym)
        if not security_id:
            out[sym] = 0.0
            continue
        fail_ts = _ltp_fail_cache.get(sym)
        if fail_ts and (now_ts - fail_ts) < LTP_FAIL_COOLDOWN:
            out[sym] = cached[1] if cached else 0.0
            continue
        missing.append((sym, security_id))

    if not missing:
        return out

    payload = {"NSE_EQ": [int(sec_id) for _, sec_id in missing]}

    def _fetch():
        r = requests.post(
            "https://api.dhan.co/v2/marketfeed/ltp",
            headers=_dhan_headers(),
            json=payload,
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    data = with_retry(_fetch, label="ltp_batch", fallback=None)
    node = (data or {}).get("data", {}).get("NSE_EQ", {}) if isinstance(data, dict) else {}

    for sym, sec_id in missing:
        item = node.get(str(sec_id), {}) if isinstance(node, dict) else {}
        price = float(item.get("last_price", 0) or 0)
        if price > 0:
            _ltp_cache[sym] = (time.time(), price)
            _ltp_fail_cache.pop(sym, None)
            out[sym] = price
        else:
            _ltp_fail_cache[sym] = now_ts
            cached = _ltp_cache.get(sym)
            out[sym] = cached[1] if cached else 0.0

    return out


def get_last_completed_5m_candle(symbol: str) -> Optional[dict]:
    candles = get_last_n_five_min_candles(symbol, 1)
    return candles[-1] if candles else None

# ─────────────────────────────────────────────────────────────
# DHAN LEVERAGE / QTY POLICY
# ─────────────────────────────────────────────────────────────
def parse_dhan_leverage(raw) -> float:
    if raw is None:
        return 0.0
    s = str(raw).strip().lower().replace("x", "").strip()
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return 0.0
    try:
        return float(m.group(1))
    except Exception:
        return 0.0

def get_last_n_five_min_candles(symbol: str, n: int = 3) -> list:
    security_id = get_security_id(symbol)
    if not security_id:
        return []

    now_ = now_ist()
    current_candle_start = now_.replace(
        minute=(now_.minute // 5) * 5,
        second=0,
        microsecond=0,
    )
    to_dt = current_candle_start
    from_dt = to_dt - timedelta(minutes=5 * (n + 5))

    payload = {
        "securityId": str(security_id),
        "exchangeSegment": "NSE_EQ",
        "instrument": "EQUITY",
        "interval": "5",
        "oi": False,
        "fromDate": from_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "toDate": to_dt.strftime("%Y-%m-%d %H:%M:%S"),
    }

    def _fetch():
        r = requests.post(
            "https://api.dhan.co/v2/charts/intraday",
            headers=_dhan_headers(),
            json=payload,
            timeout=15,
        )
        # log(f"[candles-debug] {symbol} status={r.status_code} body={r.text[:800]}")
        r.raise_for_status()
        return r.json()

    raw = with_retry(_fetch, label=f"candles:{symbol}", fallback=None)
    if not isinstance(raw, dict):
        return []

    df = _normalize_intraday_df(raw)
    if df.empty:
        log(f"[candles-debug] {symbol} normalize returned empty")
        return []

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "open", "high", "low", "close"])
    if df.empty:
        log(f"[candles-debug] {symbol} dataframe empty after dropna")
        return []

    if df["date"].dt.tz is None:
        df["date"] = df["date"].dt.tz_localize(IST)
    else:
        df["date"] = df["date"].dt.tz_convert(IST)

    df = df.sort_values("date")
    # log(f"[candles-debug] {symbol} parsed_tail={df['date'].tail(5).tolist()} current_candle_start={current_candle_start}")

    df = df[df["date"] < current_candle_start]
    if df.empty:
        log(f"[candles-debug] {symbol} empty after current-candle filter")
        return []

    agg = (
        df.set_index("date")
        .resample("5min", label="left", closed="left")
        .agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        })
        .dropna(subset=["open", "high", "low", "close"])
        .reset_index()
    )

    out = []
    for _, row in agg.tail(n).iterrows():
        out.append({
            "date": row["date"].to_pydatetime(),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"] or 0),
        })

    # log(f"[candles-debug] {symbol} candles_out={len(out)} last={out[-1] if out else None}")
    return out


def get_dhan_leverage(symbol: str, side: str) -> Optional[float]:
    key = (symbol.upper(), side.upper())
    now_ts = time.time()
    cached = _dhan_leverage_cache.get(key)
    if cached and now_ts - cached["ts"] <= DHAN_LEVERAGE_CACHE_TTL:
        return cached["lev"]

    security_id = get_security_id(symbol)
    if not security_id:
        log(f"[dhan-lev] {symbol} security_id unavailable")
        return None

    ltp = get_ltp(symbol)
    if ltp <= 0:
        log(f"[dhan-lev] {symbol} LTP unavailable due to quote failure/rate limit")
        return None

    payload = {
        "dhanClientId": DHAN_CLIENT_ID,
        "exchangeSegment": "NSE_EQ",
        "transactionType": "BUY" if side.upper() == "BUY" else "SELL",
        "quantity": 1,
        "productType": "INTRADAY",
        "securityId": security_id,
        "price": ltp,
        "triggerPrice": 0,
    }

    def _fetch():
        r = requests.post(
            "https://api.dhan.co/v2/margincalculator",
            headers=_dhan_headers(),
            json=payload,
            timeout=12,
        )
        r.raise_for_status()
        return r.json()

    data = with_retry(_fetch, label=f"dhan_lev:{symbol}", fallback=None)
    if not isinstance(data, dict):
        return None

    lev = parse_dhan_leverage(data.get("leverage"))
    _dhan_leverage_cache[key] = {"ts": now_ts, "lev": lev}
    log(f"[dhan-lev] {symbol} side={side} leverage={lev}x")
    return lev


def is_symbol_allowed_by_dhan_5x(symbol: str, side: str) -> bool:
    lev = get_dhan_leverage(symbol, side)
    if lev is None:
        log(f"[order-gate] {symbol} leverage unknown — quote API failed or margin API unavailable")
        return False
    if lev < DHAN_LEVERAGE_THRESHOLD:
        log(f"[order-gate] {symbol} Dhan leverage {lev}x < {DHAN_LEVERAGE_THRESHOLD}x — skipped")
        return False
    return True


def compute_qty(symbol: str, capital: float) -> int:
    ltp = get_ltp(symbol)
    if ltp <= 0:
        return 0
    buying_power = (capital * 0.85) * DHAN_FIXED_POLICY_LEVERAGE
    qty = max(1, int(buying_power / ltp))
    log(
        f"[qty] {symbol} ltp={ltp} capital={capital:.0f} "
        f"leverage_policy={DHAN_FIXED_POLICY_LEVERAGE}x buying_power={buying_power:.0f} qty={qty}"
    )
    return qty

# ─────────────────────────────────────────────────────────────
# DHAN ORDERS
# ─────────────────────────────────────────────────────────────
def _dhan_tx(side: str) -> str:
    return dhan.BUY if side.upper() == "BUY" else dhan.SELL

def _opposite_side(side: str) -> str:
    return "SELL" if side.upper() == "BUY" else "BUY"

def round_to_tick(price: float, symbol: str) -> float:
    tick = tick_sizes.get(symbol.upper(), 0.05)
    out = round(math.floor(price / tick) * tick, 2)
    log(f"[tick] {symbol} price={price} tick={tick} rounded={out}")
    return out


def _place_entry_order(symbol: str, side: str, qty: int) -> Optional[str]:
    security_id = get_security_id(symbol)
    if not security_id:
        log(f"[order] {symbol} security_id unavailable")
        return None

    ltp = get_ltp(symbol)
    if ltp <= 0:
        log(f"[order] {symbol} LTP unavailable")
        return None

    tx = _dhan_tx(side)
    price = round_to_tick(ltp * 1.01, symbol) if side.upper() == "BUY" else round_to_tick(ltp * 0.99, symbol)

    def _place():
        return dhan.place_order(
            security_id=security_id,
            exchange_segment=dhan.NSE,
            transaction_type=tx,
            quantity=qty,
            order_type=dhan.LIMIT,
            product_type=dhan.INTRA,
            price=price,
            validity=dhan.DAY,
        )

    resp = with_retry(_place, label=f"place:{symbol}", fallback=None)
    if isinstance(resp, dict):
        if resp.get("status") == "success":
            oid = str(resp.get("data", {}).get("orderId") or resp.get("data", {}).get("order_id") or "")
            log(f"[order] placed {side} {qty}x{symbol} @ ~{price} order_id={oid}")
            return oid or None
        log(f"[order] place failed {symbol}: {resp}")
        return None

    oid = str(resp) if resp else None
    if oid:
        log(f"[order] placed {side} {qty}x{symbol} @ ~{price} order_id={oid}")
    return oid


def _sl_limit_from_trigger(symbol: str, side: str, trigger_price: float) -> tuple[float, float]:
    tick = tick_sizes.get(symbol.upper(), 0.05)
    trigger_price = round_to_tick(trigger_price, symbol)

    if side.upper() == "BUY":
        # BUY SL, used to cover a short position
        limit_price = round(trigger_price + tick, 2)
    else:
        # SELL SL, used to exit a long position
        limit_price = round(max(tick, trigger_price - tick), 2)

    return trigger_price, limit_price

def _place_sl_order(symbol: str, side: str, qty: int, trigger_price: float) -> Optional[str]:
    security_id = get_security_id(symbol)
    if not security_id:
        return None

    # For BUY SL (covering a short): trigger must be > LTP.
    # For SELL SL (exiting a long): trigger must be < LTP.
    # Dhan rejects with DH-905 if the trigger has already been crossed.
    ltp_now = get_ltp(symbol)
    if ltp_now > 0:
        # side here is the EXIT side (BUY to close short, SELL to close long)
        entry_side = "SELL" if side.upper() == "BUY" else "BUY"
        if entry_side == "BUY" and trigger_price >= ltp_now:
            log(f"[sl] {symbol} SELL SL trigger {trigger_price} >= LTP {ltp_now} — skipping invalid SL")
            return None
        if entry_side == "SELL" and trigger_price <= ltp_now:
            log(f"[sl] {symbol} BUY SL trigger {trigger_price} <= LTP {ltp_now} — skipping invalid SL")
            return None

    tx = _dhan_tx(side)
    trigger_price, limit_price = _sl_limit_from_trigger(symbol, side, trigger_price)
    log(
        f"[sl-debug] {symbol} side={side} input_trigger={trigger_price} "
        f"rounded_trigger={round_to_tick(trigger_price, symbol)} "
        f"ltp_now={ltp_now}"
    )

    def _place():
        return dhan.place_order(
            security_id=security_id,
            exchange_segment=dhan.NSE,
            transaction_type=tx,
            quantity=qty,
            order_type=dhan.SL,
            product_type=dhan.INTRA,
            price=limit_price,
            trigger_price=trigger_price,
            validity=dhan.DAY,
        )

    resp = with_retry(_place, label=f"sl:{symbol}", fallback=None)
    if isinstance(resp, dict):
        if resp.get("status") == "success":
            oid = str(resp.get("data", {}).get("orderId") or resp.get("data", {}).get("order_id") or "")
            log(f"[sl] placed {side} SL {qty}x{symbol} trigger={trigger_price} limit={limit_price} order_id={oid}")
            return oid or None
        log(f"[sl] place failed {symbol}: {resp}")
        return None
    oid = str(resp) if resp else None
    if oid:
        log(f"[sl] placed {side} SL {qty}x{symbol} trigger={trigger_price} limit={limit_price} order_id={oid}")
    return oid


def modify_sl_order(order_id: str, symbol: str, side: str, qty: int, new_trigger: float) -> bool:
    trigger_price, limit_price = _sl_limit_from_trigger(symbol, side, new_trigger)

    def _get_order_status():
        r = requests.get(
            f"https://api.dhan.co/v2/orders/{order_id}",
            headers=_dhan_headers(),
            timeout=8,
        )
        log(f"[modify_sl] GET order {order_id} status={r.status_code} body={r.text[:1000]}")
        r.raise_for_status()
        return r.json()

    order_info = with_retry(_get_order_status, label=f"get_order:{order_id}", fallback=None)

    if isinstance(order_info, list):
        order_info = order_info[0] if order_info else None
    elif isinstance(order_info, dict) and isinstance(order_info.get("data"), list):
        data_list = order_info.get("data") or []
        order_info = data_list[0] if data_list else None
    elif isinstance(order_info, dict) and isinstance(order_info.get("data"), dict):
        order_info = order_info["data"]

    if not isinstance(order_info, dict):
        log(f"[modify_sl] {order_id} {symbol} order fetch failed: {order_info}")
        return False

    log(
        f"[modify_sl] parsed order {order_id}: "
        f"status={order_info.get('orderStatus')} "
        f"trigger={order_info.get('triggerPrice')} "
        f"price={order_info.get('price')}"
    )

    status = str(order_info.get("orderStatus", "")).upper()
    if status not in {"PENDING", "OPEN", "TRIGGER_PENDING", "PART_TRADED"}:
        log(f"[modify_sl] {order_id} {symbol} status={status} — not modifiable, skipping")
        return False

    current_trigger = float(order_info.get("triggerPrice", 0) or 0)
    current_price = float(order_info.get("price", 0) or 0)

    if round(current_trigger, 2) == round(trigger_price, 2) and round(current_price, 2) == round(limit_price, 2):
        log(f"[modify_sl] {order_id} {symbol} unchanged trigger={trigger_price} limit={limit_price} — skip modify")
        return True

    def _modify():
        payload = {
            "dhanClientId": DHAN_CLIENT_ID,
            "orderId": str(order_id),
            "orderType": "STOP_LOSS",
            "quantity": qty,
            "price": limit_price,
            "triggerPrice": trigger_price,
            "validity": "DAY",
        }
        r = requests.put(
            f"https://api.dhan.co/v2/orders/{order_id}",
            headers=_dhan_headers(),
            json=payload,
            timeout=10,
        )
        log(f"[modify_sl] PUT order {order_id} status={r.status_code} body={r.text[:1000]}")
        if not r.ok:
            logging.error(f"[modify_sl] HTTP {r.status_code} response: {r.text}")
            r.raise_for_status()
        return r.json()

    resp = with_retry(_modify, label=f"modify_sl:{order_id}", fallback=None)

    if isinstance(resp, dict):
        resp_status = str(resp.get("orderStatus", "")).upper()
        if resp.get("status") == "success" or resp_status in {"TRANSIT", "PENDING", "OPEN", "TRIGGER_PENDING", "PART_TRADED"}:
            log(f"[modify_sl] accepted {order_id} {symbol} new_trigger={trigger_price} limit={limit_price} resp={resp}")
            return True

    log(f"[modify_sl] failed {order_id} {symbol}: {resp}")
    return False

def _cancel_order(order_id: str, label: str = "") -> bool:
    def _cancel():
        return dhan.cancel_order(order_id)

    resp = with_retry(_cancel, label=label or f"cancel:{order_id}", fallback=None)
    return isinstance(resp, dict) and resp.get("status") == "success"

def _exit_position_market(symbol: str, side_to_exit: str, qty: int, reason: str = ""):
    log(f"[exit] {symbol} reason={reason}")
    exit_pending[symbol] = now_ist()

    for slm in get_active_slm_for_symbol(symbol):
        oid = str(slm.get("orderId") or slm.get("order_id") or "")
        if oid:
            _cancel_order(oid, label=f"cancel_sl:{symbol}")

    oid = _place_entry_order(symbol, side_to_exit, qty)
    bot_positions.pop(symbol, None)

    log(f"[exit] {symbol} exit_order={oid}")

# ─────────────────────────────────────────────────────────────
# MAIN ORDER FLOW
# ─────────────────────────────────────────────────────────────
def place_order(signal: dict):
    symbol = signal["symbol"].upper()
    side = signal["side"].upper()

    # --- Pre-flight Checks ---
    if not _is_entry_allowed(): return
    if not get_security_id(symbol): return
    if symbol in EXCLUDED_SYMBOLS: return

    existing = bot_positions.get(symbol)
    if existing:
        if existing["side"] == side:
            return
        log(f"[reverse] {symbol} in {existing['side']} — opposite signal {side} received, exiting and reversing")
        _exit_position_market(
            symbol,
            existing["exit_side"],
            existing["qty"],
            reason=f"reverse signal {existing['side']} -> {side}",
        )

    # ENFORCE MAX_POSITIONS (count both confirmed + in-flight entries)
    if len(bot_positions) >= MAX_POSITIONS:
        log(f"[order-gate] {symbol} skipped — MAX_POSITIONS={MAX_POSITIONS} reached ({list(bot_positions.keys())})")
        return

    if not is_symbol_allowed_by_dhan_5x(symbol, side): return

    # Capital and Qty
    capital = get_available_capital()
    if capital <= MIN_CAPITAL_SLOT: return
    qty = compute_qty(symbol, capital)
    if qty <= 0: return

    # Candle Data
    candle = get_last_completed_5m_candle(symbol)
    if not candle: return
    sl_trigger = round_to_tick(candle["low"] if side == "BUY" else candle["high"], symbol)

    # Place Entry Order
    entry_oid = _place_entry_order(symbol, side, qty)
    if not entry_oid: return

    # send_signal_telegram(f"Dhan ", symbol, side, extra=f"qty={qty} sl={sl_trigger}")
    
    # --- Optimized Execution Flow ---
    log(f"[order] Entry placed {entry_oid}. Waiting for fill confirmation...")
    
    avg_price = 0.0
    # Poll for fill status (max 3 seconds)
    for _ in range(6): 
        time.sleep(0.5)
        resp = dhan.get_order_list()
        # Find just our order
        order = next((o for o in resp.get("data", []) if str(o.get("orderId")) == entry_oid), None)
        if order and order.get("orderStatus") == "TRADED":
            avg_price = float(order.get("averageTradedPrice", 0))
            break
    
    if avg_price == 0:
        log(f"[order] {symbol} order did not fill or API error. Aborting SL placement.")
        return

    # Place SL and Target logic
    exit_side = _opposite_side(side)
    sl_oid = _place_sl_order(symbol, exit_side, qty, sl_trigger)
    
    target_price = round_to_tick(avg_price * 1.012 if side == "BUY" else avg_price * 0.988, symbol)

    bot_positions[symbol] = {
        "side": side,
        "qty": qty,
        "avg_price": avg_price,
        "entry_order_id": entry_oid,
        "sl_order_id": sl_oid,
        "exit_side": exit_side,
        "target_price": target_price # Added to dictionary for easy access by monitor
    }

    log(f"[order] OK {symbol} | Price={avg_price} | SL={sl_trigger} | Target={target_price}")
    
# ─────────────────────────────────────────────────────────────
# TRAILING
# ─────────────────────────────────────────────────────────────
def trailing_sl():
    log("[trailing] running...")
    
    for symbol, pos in list(bot_positions.items()):
        side = pos["side"]
        qty = pos["qty"]
        exit_side = pos["exit_side"]

        candle = get_last_completed_5m_candle(symbol)
        if not candle: continue

        ltp = get_ltp(symbol)
        if ltp <= 0: continue

        new_trigger = round_to_tick(candle["low"] if side == "BUY" else candle["high"], symbol)

        # Initialize defaults to prevent UnboundLocalError
        current_trigger = 0.0
        oid = None

        sl_orders = get_active_slm_for_symbol(symbol)
        if not sl_orders:
            log(f"[trailing] {symbol} no active SL found — placing fresh protective SL")
            new_oid = _place_sl_order(symbol, exit_side, qty, new_trigger)
            if new_oid:
                bot_positions[symbol]["sl_order_id"] = new_oid
            continue

        if sl_orders:
            sl_orders = sorted(sl_orders, key=lambda o: str(o.get("orderId") or o.get("order_id") or ""))
            sl = sl_orders[-1]
            oid = str(sl.get("orderId") or sl.get("order_id") or "")
            current_trigger = float(sl.get("triggerPrice", 0) or 0)

        # Logic comparison using initialized values
        if side == "BUY" and (new_trigger <= current_trigger or new_trigger >= ltp):
            continue
        if side == "SELL" and (current_trigger > 0 and new_trigger >= current_trigger or new_trigger <= ltp):
            continue
        
        if oid and modify_sl_order(oid, symbol, exit_side, qty, new_trigger):
            bot_positions[symbol]["sl_order_id"] = oid

# ─────────────────────────────────────────────────────────────
# REBUILD / SYNC / MANUAL DETECT
# ─────────────────────────────────────────────────────────────

def _position_symbol(p: dict) -> str:
    sec = str(p.get("securityId", "")).strip()
    sym = security_id_to_symbol.get(sec)
    if sym:
        return sym
    return str(p.get("tradingSymbol", "")).upper()

def _rebuild_positions_from_broker():
    log("[rebuild] reconstructing bot_positions from Dhan broker state...")
    open_pos = get_open_intraday_positions()
    active_sls = get_active_slm_orders()

    sl_map = {}
    for o in active_sls:
        sec = str(o.get("securityId", "")).strip()
        sl_map[sec] = o

    rebuilt = 0
    for pos in open_pos:
        symbol = _position_symbol(pos)
        net_qty = int(float(pos.get("netQty", 0) or 0))
        qty = abs(net_qty)
        side = "BUY" if net_qty > 0 else "SELL"
        exit_side = _opposite_side(side)
        sec = str(pos.get("securityId", "")).strip()
        sl_oid = None
        if sec in sl_map:
            sl_oid = str(sl_map[sec].get("orderId") or sl_map[sec].get("order_id") or "")

        bot_positions[symbol] = {
            "side": side,
            "qty": qty,
            "entry_order_id": "RESTORED",
            "sl_order_id": sl_oid,
            "exit_side": exit_side,
            "target_price": None, # Cannot infer strict 1% without historical execution data
        }
        rebuilt += 1
        log(f"[rebuild] restored {symbol} side={side} qty={qty} sl_order_id={sl_oid}")

    log(f"[rebuild] done — restored {rebuilt} positions: {list(bot_positions.keys())}")

def detect_manual_entry():
    log("[manual-detect] checking...")
    now = now_ist()
    open_pos = get_open_intraday_positions()
    active_sls = get_active_slm_orders()
    active_sl_sec_ids = {str(o.get("securityId", "")).strip() for o in active_sls}

    for pos in open_pos:
        symbol = _position_symbol(pos)
        net_qty = int(float(pos.get("netQty", 0) or 0))
        qty = abs(net_qty)
        if qty == 0: continue
        
        # Check if already tracked
        if symbol in bot_positions: continue
        
        sec_id = str(pos.get("securityId", "")).strip()
        if sec_id in active_sl_sec_ids: continue

        side = "BUY" if net_qty > 0 else "SELL"
        exit_side = _opposite_side(side)
        
        # Calculate target based on current price for manual trades
        ltp = get_ltp(symbol)
        target_price = round_to_tick(ltp * 1.01, symbol) if side == "BUY" else round_to_tick(ltp * 0.99, symbol)

        # Get SL trigger from candle
        candle = get_last_completed_5m_candle(symbol)
        if not candle: continue
        trigger = candle["low"] if side == "BUY" else candle["high"]

        sl_oid = _place_sl_order(symbol, exit_side, qty, trigger)
        
        bot_positions[symbol] = {
            "side": side,
            "qty": qty,
            "avg_price": ltp, # Approximate avg_price for manual
            "entry_order_id": "MANUAL",
            "sl_order_id": sl_oid,
            "exit_side": exit_side,
            "target_price": target_price,
        }
        log(f"[manual-detect] Registered manual {symbol} with target {target_price}")

def cancel_stale_sl_orders():
    log("[stale-sl] checking...")
    open_pos = get_open_intraday_positions()
    open_sec_ids = {str(p.get("securityId", "")).strip() for p in open_pos}
    active_sls = get_active_slm_orders()

    sl_by_sec = {}
    for o in active_sls:
        sec = str(o.get("securityId", "")).strip()
        sl_by_sec.setdefault(sec, []).append(o)

    for sec, orders in sl_by_sec.items():
        if sec not in open_sec_ids:
            for o in orders:
                oid = str(o.get("orderId") or o.get("order_id") or "")
                if oid:
                    log(f"[stale-sl] sec={sec} no open position — cancelling {oid}")
                    _cancel_order(oid, label=f"cancel_stale_sl:{sec}")
            continue

        if len(orders) > 1:
            sorted_orders = sorted(orders, key=lambda x: str(x.get("orderId") or x.get("order_id") or ""))
            keep = sorted_orders[-1]
            cancel = sorted_orders[:-1]
            keep_oid = str(keep.get("orderId") or keep.get("order_id") or "")
            log(f"[stale-sl] sec={sec} has {len(orders)} SLs — keeping {keep_oid}")
            for o in cancel:
                oid = str(o.get("orderId") or o.get("order_id") or "")
                if oid:
                    _cancel_order(oid, label=f"cancel_dup_sl:{sec}")

def sync_bot_positions():
    open_pos = get_open_intraday_positions()
    open_symbols = {_position_symbol(p) for p in open_pos}
    now = now_ist()

    for sym in list(exit_pending.keys()):
        age = (now - exit_pending[sym]).total_seconds()
        if sym not in open_symbols or age > EXIT_PENDING_TIMEOUT:
            exit_pending.pop(sym, None)

    for symbol in list(bot_positions.keys()):
        if symbol not in open_symbols:
            log(f"[sync] {symbol} position closed externally — removing")
            bot_positions.pop(symbol, None)
            exit_pending.pop(symbol, None)

    by_symbol = {_position_symbol(p): p for p in open_pos}
    for symbol, pos in list(bot_positions.items()):
        actual = by_symbol.get(symbol)
        if not actual:
            continue
        actual_qty = abs(int(float(actual.get("netQty", 0) or 0)))
        if pos["qty"] != actual_qty:
            log(f"[sync] {symbol} qty mismatch tracked={pos['qty']} actual={actual_qty}")
            bot_positions[symbol]["qty"] = actual_qty

def thread_profit_monitor():
    log("[thread] profit monitor started")
    while not _shutdown_event.is_set():
        try:
            if _is_market_open():
                check_profit_booking()
        except Exception as e:
            log(f"[thread] profit monitor error: {e}")
        _shutdown_event.wait(timeout=5)

def check_profit_booking():
    """Monitors active positions and books 1% profit at market."""
    # Fetch symbols to avoid redundant individual API calls
    active_symbols = list(bot_positions.keys())
    if not active_symbols:
        return

    ltp_map = get_ltp_batch(active_symbols)

    for symbol, pos in list(bot_positions.items()):
        ltp = ltp_map.get(symbol, 0)
        avg_price = pos.get("avg_price", 0)
        
        if ltp <= 0 or avg_price <= 0:
            continue

        # Calculate 1% target based on entry side
        target_price = avg_price * 1.02 if pos["side"] == "BUY" else avg_price * 0.98
        
        reached = (pos["side"] == "BUY" and ltp >= target_price) or \
                  (pos["side"] == "SELL" and ltp <= target_price)
                  
        if reached:
            log(f"[profit] {symbol} hit 1% target (LTP: {ltp}, Entry: {avg_price}). Exiting.")
            _exit_position_market(symbol, pos["exit_side"], pos["qty"], reason="1% profit target")

# ─────────────────────────────────────────────────────────────
# SQUARE OFF
# ─────────────────────────────────────────────────────────────
def auto_square_off():
    log("[square-off] 3:18 PM — closing all intraday positions")

    for sl in get_active_slm_orders():
        oid = str(sl.get("orderId") or sl.get("order_id") or "")
        if oid:
            _cancel_order(oid, label=f"squareoff_cancel_sl:{oid}")

    time.sleep(1)

    for pos in get_open_intraday_positions():
        symbol = _position_symbol(pos)
        net_qty = int(float(pos.get("netQty", 0) or 0))
        qty = abs(net_qty)
        if qty <= 0:
            continue
        exit_side = "SELL" if net_qty > 0 else "BUY"
        _place_entry_order(symbol, exit_side, qty)

    bot_positions.clear()
    exit_pending.clear()
    send_telegram("Dhan AUTO SQUARE-OFF COMPLETE")

# ─────────────────────────────────────────────────────────────
# CHARTINK
# ─────────────────────────────────────────────────────────────
def parse_cookie(raw: str) -> dict:
    cookies = {}
    for part in raw.split(";"):
        part = part.strip()
        if "=" in part:
            k, v = part.split("=", 1)
            cookies[k.strip()] = v.strip()
    return cookies


cookies = parse_cookie(CHARTINK_COOKIE_RAW)

def fetch_chartink_signals(scan_type: str, payload: dict) -> list:
    
    token = CHARTINK_CSRF_TOKEN or cookies.get("XSRF-TOKEN", "")
    if token:
        token = urllib.parse.unquote(token)

    headers = {
        "Content-Type": "application/json",
        "Referer": "https://chartink.com/",
        "User-Agent": "Mozilla/5.0",
        "X-Requested-With": "XMLHttpRequest",
    }
    if token:
        headers["X-XSRF-TOKEN"] = token

    def _call():
        r = requests.post(
            "https://chartink.com/screener/process",
            headers=headers,
            json=payload,
            cookies=cookies,
            timeout=12,
        )
        r.raise_for_status()
        data = r.json()

        if data.get("scan_error"):
            log(f"[chartink:{scan_type}] scan_error: {data['scan_error']}")
            return []

        syms = [
            d.get("nsecode", "").upper()
            for d in data.get("data", [])
            if isinstance(d, dict) and d.get("nsecode")
        ]

        if syms:
            log(f"[chartink:{scan_type}] symbols: {syms}")
        else:
            log(f"[chartink:{scan_type}] {scan_type.upper()} no signal")

        return [{"symbol": s, "side": scan_type.upper()} for s in syms]

    return with_retry(_call, label=f"chartink:{scan_type}", fallback=[])

buy_payload = {"scan_clause": '''( {1339018} (  daily volume *  daily "high+low/2" >  1500000000 and  [0] 5 minute volume *  [0] 5 minute "high+low/2" >  150000000 and  daily close >  1 day ago close *  1.04 and  [0] 5 minute close >  [-1] 5 minute close *  1.012 ) )'''}
sell_payload = {"scan_clause": '''( {1339018} (  daily volume *  daily "high+low/2" >  1500000000 and  [0] 5 minute volume *  [0] 5 minute "high+low/2" >  150000000 and  daily close <  1 day ago close *  0.96 and  [0] 5 minute close <  [-1] 5 minute close *  0.988 ) )'''}


def gather_signals() -> list:
    global _chartink_empty_streak

    
    out = []
    out += fetch_chartink_signals("BUY", buy_payload)
    out += fetch_chartink_signals("SELL", sell_payload)

    seen, uniq = set(), []
    for s in out:
        key = (s["symbol"], s["side"])
        if key not in seen:
            seen.add(key)
            uniq.append(s)

    if not uniq:
        _chartink_empty_streak += 1
        if _chartink_empty_streak >= _CHARTINK_REFRESH_AFTER:
            # log(f"[signals] {_chartink_empty_streak} consecutive empty scans")
            _chartink_empty_streak = 0
    else:
        _chartink_empty_streak = 0

    return uniq

# ─────────────────────────────────────────────────────────────
# THREADS / STARTUP / SHUTDOWN
# ─────────────────────────────────────────────────────────────
def shutdown(reason: str):
    if _shutdown_event.is_set():
        return
    log(f"[shutdown] {reason}")
    _shutdown_event.set()
    send_telegram(f"Dhan Bot stopped: {reason}")
    raise SystemExit(0)

def thread_chartink():
    """Sequential processing version to reduce LTP burst rate and 429s."""
    log("[thread] chartink started")
    while not _shutdown_event.is_set():
        try:
            if not _is_market_open():
                shutdown("Market closed — exiting bot")
                return

            if _is_entry_allowed():
                signals = gather_signals()
                if signals:
                    symbol_list = [s["symbol"].upper() for s in signals if s.get("symbol")]
                    get_ltp_batch(symbol_list)
                for signal in signals:
                    place_order(signal)
                    if _shutdown_event.wait(timeout=0.35):
                        return
            else:
                log("[thread] past entry cutoff — no fresh entries")
        except Exception:
            log(f"[thread] chartink error:\n{traceback.format_exc()}")
        _shutdown_event.wait(timeout=random.randint(8, 12))

def thread_trailing():
    log("[thread] trailing started")
    while not _shutdown_event.is_set():
        try:
            if not _is_market_open():
                shutdown("Market closed — exiting bot")
                return
            target = next_trailing_run_time()
            wait = seconds_until(target)
            _shutdown_event.wait(timeout=wait)
            if _is_market_open():
                trailing_sl()
        except Exception:
            log(f"[thread] trailing error:\n{traceback.format_exc()}")
            _shutdown_event.wait(timeout=30)


def check_targets():
    """Programmatically evaluates 1% targets to bypass broker double-margin blocks."""
    if not bot_positions:
        return
        
    symbols = list(bot_positions.keys())
    # Fetch batch LTPs. Max age 1.0s ensures high-frequency accuracy without 429 spam.
    ltps = get_ltp_batch(symbols, max_age=1.0) 
    
    for symbol, pos in list(bot_positions.items()):
        target = pos.get("target_price")
        if not target:
            continue
            
        ltp = ltps.get(symbol, 0.0)
        if ltp <= 0:
            continue
            
        side = pos["side"]
        qty = pos["qty"]
        exit_side = pos["exit_side"]
        
        if (side == "BUY" and ltp >= target) or (side == "SELL" and ltp <= target):
            log(f"[target] {symbol} HIT 1% TARGET {target} at LTP {ltp}")
            _exit_position_market(symbol, exit_side, qty, reason=f"target hit: {target}")


def thread_maintenance():
    log("[thread] maintenance started")
    last_sync_ts = 0.0
    
    while not _shutdown_event.is_set():
        try:
            if not _is_market_open():
                wait = _seconds_until_market_open()
                _shutdown_event.wait(timeout=wait)
                continue
                
            # FAST LOOP: Evaluates 1% target continuously.
            check_targets()
            
            # SLOW LOOP: Throttle heavy broker sync to 30 seconds
            now_ts = time.time()
            if now_ts - last_sync_ts > 30:
                sync_bot_positions()
                detect_manual_entry()
                cancel_stale_sl_orders()
                last_sync_ts = now_ts
                
        except Exception:
            log(f"[thread] maintenance error:\n{traceback.format_exc()}")
            
        # ====================================================================
        # TIMING FIX: Dropped to 2 seconds. 
        # This guarantees the target is evaluated almost immediately upon hitting 1%.
        # ====================================================================
        _shutdown_event.wait(timeout=2)

def thread_squareoff():
    log("[thread] squareoff started")
    triggered_date = None
    shutdown_triggered = False

    while not _shutdown_event.is_set():
        try:
            now = now_ist()
            today = now.date()
            t = _ist_hm()

            if _is_market_open() and t >= SQUAREOFF_AT and triggered_date != today:
                triggered_date = today
                auto_square_off()

            if t >= MARKET_CLOSE and not shutdown_triggered and today.weekday() < 5:
                shutdown_triggered = True
                shutdown("Market closed at 3:30 PM — EOD shutdown")
        except Exception:
            log(f"[thread] squareoff error:\n{traceback.format_exc()}")
        _shutdown_event.wait(timeout=15)

def startup():
    log("=" * 60)
    log("Dhan Order Management Bot -- STARTING UP")
    validate_dhan_auth()
    send_telegram("Dhan Bot Started")
    load_dhan_instrument_master()
    _rebuild_positions_from_broker()
    if bot_positions and _is_market_open():
        trailing_sl()

    log(f"[startup] MAX_POSITIONS={MAX_POSITIONS} MIN_CAPITAL_SLOT={MIN_CAPITAL_SLOT}")
    log(f"[startup] loaded symbols={len(security_id_map)}")
    log(f"[startup] restored positions={list(bot_positions.keys())}")
    log("[startup] ready")

def main():
    startup()

    threads = [
        threading.Thread(target=thread_chartink, daemon=True, name="chartink"),
        threading.Thread(target=thread_trailing, daemon=True, name="trailing"),
        threading.Thread(target=thread_maintenance, daemon=True, name="maintenance"),
        threading.Thread(target=thread_squareoff, daemon=True, name="squareoff"),
        threading.Thread(target=thread_profit_monitor, daemon=True, name="profit_monitor"),
    ]

    for t in threads:
        t.start()
        time.sleep(0.1)

    log("[main] all threads running -- bot is LIVE")

    try:
        while not _shutdown_event.is_set():
            _shutdown_event.wait(timeout=60)
            if not _shutdown_event.is_set():
                pos_summary = {
                    sym: {
                        "side": v["side"],
                        "qty": v["qty"],
                        "sl": v.get("sl_order_id"),
                    }
                    for sym, v in bot_positions.items()
                }
                log(f"[heartbeat] positions={pos_summary}")
    except KeyboardInterrupt:
        shutdown("KeyboardInterrupt")

if __name__ == "__main__":
    main()