#!/usr/bin/env python3
"""
Dhan trading bot (single-file procedural)

Environment variables expected in .env:
- DHAN_CLIENT_ID
- DHAN_ACCESS_TOKEN
- DHAN_BASE (optional, default https://api.dhan.co/v2)
- CHARTINK_COOKIE
- CHARTINK_CSRF_TOKEN
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID
- LOOP_INTERVAL_SECONDS (default 15)
- SIGNAL_AMOUNT (default 10000)
- MAX_POSITION (default 1)
- COOLDOWN_MINUTES (default 20)
- CACHE_MINUTES (defaults to COOLDOWN_MINUTES)
- TRIGGER_PCT (percent like "0.3" for 0.3% default)
- STEP_PCT (percent like "0.025" for 0.025% default)
- MIN_LEVERAGE_FOR_5X (default 4.99)
- ORDER_FILL_TIMEOUT (seconds, default 60)
- ORDER_POLL_INTERVAL (seconds, default 2)
- LTP_RETRY_COUNT (default 3)
- LTP_RETRY_DELAY (default 0.5)
- LTP_RETRY_BACKOFF (default 2)
- EXCLUDED_SYMBOLS (comma separated)
"""

from __future__ import annotations
import os
import time
import math
import ast
import urllib.parse
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from typing import Dict, Optional, List, Tuple
from dotenv import load_dotenv

# optional dhanhq client
try:
    from dhanhq import dhanhq
except Exception:
    dhanhq = None

# -------------------- Config --------------------
load_dotenv()
import platform

# Prevent system sleep (Windows)
if platform.system() == "Windows":
    import ctypes
    ES_CONTINUOUS = 0x80000000
    ES_SYSTEM_REQUIRED = 0x00000001
    ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)


ZONE = ZoneInfo("Asia/Kolkata")
TRADING_CUTOFF = dtime(hour=15, minute=15)  # 15:15 IST
LOOP_INTERVAL_SECONDS = int(os.getenv("LOOP_INTERVAL_SECONDS", "15"))
MAX_POSITION = int(os.getenv("MAX_POSITION", "2"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "20"))
CACHE_MINUTES = int(os.getenv("CACHE_MINUTES", str(COOLDOWN_MINUTES)))
SIGNAL_AMOUNT = float(os.getenv("SIGNAL_AMOUNT", "10000"))
BUFFER_RATIO = float(os.getenv("BUFFER_RATIO", "0.07"))  # 7% default
MIN_LEVERAGE_FOR_5X = float(os.getenv("MIN_LEVERAGE_FOR_5X", "4.99"))

DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "").strip()
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
DHAN_BASE = os.getenv("DHAN_BASE", "https://api.dhan.co/v2").rstrip("/")
DHAN_EXCHANGE_SEGMENT = os.getenv("DHAN_EXCHANGE_SEGMENT", "NSE_EQ")
DHAN_PRODUCT_TYPE = os.getenv("DHAN_PRODUCT_TYPE", "INTRADAY")
EXCLUDED_SYMBOLS = set(os.getenv("EXCLUDED_SYMBOLS", "SBIN,RELIANCE,HDFCBANK,ICICIBANK,INFY,TCS").split(","))

CHARTINK_COOKIE = os.getenv("CHARTINK_COOKIE", "")
CHARTINK_CSRF = os.getenv("CHARTINK_CSRF_TOKEN", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# TRIGGER_PCT and STEP_PCT provided in percent form (e.g., "0.3" => 0.3% => factor 0.003)
TRIGGER_PCT = float(os.getenv("TRIGGER_PCT", "0.35")) / 100.0
STEP_PCT = float(os.getenv("STEP_PCT", "0.03")) / 100.0

ORDER_FILL_TIMEOUT = int(os.getenv("ORDER_FILL_TIMEOUT", "60"))      # seconds to wait for fill
ORDER_POLL_INTERVAL = float(os.getenv("ORDER_POLL_INTERVAL", "2"))   # poll interval sec

# LTP retry/backoff settings
LTP_RETRY_COUNT = int(os.getenv("LTP_RETRY_COUNT", "3"))
LTP_RETRY_DELAY = float(os.getenv("LTP_RETRY_DELAY", "0.5"))
LTP_RETRY_BACKOFF = float(os.getenv("LTP_RETRY_BACKOFF", "2"))

# -------------------- Globals --------------------
segment = DHAN_EXCHANGE_SEGMENT
SESSION = requests.Session()
SESSION.headers.update({"access-token": DHAN_ACCESS_TOKEN})
if dhanhq:
    try:
        dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
    except Exception:
        dhan = None
else:
    dhan = None

SIDE_CACHE: Dict[Tuple[str, str], datetime] = {}
EQUITY_MASTER: pd.DataFrame = pd.DataFrame()
SYMBOL_TO_SECURITY_ID: Dict[str, int] = {}

# -------------------- Helpers --------------------
def now() -> datetime:
    return datetime.now(tz=ZONE)

def p(msg: str, *args):
    ts = now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        print(f"{ts} - {msg % args}")
    except Exception:
        print(f"{ts} - {msg} {args}")

def trading_allowed() -> bool:
    return now().time() <= TRADING_CUTOFF

def _headers_json():
    return {"Accept": "application/json", "Content-Type": "application/json", "access-token": (DHAN_ACCESS_TOKEN or "").strip()}

def _parse_cookie_blob(blob: str) -> dict:
    if not blob:
        return {}
    try:
        return ast.literal_eval(blob)
    except Exception:
        cookies = {}
        for part in blob.split(";"):
            if "=" in part:
                k, v = part.split("=", 1)
                cookies[k.strip()] = v.strip()
        return cookies

# -------------------- Telegram --------------------
def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        p("Telegram creds missing; would have sent: %s", message)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        r = requests.post(url, json=payload, timeout=6)
        r.raise_for_status()
        p("Telegram sent: %s", message)
    except Exception as e:
        p("Failed to send telegram: %s", e)

# -------------------- Chartink --------------------
def fetch_chartink_signals(scan_type: str, payload: dict) -> List[dict]:
    cookies = _parse_cookie_blob(CHARTINK_COOKIE)
    token = CHARTINK_CSRF or cookies.get("XSRF-TOKEN")
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
    try:
        r = requests.post("https://chartink.com/screener/process", headers=headers, json=payload, cookies=cookies, timeout=12)
        r.raise_for_status()
        data = r.json()
        if data.get("scan_error"):
            p("[chartink %s] scan_error %s", scan_type, data.get("scan_error"))
            return []
        syms = [d.get("nsecode", "").upper() for d in data.get("data", []) if isinstance(d, dict) and d.get("nsecode")]
        return [{"symbol": s, "side": scan_type.upper()} for s in syms]
    except Exception as e:
        p("[chartink %s] %s", scan_type, e)
        return []

# -------------------- Equity master --------------------
def fetch_equity_master() -> pd.DataFrame:
    try:
        url = f"{DHAN_BASE}/instrument/{DHAN_EXCHANGE_SEGMENT}"
        headers = {"accept": "text/csv", "access-token": DHAN_ACCESS_TOKEN}
        r = SESSION.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        df = pd.read_csv(StringIO(r.text))
        df = df[(df.get("EXCH_ID") == "NSE") & (df.get("SEGMENT") == "E") & (df.get("INSTRUMENT_TYPE") == "ES")].copy()
        if "UNDERLYING_SYMBOL" in df.columns:
            df = df.rename(columns={"UNDERLYING_SYMBOL": "SYMBOL"})
        df["SYMBOL"] = df["SYMBOL"].astype(str).str.upper().str.strip()
        df["SECURITY_ID"] = df["SECURITY_ID"].astype(int)
        p("Equity master fetched: %d rows", len(df))
        return df.drop_duplicates(subset=["SYMBOL"]).reset_index(drop=True)
    except Exception as e:
        p("fetch_equity_master failed: %s", e)
        return pd.DataFrame()

def build_equity_master():
    global EQUITY_MASTER, SYMBOL_TO_SECURITY_ID
    EQUITY_MASTER = fetch_equity_master()
    if not EQUITY_MASTER.empty:
        SYMBOL_TO_SECURITY_ID = dict(zip(EQUITY_MASTER["SYMBOL"], EQUITY_MASTER["SECURITY_ID"]))
        p("Symbol map built: %d symbols", len(SYMBOL_TO_SECURITY_ID))
    else:
        p("Equity master empty; symbol map not built")

def resolve_security_id(symbol: str) -> Optional[int]:
    return SYMBOL_TO_SECURITY_ID.get(symbol.upper())

# -------------------- Dhan & dhanhq helpers --------------------
def get_positions_via_dhan() -> dict:
    try:
        if dhan and hasattr(dhan, "get_positions"):
            return dhan.get_positions()
        return SESSION.get(f"{DHAN_BASE}/portfolio/positions", timeout=10).json()
    except Exception as e:
        p("get_positions failed: %s", e)
        return {"status": "error", "data": []}

def get_order_list_via_dhan() -> dict:
    try:
        if dhan and hasattr(dhan, "get_order_list"):
            return dhan.get_order_list()
        return SESSION.get(f"{DHAN_BASE}/orders/list", timeout=10).json()
    except Exception as e:
        p("get_order_list failed: %s", e)
        return {"status": "error", "data": []}

def get_fund_limits_via_dhan() -> dict:
    try:
        if dhan and hasattr(dhan, "get_fund_limits"):
            return dhan.get_fund_limits()
        return SESSION.get(f"{DHAN_BASE}/funds/limits", timeout=8).json()
    except Exception as e:
        p("get_fund_limits failed: %s", e)
        return {"status": "error", "data": {}}

# -------------------- Robust LTP with retries --------------------
def _fetch_ltp_once(security_id: int) -> Optional[float]:
    try:
        resp = None
        if dhan and hasattr(dhan, "ohlc_data"):
            resp = dhan.ohlc_data(securities={segment: [security_id]})
        else:
            try:
                r = SESSION.get(f"{DHAN_BASE}/ohlc?securities={segment}:{security_id}", timeout=6)
                r.raise_for_status()
                resp = r.json()
            except Exception as he:
                p("HTTP ohlc fallback failed for %s: %s", security_id, he)
                resp = None

        if not isinstance(resp, dict):
            return None

        ltp = None
        try:
            ltp = resp.get('data', {}).get('data', {}).get(segment, {}).get(str(security_id), {}).get('last_price')
            if ltp is None:
                data_node = resp.get('data') or {}
                sec_node = None
                if isinstance(data_node, dict) and data_node.get(segment):
                    sec_node = data_node.get(segment, {}).get(str(security_id), {})
                if not sec_node:
                    for k, v in (data_node.items() if isinstance(data_node, dict) else []):
                        if isinstance(v, dict) and v.get(str(security_id)):
                            sec_node = v.get(str(security_id))
                            break
                if sec_node:
                    ltp = sec_node.get('last_price') or sec_node.get('lastPrice') or sec_node.get('ltp') or sec_node.get('last')
            if ltp is None:
                ltp = resp.get('last_price') or resp.get('lastPrice') or resp.get('ltp')
        except Exception:
            ltp = None

        if ltp is not None:
            try:
                return float(ltp)
            except Exception:
                return None
    except Exception as e:
        p("Unexpected error in _fetch_ltp_once for %s: %s", security_id, e)
    return None

def get_ltp_for_security(security_id: int) -> Optional[float]:
    delay = LTP_RETRY_DELAY
    for attempt in range(1, LTP_RETRY_COUNT + 1):
        try:
            ltp = _fetch_ltp_once(security_id)
            if ltp is not None:
                if attempt > 1:
                    p("LTP fetched on attempt %d for %s: %.2f", attempt, security_id, ltp)
                return ltp
            else:
                p("LTP not available on attempt %d for %s; will retry after %.2fs", attempt, security_id, delay)
        except Exception as e:
            p("Error fetching LTP (attempt %d) for %s: %s", attempt, security_id, e)
        if attempt < LTP_RETRY_COUNT:
            time.sleep(delay)
            delay *= LTP_RETRY_BACKOFF
    p("LTP fetch exhausted %d attempts for %s; returning None", LTP_RETRY_COUNT, security_id)
    return None

def get_ltp(security_id: int) -> Optional[float]:
    return get_ltp_for_security(security_id)

# -------------------- Active MIS positions & pending orders --------------------
def get_active_mis_positions() -> List[dict]:
    active_mis_positions: List[dict] = []
    all_positions = get_positions_via_dhan()
    if all_positions and 'data' in all_positions:
        for position in all_positions['data']:
            if position.get('productType') == 'INTRADAY' and position.get('positionType') != 'CLOSED':
                active_mis_positions.append({
                    "tradingSymbol": position.get("tradingSymbol"),
                    "positionType": position.get("positionType"),
                    "securityId": position.get("securityId"),
                    "netQty": abs(int(position.get("netQty", 0)))
                })
    return active_mis_positions

def get_pending_orders_debug() -> List[dict]:
    orders = get_order_list_via_dhan().get('data', []) or []
    pending_orders = [
        {
            "orderId": o.get("orderId"),
            "tradingSymbol": o.get("tradingSymbol"),
            "transactionType": o.get("transactionType"),
            "quantity": o.get("quantity"),
            "price": o.get("price"),
            "orderType": o.get("orderType"),
            "orderStatus": o.get("orderStatus"),
            "triggerPrice": o.get("triggerPrice")
        }
        for o in orders if (o.get("orderStatus") or "").upper() == "PENDING"
    ]
    return pending_orders

# -------------------- Margincalc & 5x check --------------------
def is_symbol_5x_eligible(symbol: str) -> bool:
    if symbol.upper() in EXCLUDED_SYMBOLS:
        p("Symbol %s excluded from margin checks", symbol)
        return False
    sid = resolve_security_id(symbol)
    if not sid:
        p("No security id for %s", symbol)
        return False
    payload = {
        "dhanClientId": DHAN_CLIENT_ID,
        "exchangeSegment": DHAN_EXCHANGE_SEGMENT,
        "transactionType": "BUY",
        "quantity": 1,
        "productType": DHAN_PRODUCT_TYPE,
        "securityId": str(int(sid)),
        "price": 20.0,
        "triggerPrice": 20.0,
    }
    try:
        r = SESSION.post(f"{DHAN_BASE}/margincalculator", headers=_headers_json(), json=payload, timeout=10)
        data = r.json() if r.status_code < 400 else {}
        lev = data.get("leverage") or (data.get("data", {}) or {}).get("leverage") or (data.get("data", {}) or {}).get("max_leverage")
        if lev:
            if isinstance(lev, str):
                lev_val = lev.strip().upper().replace("X", "")
                try:
                    lev_val = float(lev_val)
                except:
                    lev_val = 0.0
            else:
                try:
                    lev_val = float(lev)
                except:
                    lev_val = 0.0
            p("Margin API returned leverage %s for %s", lev_val, symbol)
            return lev_val >= MIN_LEVERAGE_FOR_5X
        margin_amt = (data.get("data", {}) or {}).get("required_margin") or (data.get("data", {}) or {}).get("margin") or data.get("required_margin") or data.get("margin")
        if margin_amt:
            try:
                margin_amt = float(margin_amt)
                ltp_val = get_ltp(int(sid)) or 0.0
                if ltp_val > 0 and margin_amt > 0:
                    lev_computed = ltp_val / margin_amt
                    p("Computed leverage from margin: %.4f for %s", lev_computed, symbol)
                    return lev_computed >= MIN_LEVERAGE_FOR_5X
            except Exception as e:
                p("[5x check compute] %s", e)
        return False
    except Exception as e:
        p("[5x check] %s", e)
        return False

# -------------------- Quantity helpers --------------------
def compute_quantity_from_balance(available_balance: float, price: float, leverage: float) -> int:
    try:
        effective_balance = max(0.0, available_balance * (1.0 - BUFFER_RATIO))
        capital = min(SIGNAL_AMOUNT, effective_balance)
        qty = math.floor((capital * leverage) / price)
        return max(0, int(qty))
    except Exception as e:
        p("compute_quantity failed: %s", e)
        return 0

def price_to_percent_delta(price: float, absolute_delta: float) -> float:
    return (absolute_delta / price) * 100.0

# -------------------- Order placement & reconcile --------------------
def place_superorder_absolute(payload: dict) -> dict:
    try:
        url = f"{DHAN_BASE}/super/orders"
        p("POST %s -> %s", url, payload)
        r = SESSION.post(url, json=payload, timeout=15)
        r.raise_for_status()
        j = r.json()
        p("superorder response: %s", j)
        return j
    except Exception as e:
        p("place_superorder failed: %s", e)
        return {"status": "error", "error": str(e)}

def modify_order(**kwargs) -> dict:
    try:
        url = f"{DHAN_BASE}/orders/modify"
        p("POST %s -> %s", url, kwargs)
        r = SESSION.post(url, json=kwargs, timeout=12)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        p("modify_order failed: %s", e)
        return {"status": "error", "error": str(e)}

def cancel_order(order_id: str) -> dict:
    try:
        url = f"{DHAN_BASE}/orders/cancel"
        p("POST %s -> order_id=%s", url, order_id)
        r = SESSION.post(url, json={"order_id": order_id}, timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        p("cancel_order failed: %s", e)
        return {"status": "error", "error": str(e)}

def reconcile_orders():
    try:
        orders = get_order_list_via_dhan().get("data", []) or []
        for o in orders:
            try:
                status = (o.get("orderStatus") or "").upper()
                if status not in ("PENDING", "OPEN", "PARTIALLY_FILLED"):
                    continue
                is_super = o.get("is_superorder") or (o.get("tag") == "superorder_bot")
                if is_super:
                    continue
                order_id = o.get("orderId") or o.get("dhanOrderId") or o.get("exchangeOrderId")
                if not order_id:
                    continue
                p("Attempting to convert order %s -> superorder", order_id)
                resp = modify_order(convert_to_super=True, order_id=order_id)
                if resp.get("status") in ("success", "ok"):
                    p("Converted %s to superorder", order_id)
                    continue
                p("Conversion failed for %s; canceling and recreating as absolute superorder", order_id)
                cancel_order(order_id)
                symbol = (o.get("tradingSymbol") or o.get("symbol") or "").upper()
                sid = int(o.get("securityId") or o.get("security_id") or 0)
                qty = int(o.get("orderQty") or o.get("quantity") or 0)
                try:
                    ltp_val = get_ltp(sid) or 0.0
                except:
                    ltp_val = 0.0
                if ltp_val and TRIGGER_PCT > 0:
                    if (o.get("transactionType") or o.get("side") or "BUY").upper() == "SELL":
                        stopLossPrice = round(ltp_val + (ltp_val * TRIGGER_PCT), 2)
                    else:
                        stopLossPrice = round(max(0.0, ltp_val - (ltp_val * TRIGGER_PCT)), 2)
                else:
                    stopLossPrice = round(o.get("stopLossPrice") or o.get("stop_loss_price") or 0.0, 2)
                trailingJump = round((ltp_val * STEP_PCT) if ltp_val else (o.get("trailingJump") or 0.0), 2)
                super_payload = {
                    "transactionType": (o.get("transactionType") or o.get("side") or "BUY"),
                    "exchangeSegment": DHAN_EXCHANGE_SEGMENT,
                    "productType": DHAN_PRODUCT_TYPE,
                    "orderType": o.get("orderType") or "MARKET",
                    "securityId": str(int(sid)),
                    "quantity": int(qty),
                    "price": 0.0,
                    "targetPrice": float(o.get("targetPrice") or 0.0),
                    "stopLossPrice": float(stopLossPrice),
                    "trailingJump": float(trailingJump),
                    "tag": "superorder_bot",
                    "client_id": DHAN_CLIENT_ID,
                }
                place_superorder_absolute(super_payload)
            except Exception as e:
                p("Error reconciling order: %s", e)
    except Exception as e:
        p("Failed to fetch orders for reconcile: %s", e)

# -------------------- Manual detection --------------------
def check_manual_changes():
    try:
        active = get_active_mis_positions()
        if active:
            p("Active MIS Positions (manual or bot):")
            for pos in active:
                p("%s", pos)
    except Exception as e:
        p("check_manual_changes failed: %s", e)

# -------------------- Cache utils --------------------
def cache_is_recent(symbol: str, side: str, minutes: int = CACHE_MINUTES) -> bool:
    k = (symbol.upper(), side.upper())
    t = SIDE_CACHE.get(k)
    if not t:
        return False
    return (now() - t) < timedelta(minutes=minutes)

def cache_update(symbol: str, side: str):
    SIDE_CACHE[(symbol.upper(), side.upper())] = now()

# -------------------- Signal handler (place & wait for fill -> Telegram after fill) --------------------
def handle_signal(sig: dict):
    """
    Handle a Chartink signal, place absolute-rupee superorder, poll until the broker reports
    the order as filled/open (including 'TRADED'), then send Telegram with filled info.
    """
    symbol = (sig.get("symbol") or "").upper()
    side = (sig.get("side") or "").upper()
    if not symbol or not side:
        return

    p("Signal received -> %s %s", side, symbol)

    # cooldown/cache
    if cache_is_recent(symbol, side):
        p("Skipping %s %s due to recent cache/cooldown", side, symbol)
        return
    if not trading_allowed():
        p("Trading cutoff reached; skipping %s %s", side, symbol)
        return

    cache_update(symbol, side)

    # check active MIS positions
    active_mis = get_active_mis_positions()
    if len(active_mis) >= MAX_POSITION:
        p("Max MIS positions reached (%d). Skipping %s %s and notifying", len(active_mis), side, symbol)
        # send_telegram(f"SKIPPED (max pos reached): {side} {symbol}")
        return

    sid = resolve_security_id(symbol)
    if sid is None:
        p("Security id not found for %s", symbol)
        return

    ltp = get_ltp(sid)
    if ltp is None or ltp <= 0:
        p("LTP not available/zero for %s", symbol)
        return

    if not is_symbol_5x_eligible(symbol):
        p("Symbol %s not eligible for 5x; skipping", symbol)
        return

    # funds and leverage
    try:
        funds = get_fund_limits_via_dhan()
        available_balance = float((funds.get("data", {}) or {}).get("availabelBalance", 0.0) or 0.0)
    except Exception as e:
        p("Failed to fetch funds: %s", e)
        available_balance = 0.0

    lev = None
    try:
        payload = {
            "dhanClientId": DHAN_CLIENT_ID,
            "exchangeSegment": DHAN_EXCHANGE_SEGMENT,
            "transactionType": "BUY",
            "quantity": 1,
            "productType": DHAN_PRODUCT_TYPE,
            "securityId": str(int(sid)),
            "price": ltp,
            "triggerPrice": ltp,
        }
        r = SESSION.post(f"{DHAN_BASE}/margincalculator", headers=_headers_json(), json=payload, timeout=8)
        if r.status_code < 400:
            d = r.json()
            lev = (d.get("data") or {}).get("max_leverage") or (d.get("data") or {}).get("leverage") or d.get("leverage")
            if lev:
                if isinstance(lev, str):
                    lev = lev.strip().upper().replace("X", "")
                lev = float(lev)
        if not lev:
            lev = 5.0
    except Exception as e:
        p("Failed to determine leverage; defaulting to 5.0: %s", e)
        lev = 5.0

    qty = compute_quantity_from_balance(available_balance, ltp, lev)
    if qty <= 0:
        p("Computed qty 0 for %s (balance=%.2f ltp=%.2f lev=%.2f)", symbol, available_balance, ltp, lev)
        return

    # compute trigger and step (fractions already in TRIGGER_PCT / STEP_PCT)
    trigger_abs = round(ltp * TRIGGER_PCT, 2)
    step_abs = round(ltp * STEP_PCT, 2)

    if side == "BUY":
        stop_loss_price = round(max(0.0, ltp - trigger_abs), 2)
    else:
        stop_loss_price = round(ltp + trigger_abs, 2)
    trailing_jump = step_abs

    super_payload = {
        "transactionType": "BUY" if side == "BUY" else "SELL",
        "exchangeSegment": DHAN_EXCHANGE_SEGMENT,
        "productType": DHAN_PRODUCT_TYPE,
        "orderType": "MARKET",
        "securityId": str(int(sid)),
        "quantity": int(qty),
        "price": 0.0,
        "targetPrice": 0.0,
        "stopLossPrice": float(stop_loss_price),
        "trailingJump": float(trailing_jump),
        "tag": "superorder_bot",
        "client_id": DHAN_CLIENT_ID,
    }

    p("Placing superorder: %s qty=%d ltp=%.2f stopLoss=%.2f trailingJump=%.2f",
      symbol, qty, ltp, stop_loss_price, trailing_jump)

    resp = place_superorder_absolute(super_payload)

    # --- Try to extract order_id & status from immediate response ---
    order_id = None
    initial_status = None
    try:
        if isinstance(resp, dict):
            data = resp.get("data") or resp
            # if API returned array or object(s), try common keys
            if isinstance(data, dict):
                order_id = data.get("orderId") or data.get("order_id") or data.get("id") or data.get("dhanOrderId")
                initial_status = (data.get("orderStatus") or data.get("status") or data.get("order_status") or None)
                # sometimes an 'order' sub-object exists
                if not order_id and data.get("order"):
                    o = data.get("order")
                    order_id = o.get("orderId") or o.get("order_id") or o.get("id")
                    initial_status = initial_status or (o.get("orderStatus") or o.get("status"))
            elif isinstance(data, list) and len(data) > 0:
                first = data[0]
                if isinstance(first, dict):
                    order_id = first.get("orderId") or first.get("order_id") or first.get("id")
                    initial_status = (first.get("orderStatus") or first.get("status") or first.get("order_status") or None)
    except Exception as e:
        p("Error extracting order info from response: %s", e)

    # quick check: if initial_status indicates filled/traded, we can skip polling
    # Normalize status string
    def norm_status(s):
        try:
            return str(s).strip().upper()
        except Exception:
            return None

    initial_status_norm = norm_status(initial_status)
    p("Initial API response order_id=%s status=%s", order_id or "N/A", initial_status_norm or "N/A")
    # statuses we treat as 'filled/active'
    FILLED_STATUSES = {"OPEN", "FILLED", "COMPLETED", "PARTIALLY_FILLED", "COMPLETE", "TRADED", "TRADE", "EXECUTED"}

    if initial_status_norm in FILLED_STATUSES:
        # Try to extract price/filled qty from the response and notify immediately
        try:
            filled_price = None
            filled_qty = qty
            data = resp.get("data") or resp if isinstance(resp, dict) else None
            if isinstance(data, dict):
                filled_price = data.get("avgPrice") or data.get("filledPrice") or data.get("price")
                filled_qty = data.get("filledQuantity") or data.get("filled_qty") or data.get("quantity") or filled_qty
            elif isinstance(data, list) and len(data) > 0:
                first = data[0]
                filled_price = first.get("avgPrice") or first.get("filledPrice") or first.get("price")
                filled_qty = first.get("filledQuantity") or first.get("quantity") or filled_qty

            filled_price = float(filled_price) if filled_price is not None else float(ltp)
            filled_qty = int(float(filled_qty)) if filled_qty is not None else int(qty)
        except Exception:
            filled_price = ltp
            filled_qty = int(qty)

        msg = f"ORDER FILLED: {symbol} {side} qty={filled_qty} price={filled_price:.2f} status={initial_status_norm}"
        if order_id:
            msg += f" order_id={order_id}"
        send_telegram(msg)
        p("Immediate fill detected and notification sent: %s", msg)
        return

    # --- Polling loop (order list) until we detect filled/open/traded (robust) ---
    poll_interval = float(os.getenv("ORDER_POLL_INTERVAL", str(ORDER_POLL_INTERVAL)))
    fill_timeout = int(os.getenv("ORDER_FILL_TIMEOUT", str(ORDER_FILL_TIMEOUT)))
    end_time = time.time() + fill_timeout
    order_info = None
    matched = False

    def order_status_from(o):
        return norm_status(o.get("orderStatus") or o.get("order_status") or o.get("status") or o.get("orderstate") or "")

    def order_matches_candidate(o):
        try:
            # match security id
            o_sid = o.get("securityId") or o.get("security_id") or o.get("instrument_id") or o.get("securityIdStr") or o.get("symbol") or o.get("tradingSymbol")
            if o_sid is not None:
                # sometimes tradingSymbol is symbol string, skip strict match if it's not numeric
                try:
                    if str(int(o_sid)) != str(int(sid)):
                        # if o_sid isn't numeric, compare symbol names
                        if str(o_sid).upper() != symbol:
                            return False
                except Exception:
                    if str(o_sid).upper() != symbol:
                        return False
            # match quantity loosely
            o_qty = o.get("quantity") or o.get("orderQty") or o.get("qty") or o.get("filledQuantity") or o.get("filledQty")
            if o_qty is not None:
                try:
                    if int(float(o_qty)) != int(qty):
                        return False
                except Exception:
                    pass
            # match side
            o_side = norm_status(o.get("transactionType") or o.get("transaction_type") or o.get("side") or "")
            if o_side and o_side != side:
                return False
            return True
        except Exception:
            return False

    # Poll until matched order has a filled/open status or timeout
    while time.time() < end_time:
        try:
            orders_resp = get_order_list_via_dhan().get("data", []) or []
            order_info = None
            # if we have order_id, prioritize matching it
            if order_id:
                for o in orders_resp:
                    # some APIs return numeric order ids, normalize to str
                    if str(o.get("orderId") or o.get("order_id") or o.get("exchangeOrderId") or "") == str(order_id):
                        order_info = o
                        break
            if not order_info:
                # fallback: try to find candidate match by sid/qty/side
                for o in orders_resp:
                    if order_matches_candidate(o):
                        order_info = o
                        break

            if order_info:
                status = order_status_from(order_info)
                p("Polled order status for %s: order_id=%s status=%s", symbol, order_info.get("orderId") or order_info.get("order_id") or order_id or "N/A", status)
                if status in FILLED_STATUSES:
                    matched = True
                    break
            else:
                p("No matching order found yet for %s; waiting...", symbol)
        except Exception as e:
            print("Error polling orders: %s", e)

        time.sleep(poll_interval)

    if not matched:
        print("Order for %s not filled/open within %ds (order_id=%s). Notifying pending.", symbol, fill_timeout, order_id)
        # send_telegram(f"ORDER NOT FILLED WITHIN {fill_timeout}s: {symbol} {side} qty={qty} order_id={order_id or 'N/A'}")
        return

    # extract final info and send telegram
    try:
        if not order_info:
            # last attempt to find it
            orders_resp = get_order_list_via_dhan().get("data", []) or []
            for o in orders_resp:
                if order_matches_candidate(o):
                    order_info = o
                    break

        order_id_final = (order_info.get("orderId") or order_info.get("order_id") or order_info.get("exchangeOrderId") or order_id or "N/A")
        status_final = order_status_from(order_info) or "UNKNOWN"
        fill_price = (order_info.get("avgPrice") or order_info.get("filledPrice") or order_info.get("avg_price") or order_info.get("filled_price") or order_info.get("price") or None)
        filled_qty = (order_info.get("filledQuantity") or order_info.get("filled_qty") or order_info.get("filledQty") or order_info.get("filled") or order_info.get("quantity") or qty)
        try:
            fill_price_float = float(fill_price) if fill_price is not None else float(ltp)
        except Exception:
            fill_price_float = ltp
        try:
            filled_qty_int = int(float(filled_qty)) if filled_qty is not None else int(qty)
        except Exception:
            filled_qty_int = int(qty)
    except Exception as e:
        print("Error extracting final order info: %s", e)
        order_id_final = order_id or "N/A"
        status_final = "UNKNOWN"
        fill_price_float = ltp
        filled_qty_int = int(qty)

    placed_price_str = f"{fill_price_float:.2f}"
    tg_msg = f"ORDER FILLED: {symbol} {side} qty={filled_qty_int} price={placed_price_str} status={status_final}"
    if order_id_final:
        tg_msg += f" order_id={order_id_final}"

    send_telegram(tg_msg)
    print("Order fill notification sent: %s", tg_msg)


def handle_signal(sig: dict):
    symbol = (sig.get("symbol") or "").upper()
    side = (sig.get("side") or "").upper()
    if not symbol or not side:
        return

    p("Signal received -> %s %s", side, symbol)

    if cache_is_recent(symbol, side):
        p("Skipping %s %s due to recent cache/cooldown", side, symbol)
        return
    if not trading_allowed():
        p("Trading cutoff reached; skipping %s %s", side, symbol)
        return

    cache_update(symbol, side)

    active_mis = get_active_mis_positions()
    if len(active_mis) >= MAX_POSITION:
        p("Max MIS positions reached (%d). Skipping %s %s and notifying", len(active_mis), side, symbol)
        send_telegram(f"SKIPPED (max pos reached): {side} {symbol}")
        return

    sid = resolve_security_id(symbol)
    if sid is None:
        p("Security id not found for %s", symbol)
        return

    ltp = get_ltp(sid)
    if ltp is None or ltp <= 0:
        p("LTP not available/zero for %s", symbol)
        return

    if not is_symbol_5x_eligible(symbol):
        p("Symbol %s not eligible for 5x; skipping", symbol)
        return

    try:
        funds = get_fund_limits_via_dhan()
        available_balance = float((funds.get("data", {}) or {}).get("availabelBalance", 0.0) or 0.0)
    except Exception as e:
        p("Failed to fetch funds: %s", e)
        available_balance = 0.0

    lev = None
    try:
        payload = {
            "dhanClientId": DHAN_CLIENT_ID,
            "exchangeSegment": DHAN_EXCHANGE_SEGMENT,
            "transactionType": "BUY",
            "quantity": 1,
            "productType": DHAN_PRODUCT_TYPE,
            "securityId": str(int(sid)),
            "price": ltp,
            "triggerPrice": ltp,
        }
        r = SESSION.post(f"{DHAN_BASE}/margincalculator", headers=_headers_json(), json=payload, timeout=8)
        if r.status_code < 400:
            d = r.json()
            lev = (d.get("data") or {}).get("max_leverage") or (d.get("data") or {}).get("leverage") or d.get("leverage")
            if lev:
                if isinstance(lev, str):
                    lev = lev.strip().upper().replace("X", "")
                lev = float(lev)
        if not lev:
            lev = 5.0
    except Exception as e:
        p("Failed to determine leverage; defaulting to 5.0: %s", e)
        lev = 5.0

    qty = compute_quantity_from_balance(available_balance, ltp, lev)
    if qty <= 0:
        p("Computed qty 0 for %s (balance=%.2f ltp=%.2f lev=%.2f)", symbol, available_balance, ltp, lev)
        return

    trigger_abs = round(ltp * TRIGGER_PCT, 2)
    step_abs = round(ltp * STEP_PCT, 2)

    if side == "BUY":
        stop_loss_price = round(max(0.0, ltp - trigger_abs), 2)
    else:
        stop_loss_price = round(ltp + trigger_abs, 2)

    trailing_jump = step_abs

    super_payload = {
        "transactionType": "BUY" if side == "BUY" else "SELL",
        "exchangeSegment": DHAN_EXCHANGE_SEGMENT,
        "productType": DHAN_PRODUCT_TYPE,
        "orderType": "MARKET",
        "securityId": str(int(sid)),
        "quantity": int(qty),
        "price": 0.0,
        "targetPrice": 0.0,
        "stopLossPrice": float(stop_loss_price),
        "trailingJump": float(trailing_jump),
        "tag": "superorder_bot",
        "client_id": DHAN_CLIENT_ID,
    }

    p("Placing superorder: %s qty=%d ltp=%.2f stopLoss=%.2f trailingJump=%.2f",
      symbol, qty, ltp, stop_loss_price, trailing_jump)

    resp = place_superorder_absolute(super_payload)

    # extract order id best-effort
    order_id = None
    try:
        if isinstance(resp, dict):
            data = resp.get("data") or resp
            if isinstance(data, dict):
                order_id = data.get("orderId") or data.get("order_id") or data.get("id") or data.get("dhanOrderId")
            if not order_id and isinstance(data, list) and len(data) > 0:
                first = data[0] or {}
                order_id = first.get("orderId") or first.get("order_id") or first.get("id") or first.get("dhanOrderId")
    except Exception as e:
        p("Error extracting order id from response: %s", e)

    end_time = time.time() + ORDER_FILL_TIMEOUT
    matched = False
    order_info = None

    def order_matches_candidate(o):
        try:
            o_sid = o.get("securityId") or o.get("security_id") or o.get("instrument_id") or o.get("securityIdStr")
            if o_sid is not None:
                try:
                    if str(int(o_sid)) != str(int(sid)):
                        return False
                except Exception:
                    if str(o_sid) != str(sid):
                        return False
            o_qty = o.get("quantity") or o.get("orderQty") or o.get("qty") or o.get("filledQuantity")
            if o_qty is not None:
                try:
                    if int(float(o_qty)) != int(qty):
                        return False
                except Exception:
                    pass
            o_side = (o.get("transactionType") or o.get("transaction_type") or o.get("side") or "").upper()
            if o_side and o_side != side:
                return False
            return True
        except Exception:
            return False

    while time.time() < end_time:
        try:
            orders_resp = get_order_list_via_dhan().get("data", []) or []
            if order_id:
                for o in orders_resp:
                    if (o.get("orderId") == order_id or o.get("order_id") == order_id or
                        str(o.get("exchangeOrderId") or o.get("exchange_order_id") or "") == str(order_id)):
                        order_info = o
                        break
            else:
                for o in orders_resp:
                    if order_matches_candidate(o):
                        order_info = o
                        break

            if order_info:
                status = (order_info.get("orderStatus") or order_info.get("order_status") or "").upper()
                p("Polled order status for %s: order_id=%s status=%s", symbol, order_info.get("orderId") or order_info.get("order_id") or order_id, status)
                if status in ("OPEN", "FILLED", "COMPLETED", "PARTIALLY_FILLED", "COMPLETE"):
                    matched = True
                    break
            else:
                p("No matching order found yet for %s; waiting...", symbol)
        except Exception as e:
            p("Error polling orders: %s", e)

        time.sleep(ORDER_POLL_INTERVAL)

    if not matched:
        p("Order for %s not filled/open within %ds (order_id=%s). Notifying pending.", symbol, ORDER_FILL_TIMEOUT, order_id)
        p(f"ORDER NOT FILLED WITHIN {ORDER_FILL_TIMEOUT}s: {symbol} {side} qty={qty} order_id={order_id or 'N/A'}")
        return

    # extract fill info
    try:
        if not order_info:
            orders_resp = get_order_list_via_dhan().get("data", []) or []
            for o in orders_resp:
                if order_matches_candidate(o):
                    order_info = o
                    break

        order_id_final = order_info.get("orderId") or order_info.get("order_id") or order_info.get("exchangeOrderId") or order_id
        status_final = (order_info.get("orderStatus") or order_info.get("order_status") or "").upper()
        fill_price = (order_info.get("avgPrice") or order_info.get("filledPrice") or
                      order_info.get("avg_price") or order_info.get("filled_price") or
                      order_info.get("price") or None)
        filled_qty = (order_info.get("filledQuantity") or order_info.get("filled_qty") or order_info.get("filledQty") or order_info.get("filled") or order_info.get("quantity") or qty)
        try:
            fill_price_float = float(fill_price) if fill_price is not None else float(ltp)
        except Exception:
            fill_price_float = ltp
        try:
            filled_qty_int = int(float(filled_qty)) if filled_qty is not None else int(qty)
        except Exception:
            filled_qty_int = int(qty)
    except Exception as e:
        p("Error extracting final order info: %s", e)
        order_id_final = order_id or "N/A"
        status_final = "UNKNOWN"
        fill_price_float = ltp
        filled_qty_int = int(qty)

    placed_price_str = f"{fill_price_float:.2f}"
    tg_msg = f"ORDER FILLED: {symbol} {side} qty={filled_qty_int} price={placed_price_str} status={status_final}"
    if order_id_final:
        tg_msg += f" order_id={order_id_final}"

    send_telegram(tg_msg)
    p("Order fill notification sent: %s", tg_msg)

# -------------------- Main loop --------------------
def check_manual_and_reconcile_each_loop():
    reconcile_orders()
    check_manual_changes()

def main_loop():
    build_equity_master()

    # Replace with your real Chartink scan_clauses (they must be valid Chartink requests)
    buy_payload  = {"scan_clause": '''( {1357043} ( ( {cash} ( ( {cash} ( daily close > 25 and daily close < 80 and( {cash} ( [0] 10 minute volume > 50000 or( {cash} ( [0] 5 minute volume > 30000 and [-1] 5 minute volume > 20000 ) ) ) ) ) ) or( {cash} ( daily close > 80 and daily close < 150 and( {cash} ( [0] 10 minute volume > 45000 or( {cash} ( [0] 5 minute volume > 25000 and [-1] 5 minute volume > 20000 ) ) ) ) ) ) or( {cash} ( daily close > 150 and daily close < 500 and( {cash} ( [0] 10 minute volume > 35000 and( {cash} ( [0] 5 minute volume > 14000 and [-1] 5 minute volume > 14000 ) ) ) ) ) ) or( {cash} ( daily close > 500 and daily close < 3000 and( {cash} ( [0] 10 minute volume > 30000 or( {cash} ( [-1] 5 minute volume > 10000 and [0] 5 minute volume > 25000 ) ) ) ) ) ) ) ) and abs( [0] 5 minute close - [0] 5 minute open ) > [0] 5 minute open * 0.0023 and [0] 5 minute close > [0] 5 minute supertrend( 18 , 1.1 ) and [0] 5 minute count( 5, 1 where [0] 5 minute close > [0] 5 minute supertrend( 18 , 1.1 ) ) > 1 and [0] 5 minute close > 1 day ago close * 1.016 and daily volume > 600000 and abs( [0] 5 minute {custom_indicator_185281_start}"{custom_indicator_185278_start}"ema(  {custom_indicator_185277_start}"ema(  close - 1 candle ago close , 10 )"{custom_indicator_185277_end} , 26 )"{custom_indicator_185278_end} /  {custom_indicator_185280_start}"ema(  {custom_indicator_185279_start}"ema( abs(  close - 1 candle ago close ) , 10 )"{custom_indicator_185279_end} , 26 )"{custom_indicator_185280_end} * 100"{custom_indicator_185281_end} - [0] 5 minute {custom_indicator_185282_start}"ema(  {custom_indicator_185278_start}"ema(  {custom_indicator_185277_start}"ema(  close - 1 candle ago close , 10 )"{custom_indicator_185277_end} , 26 )"{custom_indicator_185278_end} /  {custom_indicator_185280_start}"ema(  {custom_indicator_185279_start}"ema( abs(  close - 1 candle ago close ) , 10 )"{custom_indicator_185279_end} , 26 )"{custom_indicator_185280_end} * 100 , 20 )"{custom_indicator_185282_end} ) > 8 and [0] 5 minute {custom_indicator_185281_start}"{custom_indicator_185278_start}"ema(  {custom_indicator_185277_start}"ema(  close - 1 candle ago close , 10 )"{custom_indicator_185277_end} , 26 )"{custom_indicator_185278_end} /  {custom_indicator_185280_start}"ema(  {custom_indicator_185279_start}"ema( abs(  close - 1 candle ago close ) , 10 )"{custom_indicator_185279_end} , 26 )"{custom_indicator_185280_end} * 100"{custom_indicator_185281_end} > 20 and [0] 5 minute open < [0] 5 minute close ) )'''}
    sell_payload = {"scan_clause": '''( {1357043} ( ( {cash} ( ( {cash} ( daily close > 25 and daily close < 80 and( {cash} ( [0] 10 minute volume > 50000 or( {cash} ( [0] 5 minute volume > 30000 and [-1] 5 minute volume > 20000 ) ) ) ) ) ) or( {cash} ( daily close > 80 and daily close < 150 and( {cash} ( [0] 10 minute volume > 45000 or( {cash} ( [0] 5 minute volume > 25000 and [-1] 5 minute volume > 20000 ) ) ) ) ) ) or( {cash} ( daily close > 150 and daily close < 500 and( {cash} ( [0] 10 minute volume > 35000 and( {cash} ( [0] 5 minute volume > 14000 and [-1] 5 minute volume > 14000 ) ) ) ) ) ) or( {cash} ( daily close > 500 and daily close < 3000 and( {cash} ( [0] 10 minute volume > 30000 or( {cash} ( [-1] 5 minute volume > 10000 and [0] 5 minute volume > 25000 ) ) ) ) ) ) ) ) and abs( [0] 5 minute close - [0] 5 minute open ) > [0] 5 minute open * 0.0023 and [0] 5 minute close < [0] 5 minute supertrend( 18 , 1.1 ) and [0] 5 minute close > [-1] 5 minute min( 36 , [-1] 5 minute close ) * 1.005 and [0] 5 minute count( 5, 1 where [0] 5 minute close < [0] 5 minute supertrend( 18 , 1.1 ) ) > 1 and [0] 5 minute close < 1 day ago close * 0.985 and daily volume > 600000 and abs( [0] 5 minute {custom_indicator_185281_start}"{custom_indicator_185278_start}"ema(  {custom_indicator_185277_start}"ema(  close - 1 candle ago close , 10 )"{custom_indicator_185277_end} , 26 )"{custom_indicator_185278_end} /  {custom_indicator_185280_start}"ema(  {custom_indicator_185279_start}"ema( abs(  close - 1 candle ago close ) , 10 )"{custom_indicator_185279_end} , 26 )"{custom_indicator_185280_end} * 100"{custom_indicator_185281_end} - [0] 5 minute {custom_indicator_185282_start}"ema(  {custom_indicator_185278_start}"ema(  {custom_indicator_185277_start}"ema(  close - 1 candle ago close , 10 )"{custom_indicator_185277_end} , 26 )"{custom_indicator_185278_end} /  {custom_indicator_185280_start}"ema(  {custom_indicator_185279_start}"ema( abs(  close - 1 candle ago close ) , 10 )"{custom_indicator_185279_end} , 26 )"{custom_indicator_185280_end} * 100 , 20 )"{custom_indicator_185282_end} ) > 8 and [0] 5 minute {custom_indicator_185281_start}"{custom_indicator_185278_start}"ema(  {custom_indicator_185277_start}"ema(  close - 1 candle ago close , 10 )"{custom_indicator_185277_end} , 26 )"{custom_indicator_185278_end} /  {custom_indicator_185280_start}"ema(  {custom_indicator_185279_start}"ema( abs(  close - 1 candle ago close ) , 10 )"{custom_indicator_185279_end} , 26 )"{custom_indicator_185280_end} * 100"{custom_indicator_185281_end} < -20 and [0] 5 minute open > [0] 5 minute close ) )'''}

    p("Bot starting (cutoff %s). Loop interval: %ds", TRADING_CUTOFF.strftime("%H:%M"), LOOP_INTERVAL_SECONDS)
    p("TRIGGER_PCT=%.6f (%.4f%%) STEP_PCT=%.8f (%.6f%%)", TRIGGER_PCT, TRIGGER_PCT*100, STEP_PCT, STEP_PCT*100)

    while True:
        try:
            if not trading_allowed():
                p("Trading closed for today (after %s). Sleeping 60s.", TRADING_CUTOFF.strftime("%H:%M"))
                break

            buy_signals = fetch_chartink_signals("BUY", buy_payload)
            sell_signals = fetch_chartink_signals("SELL", sell_payload)
            all_signals = (buy_signals or []) + (sell_signals or [])

            check_manual_and_reconcile_each_loop()

            pending = get_pending_orders_debug()
            if pending:
                p("Pending orders (%d):", len(pending))
                for po in pending:
                    p("%s", po)
            funds = get_fund_limits_via_dhan()
            p("Available balance: %s", (funds.get("data", {}) or {}).get("availabelBalance"))

            for sig in all_signals:
                try:
                    handle_signal(sig)
                except Exception as e:
                    p("Error handling signal %s: %s", sig, e)

        except Exception as e:
            p("Main loop error: %s", e)

        time.sleep(LOOP_INTERVAL_SECONDS)

if __name__ == "__main__":
    try:
        main_loop()
    except KeyboardInterrupt:
        p("Bot stopped by user (KeyboardInterrupt)")
    except Exception as e:
        p("Unhandled exception in main: %s", e)
