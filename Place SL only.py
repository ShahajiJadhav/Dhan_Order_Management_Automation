#!/usr/bin/env python3
"""
dhan_ensure_place_only_slm.py

Dhan-adapted "place-only" SL-M manager using Dhan SDK and your
`get_active_mis_positions()` shape.

Behavior:
- Every loop (fast watcher and 5-minute cadence) we ONLY:
  * Place missing SL-M orders for active intraday positions (INTRADAY)
  * Cancel SL / SL-M orders that have no corresponding active position (orphans)
- We DO NOT modify existing SL-M orders, and we DO NOT exit positions at market.

Environment variables expected in .env:
 - DHAN_CLIENT_ID
 - DHAN_ACCESS_TOKEN
 - SIMULATION_MODE (optional)
 - DHAN_BASE (optional, used when fetching instrument master CSV)
 - DHAN_EXCHANGE_SEGMENT (optional, default: EQUITY)

Run:
 python3 dhan_ensure_place_only_slm.py
"""

import os
import time
import random
import threading
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from dotenv import load_dotenv

load_dotenv()

# ---------------- Config -----------------
SIMULATION_MODE = os.getenv('SIMULATION_MODE', 'false').lower() in ('1','true','yes')
DHAN_CLIENT_ID = os.getenv('DHAN_CLIENT_ID', '')
DHAN_ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN', '')
SEGMENT = os.getenv('DHAN_EXCHANGE_SEGMENT', 'NSE_EQ')
TRAIL_OFFSET_SECONDS = int(os.getenv('TRAIL_OFFSET_SECONDS', '90'))  # offset from 5-min boundary
CUTOFF_EXIT_H, CUTOFF_EXIT_M = int(os.getenv('CUTOFF_EXIT_H', '15')), int(os.getenv('CUTOFF_EXIT_M', '0'))
FAST_WATCHER_MIN_S = int(os.getenv('FAST_WATCHER_MIN_S', '8'))
FAST_WATCHER_MAX_S = int(os.getenv('FAST_WATCHER_MAX_S', '13'))
DEFAULT_TICK = float(os.getenv('DEFAULT_TICK', '0.05'))

# ---------------- Dhan client init -----------------
try:
    from dhanhq import dhanhq
except Exception as e:
    dhanhq = None
    print(f"[init] dhanhq import failed: {e}")

if dhanhq:
    dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
else:
    dhan = None

# ---------------- Helpers -----------------
import pytz
india_tz = pytz.timezone('Asia/Kolkata')


def now_ist():
    return datetime.now(india_tz)


def before_cutoff():
    n = now_ist()
    return (n.hour, n.minute) < (CUTOFF_EXIT_H, CUTOFF_EXIT_M)


def round_to_tick(price, tick=DEFAULT_TICK):
    steps = round(price / tick)
    return round(steps * tick, 2)

# ---------------- Instrument master & maps -----------------
import pandas as pd
from io import StringIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests

HARDENED_HTTP = requests.Session()
retry_cfg = Retry(total=5, backoff_factor=0.4, status_forcelist=(429,500,502,503,504), allowed_methods=frozenset(["GET","POST"]))
HARDENED_HTTP.mount('https://', HTTPAdapter(max_retries=retry_cfg))

symbol_to_security_id: Dict[str, int] = {}
_all_instruments_df: Optional[pd.DataFrame] = None


def fetch_dhan_equity_master(access_token: str) -> pd.DataFrame:
    """Robust fetch for Dhan instrument master.

    Tries multiple plausible endpoints/segment names and falls back to an SDK method if available.
    Returns an empty DataFrame on failure (caller must handle it).
    """
    DHAN_BASE = os.getenv('DHAN_BASE', 'https://api.dhan.co')
    # try a list of likely exchange segment names (common variants)
    candidates = [os.getenv('DHAN_EXCHANGE_SEGMENT', 'EQUITY'), 'NSE_EQ', 'nse_eq', 'EQUITY', 'EQUITY_NSE', 'NSE']
    last_exc = None
    for seg in candidates:
        url = f"{DHAN_BASE}/instrument/{seg}"
        try:
            hdr = {"accept": "text/csv", "access-token": access_token}
            r = HARDENED_HTTP.get(url, headers=hdr, timeout=20)
            r.raise_for_status()
            df = pd.read_csv(StringIO(r.text))
            # normalize columns
            if 'UNDERLYING_SYMBOL' in df.columns:
                df = df.rename(columns={'UNDERLYING_SYMBOL': 'SYMBOL'})
            if 'SYMBOL' in df.columns:
                df['SYMBOL'] = df['SYMBOL'].astype(str).str.upper().str.strip()
            if 'SECURITY_ID' in df.columns:
                df['SECURITY_ID'] = df['SECURITY_ID'].astype(int)
            print(f"[init] fetched instrument master from {url} (len={len(df)})")
            return df.drop_duplicates(subset=['SYMBOL']).reset_index(drop=True)
        except Exception as e:
            last_exc = e
            print(f"[init] fetch failed for segment {seg}: {e}")
            continue

    # Try SDK-provided method if available
    try:
        if dhan and hasattr(dhan, 'get_instruments'):
            print('[init] attempting SDK get_instruments()')
            resp = dhan.get_instruments() or {}
            # attempt to coerce to DataFrame if possible
            if isinstance(resp, dict) and 'data' in resp:
                df = pd.DataFrame(resp['data'])
            elif isinstance(resp, list):
                df = pd.DataFrame(resp)
            else:
                raise RuntimeError('unexpected response from dhan.get_instruments()')
            if 'UNDERLYING_SYMBOL' in df.columns:
                df = df.rename(columns={'UNDERLYING_SYMBOL': 'SYMBOL'})
            if 'SYMBOL' in df.columns:
                df['SYMBOL'] = df['SYMBOL'].astype(str).str.upper().str.strip()
            if 'SECURITY_ID' in df.columns:
                df['SECURITY_ID'] = df['SECURITY_ID'].astype(int)
            print(f"[init] fetched instrument master via SDK (len={len(df)})")
            return df.drop_duplicates(subset=['SYMBOL']).reset_index(drop=True)
    except Exception as e:
        last_exc = e
        print(f"[init] SDK instrument fetch failed: {e}")

    # final fallback: return empty DataFrame
    print(f"[init] instruments load failed after trying endpoints; last error: {last_exc}")
    return pd.DataFrame()



def build_security_maps(df: pd.DataFrame):
    return dict(zip(df['SYMBOL'], df['SECURITY_ID']))


def load_instruments_once():
    global _all_instruments_df, symbol_to_security_id
    if _all_instruments_df is not None:
        return
    try:
        if dhan:
            df = fetch_dhan_equity_master(DHAN_ACCESS_TOKEN)
            _all_instruments_df = df
            symbol_to_security_id = build_security_maps(df)
            print(f"[init] loaded {len(symbol_to_security_id)} symbols from Dhan master")
        else:
            print('[init] dhan client missing; instrument load skipped')
            _all_instruments_df = pd.DataFrame()
    except Exception as e:
        print(f"[init] instruments load failed: {e}")
        _all_instruments_df = pd.DataFrame()


def resolve_security_id(symbol: str) -> Optional[int]:
    return symbol_to_security_id.get(symbol.upper().strip())

# ------------------- User-provided get_active_mis_positions -------------------

def get_active_mis_positions() -> List[dict]:
    """Return a list of active INTRADAY positions in the user's format.

    Each dict has keys: tradingSymbol, positionType, securityId, netQty
    """
    active_mis_positions: List[dict] = []
    try:
        all_positions = dhan.get_positions()
    except Exception as e:
        print(f"[get_active_mis_positions] failed to fetch positions: {e}")
        return active_mis_positions

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

# ---------------- LTP and candles via Dhan SDK -----------------

def get_ltp_for_symbol(symbol: str, sec_id: Optional[int] = None) -> Optional[float]:
    if sec_id is None:
        sec_id = resolve_security_id(symbol)
    if not sec_id:
        print(f"[ltp] no security id for {symbol}")
        return None
    try:
        resp = dhan.ohlc_data(securities={SEGMENT: [sec_id]})
        ltp = resp.get('data', {}).get('data', {}).get(SEGMENT, {}).get(str(sec_id), {}).get('last_price')
        if ltp is not None:
            return float(ltp)
    except Exception as e:
        print(f"[ltp] error for {symbol}: {e}")
    return None

# ---------------- Broker order helpers -----------------

def fetch_symbol_slm_from_broker(symbol: str, expected_trans: str):
    ACCEPTABLE = {"OPEN", "PENDING", "TRIGGER PENDING", "VALIDATION PENDING", "PUT ORDER REQUEST RECEIVED"}
    best = None
    try:
        orders = dhan.get_order_list().get('data', []) or []
        for o in orders:
            prod = str(o.get('product') or '').upper()
            if prod not in ('INTRADAY', 'MIS'):
                continue
            if (o.get('variety') or '').lower() != 'regular':
                continue
            otype = (o.get('orderType') or o.get('order_type') or '').upper()
            if otype not in ('SL-M', 'SLM', 'SLM_ORDER'):
                continue
            if (o.get('tradingSymbol') or '').upper() != symbol.upper():
                continue
            if (o.get('transactionType') or o.get('transaction_type') or '').upper() != expected_trans.upper():
                continue
            if (o.get('status') or '').upper() not in ACCEPTABLE:
                continue
            cur = {'order_id': o.get('orderId') or o.get('order_id'), 'trigger_price': float(o.get('triggerPrice') or o.get('trigger_price') or 0.0), 'status': o.get('status'), 'updated_at': o.get('updatedAt') or o.get('orderTimestamp')}
            if not best or str(cur.get('updated_at','')) > str(best.get('updated_at','')):
                best = cur
    except Exception as e:
        print(f"[fetch_slm] {symbol} {e}")
    return best


def place_slm_order(symbol: str, direction: str, qty: int, trigger_price: float):
    if SIMULATION_MODE:
        print(f"[SIM SLM] {symbol} {direction} qty={qty} trig={trigger_price}")
        return f"SIM_SLM_{symbol}"
    try:
        trans = 'SELL' if direction.upper() == 'BUY' else 'BUY'
        # prefer using provided security id mapping when available
        sec_id = resolve_security_id(symbol)
        payload = {
            'exchangeSegment': SEGMENT,
            'securityId': sec_id,
            'orderType': 'SL-M',
            'transactionType': trans,
            'quantity': qty,
            'triggerPrice': trigger_price,
            'price': 0.0,
            'productType': 'INTRADAY',
            'variety': 'regular'
        }
        r = dhan.place_order(payload)
        oid = None
        if isinstance(r, dict):
            oid = r.get('data', {}).get('orderId') or r.get('data', {}).get('order_id') or r.get('orderId')
        print(f"[place_slm] {symbol} oid={oid} trig={trigger_price}")
        return oid
    except Exception as e:
        print(f"[place_slm] {symbol} {e}")
        return None


def cancel_order_by_id(order_id: str):
    if SIMULATION_MODE:
        print(f"[SIM cancel] oid={order_id}")
        return True
    try:
        dhan.cancel_order(order_id)
        return True
    except Exception as e:
        print(f"[cancel] {order_id} {e}")
        return False

# ---------------- Reconcile orphan SL/SL-M orders -----------------

def reconcile_orphan_orders(active_symbols: set):
    if not before_cutoff():
        print('[reconcile] cutoff reached - skipping orphan cancellation')
        return
    try:
        orders = dhan.get_order_list().get('data', []) or []
        for o in orders:
            otype = (o.get('orderType') or o.get('order_type') or '').upper()
            if otype not in ('SL', 'SL-M', 'SLM'):
                continue
            sym = (o.get('tradingSymbol') or o.get('trading_symbol') or '').upper()
            if sym in active_symbols:
                continue
            status = (o.get('status') or '').upper()
            if status not in ('OPEN', 'PENDING', 'TRIGGER PENDING', 'VALIDATION PENDING'):
                continue
            try:
                cancel_order_by_id(o.get('orderId') or o.get('order_id'))
                print(f"[reconcile] canceled orphan SL for {sym} oid={o.get('orderId') or o.get('order_id')}")
            except Exception as e:
                print(f"[reconcile] cancel failed for {sym}: {e}")
    except Exception as e:
        print(f"[reconcile] listing orders failed: {e}")

# ---------------- SL computation using Dhan OHLC -----------------

def compute_sl_and_meta(symbol: Optional[str], direction: str, sec_id: Optional[int] = None):
    """Compute trigger using Dhan OHLC. Prefer sec_id if provided (from position)."""
    if sec_id is None and symbol is not None:
        sec_id = resolve_security_id(symbol)
    if not sec_id:
        print(f"[compute_sl] no security id for {symbol}")
        return None, None
    try:
        resp = dhan.ohlc_data(securities={SEGMENT: [sec_id]})
        data = resp.get('data', {}).get('data', {}).get(SEGMENT, {}).get(str(sec_id), {})
        ohlc = data.get('ohlc') or {}
        low = ohlc.get('low')
        high = ohlc.get('high')
        raw = low if direction.upper() == 'BUY' else high
        if raw is None:
            return None, None
        trig = round_to_tick(float(raw))
        return trig, {'raw': raw, 'ohlc': ohlc}
    except Exception as e:
        print(f"[compute_sl] {symbol} {e}")
        return None, None

# ---------------- Fast watcher -----------------

def fast_order_watcher(stop_event: threading.Event):
    """
    Fast watcher runs every 5 seconds (fixed cadence):
      - fetch current INTRADAY positions via get_active_mis_positions()
      - cancel SL / SL-M orders for symbols that no longer have INTRADAY positions (orphans)
      - ensure each active INTRADAY position has a matching SL-M; if missing, place one using the latest 5m extreme

    This watcher intentionally does NOT modify existing SL-M orders — only places missing SLMs and cancels orphan SLMs.
    """
    print('[watcher] fast watcher started (5s cadence) for Dhan (SDK)')
    while not stop_event.is_set() and before_cutoff():
        try:
            positions = get_active_mis_positions()
            active_symbols = set([p['tradingSymbol'].upper() for p in positions])

            # 1) Cancel orphan SL/SL-M orders (orders with no corresponding active position)
            try:
                orders = dhan.get_order_list().get('data', []) or []
                for o in orders:
                    otype = (o.get('orderType') or o.get('order_type') or '').upper()
                    if otype not in ('SL', 'SL-M', 'SLM'):
                        continue
                    sym = (o.get('tradingSymbol') or o.get('trading_symbol') or '').upper()
                    if sym in active_symbols:
                        continue
                    status = (o.get('status') or '').upper()
                    if status not in ('OPEN', 'PENDING', 'TRIGGER PENDING', 'VALIDATION PENDING'):
                        continue
                    try:
                        cancel_order_by_id(o.get('orderId') or o.get('order_id'))
                        print(f"[watcher-reconcile] canceled orphan SL for {sym} oid={o.get('orderId') or o.get('order_id')}")
                    except Exception as e:
                        print(f"[watcher-reconcile] cancel failed for {sym}: {e}")
            except Exception as e:
                print(f"[watcher-reconcile] listing/cancel orders failed: {e}")

            # 2) Ensure active INTRADAY positions have an SL-M. If missing, place one using last 5m extreme.
            try:
                for p in positions:
                    if not before_cutoff():
                        break
                    sym = p['tradingSymbol'].upper()
                    direction = p['positionType']
                    qty = int(p['netQty'])
                    sec_id = p.get('securityId')
                    expected_trans = 'SELL' if direction.upper() == 'BUY' else 'BUY'
                    existing_slm = fetch_symbol_slm_from_broker(sym, expected_trans)
                    if existing_slm:
                        # SL-M already present — skip
                        continue
                    # compute trigger (uses sec_id if provided)
                    new_trig, last = compute_sl_and_meta(sym, direction, sec_id=sec_id)
                    if not new_trig:
                        print(f"[watcher] cannot compute trig for {sym} -> skipping")
                        continue
                    # place SL-M
                    oid = place_slm_order(sym, direction, qty, new_trig)
                    if oid:
                        print(f"[watcher] placed SL-M for {sym} trig={new_trig} oid={oid}")
                    else:
                        print(f"[watcher] failed to place SL-M for {sym}")
            except Exception as e:
                print(f"[watcher] ensure-SLM loop error: {e}")

        except Exception as e:
            print(f"[watcher] error: {e}")

        # fixed 5-second cadence
        stop_event.wait(5)
    print('[watcher] exiting')

# ---------------- Trail loop (5-min cadence) -----------------

def trail_loop(poll_interval_seconds=300):
    print('[trail] start (Dhan place-only mode - SDK)')
    while True:
        if not before_cutoff():
            print('[trail] cutoff reached — stopping')
            break
        positions = get_active_mis_positions()
        active_syms = set([p['tradingSymbol'].upper() for p in positions])

        reconcile_orphan_orders(active_syms)

        if not positions:
            print('[trail] no active positions — sleeping')
        for p in positions:
            direction = p['positionType']
            sym = p['tradingSymbol'].upper()
            qty = int(p['netQty'])
            sec_id = p.get('securityId')
            new_trig, last = compute_sl_and_meta(sym, direction, sec_id=sec_id)
            if not new_trig:
                print(f"[diag] {sym} cannot compute new_trig — skipping")
                continue
            expected_trans = 'SELL' if direction.upper() == 'BUY' else 'BUY'
            existing_slm = fetch_symbol_slm_from_broker(sym, expected_trans)
            if existing_slm:
                print(f"[diag] {sym} existing SL-M present (oid={existing_slm.get('order_id')}) — skipping placement")
                continue
            oid = place_slm_order(sym, direction, qty, new_trig)
            if oid:
                print(f"[diag] {sym} placed SL-M trig={new_trig} oid={oid}")
            else:
                print(f"[diag] {sym} failed to place SL-M")

        # sleep until next 5-min boundary + small offset
        now = now_ist()
        minute_block = (now.minute // 5) * 5
        base = now.replace(minute=minute_block, second=0, microsecond=0)
        candidate = base + timedelta(minutes=5, seconds=TRAIL_OFFSET_SECONDS)
        if candidate <= now:
            candidate += timedelta(minutes=5)
        sleep_s = max(1, (candidate - now).total_seconds())
        print(f"[trail] sleeping {int(sleep_s)}s until next tick {candidate.strftime('%H:%M:%S')}")
        time.sleep(sleep_s)


if __name__ == '__main__':
    load_instruments_once()
    stop_evt = threading.Event()
    watcher_thread = threading.Thread(target=fast_order_watcher, args=(stop_evt,), daemon=True)
    watcher_thread.start()

    try:
        trail_loop()
    except KeyboardInterrupt:
        print('stopped by user')
    finally:
        stop_evt.set()
        watcher_thread.join(timeout=3)
        print('exiting')
