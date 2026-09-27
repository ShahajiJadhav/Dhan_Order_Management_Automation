from __future__ import annotations

import asyncio
import datetime
import io
import json
import logging
import math
import os
from pathlib import Path
import socket
import struct
import sys
import time
from typing import Optional, Dict, Any
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import httpx
import pandas as pd
import websockets

# ─────────────────────────────────────────────────────────────
# CONFIGURATION & CONSTANTS
# ─────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"
SECURITY_FILE = BASE_DIR / "Security_IDs.csv"
LOG_FILE = BASE_DIR / "dhan_momentum_engine.log"

load_dotenv(ENV_FILE, override=True)

DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "").strip()
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

if not all((DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)):
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN in .env")

WSS_BASE = "wss://api-feed.dhan.co"
BATCH_SIZE = 100
RESPONSE_CODE_QUOTE = 4

# Strict 1-Minute Strategy Rules
PRICE_MOVE_THRESHOLD = 1.0         # >= +1.0% for BUY, <= -1.0% for SELL
MIN_TOTAL_TV_CR = 20.0             # 1-minute Traded Value must be > 20 Crore
SUPER_PCT_ABOVE_60L = 35.0         # Super Tier: >= 35% ticks strictly > 60L
MIN_PCT_10_TO_60L = 60.0           # Standard Tier: >= 60% ticks between 10L and 60L
MIN_PCT_ABOVE_60L = 10.0           # Standard Tier: >= 10% ticks > 60L
MIN_TICKS_PER_BAR = 12             # Filter out illiquid sparse symbols
ALERT_COOLDOWN_MINS = 3            # Cooldown per symbol per side

IST = ZoneInfo("Asia/Kolkata")
MARKET_START = datetime.time(9, 15)
MARKET_CLOSE = datetime.time(15, 30)

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
def _safe_stream_handler() -> logging.StreamHandler:
    stream = (
        io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
        if sys.platform == "win32"
        else sys.stdout
    )
    h = logging.StreamHandler(stream)
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    return h

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[_safe_stream_handler(), logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
logger = logging.getLogger("DhanMomentumEngine")

# ─────────────────────────────────────────────────────────────
# GLOBAL STATE
# ─────────────────────────────────────────────────────────────
http_client: Optional[httpx.AsyncClient] = None
ALERT_QUEUE: asyncio.Queue[str] = asyncio.Queue()

SECURITY_ID_MAP: dict[int, str] = {}
LAST_VOLUME: dict[int, int] = {}
TRACKERS: dict[int, Pure1MinTracker] = {}

# ─────────────────────────────────────────────────────────────
# PURE 1-MINUTE BROKER BAR TRACKER (O(1) Memory Engine)
# ─────────────────────────────────────────────────────────────
class Pure1MinTracker:
    __slots__ = (
        'symbol', 'current_minute',
        'open_ltp', 'last_ltp', 'last_vol',
        'cnt_10_60', 'cnt_above_60', 'total_ticks', 'total_tv',
        'last_signal_min'
    )

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.current_minute = -1
        self.open_ltp = 0.0
        self.last_ltp = 0.0
        self.last_vol = 0
        self.cnt_10_60 = 0
        self.cnt_above_60 = 0
        self.total_ticks = 0
        self.total_tv = 0.0
        self.last_signal_min = {"BUY": -10, "SELL": -10}

    def process_tick(self, epoch_sec: float, ltp: float, total_vol: int) -> Optional[Dict[str, Any]]:
        # Integer division by 60 matches the broker's 1-minute clock boundary
        minute_bucket = int(epoch_sec // 60)
        signal = None

        # -------------------------------------------------------------
        # MINUTE ROLLOVER: Evaluate the closed 1-minute candle
        # -------------------------------------------------------------
        if minute_bucket != self.current_minute:
            if self.current_minute != -1 and self.total_ticks >= MIN_TICKS_PER_BAR:
                pct_10_60 = (self.cnt_10_60 / self.total_ticks) * 100.0
                pct_above_60 = (self.cnt_above_60 / self.total_ticks) * 100.0
                price_diff = ((self.last_ltp - self.open_ltp) / self.open_ltp) * 100.0

                # 1. Total Traded Value must exceed 20 Crore & Price Move >= 1.0%
                if (self.total_tv > MIN_TOTAL_TV_CR) and (abs(price_diff) >= PRICE_MOVE_THRESHOLD):
                    active_sig = None

                    # Check Tier A: Super Tier (>= 35% above 60 Lakhs)
                    if pct_above_60 >= SUPER_PCT_ABOVE_60L:
                        active_sig = "SUPER_BUY" if price_diff >= PRICE_MOVE_THRESHOLD else "SUPER_SELL"

                    # Check Tier B: Standard Tier (>= 60% in 10L-60L AND >= 10% above 60L)
                    elif (pct_10_60 >= MIN_PCT_10_TO_60L) and (pct_above_60 >= MIN_PCT_ABOVE_60L):
                        active_sig = "BUY" if price_diff >= PRICE_MOVE_THRESHOLD else "SELL"

                    # Debounce per side
                    if active_sig:
                        base_side = "BUY" if "BUY" in active_sig else "SELL"
                        if (self.current_minute - self.last_signal_min[base_side] >= ALERT_COOLDOWN_MINS):
                            self.last_signal_min[base_side] = self.current_minute
                            signal = {
                                "symbol": self.symbol,
                                "signal_type": active_sig,
                                "side": base_side,
                                "is_super": "SUPER" in active_sig,
                                "minute_timestamp": self.current_minute * 60,
                                "open_price": self.open_ltp,
                                "close_price": self.last_ltp,
                                "price_diff_pct": round(price_diff, 2),
                                "pct_10_to_60L": round(pct_10_60, 1),
                                "pct_above_60L": round(pct_above_60, 1),
                                "total_ticks": self.total_ticks,
                                "total_tv_cr": round(self.total_tv, 2)
                            }

            # Reset state for the new 1-minute candle
            self.current_minute = minute_bucket
            self.open_ltp = ltp
            self.cnt_10_60 = 0
            self.cnt_above_60 = 0
            self.total_ticks = 0
            self.total_tv = 0.0

        # Baseline capture on initial tick
        if self.last_ltp == 0.0 or self.last_vol == 0:
            self.last_ltp = ltp
            self.last_vol = total_vol
            return signal

        # -------------------------------------------------------------
        # TICK PROCESSING HOT PATH
        # -------------------------------------------------------------
        delta_qty = total_vol - self.last_vol
        if delta_qty < 0:
            delta_qty = 0

        # Traded value in Crores: (qty * ltp) / 10^7
        tv_cr = (delta_qty * ltp) * 0.0000001
        self.last_ltp = ltp
        self.last_vol = total_vol

        if 0.10 <= tv_cr <= 0.60:
            self.cnt_10_60 += 1
        elif tv_cr > 0.60:
            self.cnt_above_60 += 1

        self.total_ticks += 1
        self.total_tv += tv_cr

        return signal

# ─────────────────────────────────────────────────────────────
# TELEGRAM NOTIFIER
# ─────────────────────────────────────────────────────────────
async def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or http_client is None:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        await http_client.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=4.0
        )
    except Exception as exc:
        logger.error("[telegram] post failed: %s", exc)

async def alert_dispatcher():
    while True:
        await asyncio.sleep(1.0)
        messages = []
        while not ALERT_QUEUE.empty():
            try:
                messages.append(ALERT_QUEUE.get_nowait())
            except asyncio.QueueEmpty:
                break
        if not messages:
            continue
        batch = ""
        for m in messages:
            candidate = f"{batch}\n\n{m}" if batch else m
            if len(candidate) > 3500:
                await send_telegram(batch)
                batch = m
            else:
                batch = candidate
        if batch:
            await send_telegram(batch)

async def queue_alert(message: str):
    await ALERT_QUEUE.put(message)

# ─────────────────────────────────────────────────────────────
# INSTRUMENTS LOADING
# ─────────────────────────────────────────────────────────────
def load_securities(path: Path):
    global SECURITY_ID_MAP, TRACKERS
    df = pd.read_csv(path, sep=None, engine="python")
    df.columns = [str(c).strip().upper() for c in df.columns]

    sec_col = next((c for c in df.columns if "SECURITY" in c or "SEC_ID" in c), None)
    sym_col = next((c for c in df.columns if "SYMBOL" in c or "TRADING" in c), None)

    if not sec_col or not sym_col:
        raise ValueError("Missing security ID or symbol column in CSV")

    df = df.dropna(subset=[sec_col, sym_col])
    df[sec_col] = pd.to_numeric(df[sec_col], errors="coerce").fillna(0).astype(int)
    df[sym_col] = df[sym_col].astype(str).str.strip().str.upper()
    df = df[df[sec_col] > 0]

    for _, row in df.iterrows():
        sid = int(row[sec_col])
        sym = str(row[sym_col])
        SECURITY_ID_MAP[sid] = sym
        TRACKERS[sid] = Pure1MinTracker(sym)

    logger.info("Loaded %d securities from %s", len(SECURITY_ID_MAP), path.name)

# ─────────────────────────────────────────────────────────────
# WEBSOCKET STREAMING & SIGNAL ENGINE
# ─────────────────────────────────────────────────────────────
async def handle_tick(sec_id: int, ltp: float, volume: int, now_ts: float):
    tracker = TRACKERS.get(sec_id)
    if not tracker:
        return

    signal = tracker.process_tick(now_ts, ltp, volume)
    if signal:
        sym = signal["symbol"]
        sig_type = signal["signal_type"]
        clock_time = time.strftime('%H:%M', time.localtime(signal['minute_timestamp']))

        if sig_type == "SUPER_BUY":
            header = "🔥⚡ <b>SUPER BULLISH BREAKOUT</b>"
        elif sig_type == "SUPER_SELL":
            header = "🚨💥 <b>SUPER BEARISH BREAKDOWN</b>"
        elif sig_type == "BUY":
            header = "🟢 <b>BUY BREAKOUT</b>"
        else:
            header = "🔴 <b>SELL BREAKDOWN</b>"

        logger.info(
            "[%s] %s | %+0.2f%% | TV=₹%.2fCr | Ticks=%d (>60L: %.1f%%, 10L-60L: %.1f%%)",
            sig_type, sym, signal['price_diff_pct'], 
            signal['total_tv_cr'], signal['total_ticks'],
            signal['pct_above_60L'], signal['pct_10_to_60L']
        )

        tier_tag = " [SUPER: >60L >= 35%]" if signal["is_super"] else " [STANDARD]"
        alert_msg = (
            f"{header}: <b>{sym}</b>{tier_tag}\n"
            f"<b>Bar:</b> 1-Minute ({clock_time})\n"
            f"<b>Price Move:</b> ₹{signal['open_price']:.2f} ➔ ₹{signal['close_price']:.2f} (<b>{signal['price_diff_pct']:+0.2f}%</b>)\n"
            f"<b>Total Traded Value:</b> <b>₹{signal['total_tv_cr']} Cr</b> (Req: > ₹20 Cr)\n"
            f"<b>Ticks:</b> {signal['total_ticks']}\n"
            f"• > 60L: <b>{signal['pct_above_60L']}%</b>\n"
            f"• 10L – 60L: <b>{signal['pct_10_to_60L']}%</b>"
        )
        await queue_alert(alert_msg)

def parse_quote_packet(data: bytes):
    if len(data) < 26:
        return None
    try:
        response_code, _, _, sec_id = struct.unpack_from("<BHBI", data, 0)
        if response_code != RESPONSE_CODE_QUOTE:
            return None
        ltp = struct.unpack_from("<f", data, 8)[0]
        if not (math.isfinite(ltp) and 0.0 < ltp < 1e9):
            return None
        volume = struct.unpack_from("<I", data, 22)[0]
        return sec_id, ltp, volume
    except (struct.error, OverflowError):
        return None

# ─────────────────────────────────────────────────────────────
# MAIN CLIENT RUNNER
# ─────────────────────────────────────────────────────────────
async def run_client():
    global http_client
    http_client = httpx.AsyncClient(timeout=5.0)

    load_securities(SECURITY_FILE)
    sec_ids = list(SECURITY_ID_MAP.keys())

    logger.info(
        "[SCANNER MODE] Tracking %d instruments | Pure 1-Min Bars | TV > 20 Cr | Move >= +-1.0%%",
        len(sec_ids)
    )

    asyncio.create_task(alert_dispatcher())

    payloads = []
    for i in range(0, len(sec_ids), BATCH_SIZE):
        chunk = sec_ids[i:i + BATCH_SIZE]
        payloads.append(json.dumps({
            "RequestCode": 17,
            "InstrumentCount": len(chunk),
            "InstrumentList": [{"ExchangeSegment": "NSE_EQ", "SecurityId": str(s)} for s in chunk],
        }))

    feed_url = f"{WSS_BASE}?{urlencode({'version': '2', 'token': DHAN_ACCESS_TOKEN, 'clientId': DHAN_CLIENT_ID, 'authType': '2'})}"
    backoff = 2

    while True:
        try:
            logger.info("Connecting to Dhan WebSocket feed for %d instruments...", len(sec_ids))
            async with websockets.connect(feed_url, ping_interval=None, ping_timeout=None, max_size=10_000_000) as ws:
                try:
                    sock = ws.transport.get_extra_info("socket")
                    if sock:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                except Exception:
                    pass

                LAST_VOLUME.clear()
                for p in payloads:
                    await ws.send(p)
                    await asyncio.sleep(0.04)

                logger.info("Subscribed successfully to %d symbols. Listening for 1-minute breakouts...", len(sec_ids))
                backoff = 2

                while True:
                    raw = await ws.recv()
                    if not isinstance(raw, bytes):
                        continue
                    parsed = parse_quote_packet(raw)
                    if parsed:
                        sec_id, ltp, volume = parsed
                        await handle_tick(sec_id, ltp, volume, time.time())

        except websockets.ConnectionClosed as exc:
            logger.warning("WebSocket disconnected (%s: %s). Reconnecting in %ds...", exc.code, exc.reason, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except Exception as exc:
            logger.error("WebSocket runtime error: %s", exc, exc_info=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

if __name__ == "__main__":
    try:
        asyncio.run(run_client())
    except KeyboardInterrupt:
        logger.info("Scanner engine terminated by user.")