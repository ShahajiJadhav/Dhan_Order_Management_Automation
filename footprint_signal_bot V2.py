
import os
import csv
import time
import logging
import threading
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from dhanhq import DhanContext, MarketFeed

IST = ZoneInfo("Asia/Kolkata")

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

load_dotenv()

DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "").strip()
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# All paths default to the current working directory, so the same script
# and .env work unchanged whether run locally on Windows or on the
# DigitalOcean droplet -- just `cd` into wherever it lives before running.
BASE_DIR = os.getcwd()

# CSV with columns: SECURITY_ID, TRADING_SYMBOL, EXCHANGE_SEGMENT (e.g. NSE_EQ)
SECURITY_FILE_PATH = os.getenv("SECURITY_FILE_PATH", os.path.join(BASE_DIR, "Security_IDs.csv"))

LOG_DIR = os.path.join(BASE_DIR, "logs")
SIGNAL_LOG_PATH = os.path.join(LOG_DIR, "footprint_signals.csv")
APP_LOG_PATH = os.path.join(LOG_DIR, "footprint_bot.log")

TV_THRESHOLD = 19_500_000  # 1 Crore 95 Lakh, in rupees
MIN_GAP_PCT = 0.004         # 0.4% of the heavy level's own price

# If consecutive ticks for a symbol land on meaningfully different prices
# (not just paise-level feed noise), the volume delta between them almost
# certainly traded across a path, not entirely at the newer price. Above
# this gap, the delta is split 50/50 between the previous and current
# price instead of dumped entirely onto the current one.
PRICE_SPLIT_GAP_PCT = 0.001  # 0.1%

# If a single tick's price gap from the previous tick is >= this, AND that
# tick's own traded value is >= GAP_ALERT_TV_THRESHOLD, fire a standalone
# heads-up alert -- independent of the heavy-level BUY/SELL signals, and
# doesn't consume a symbol's buy/sell slot for the candle.
GAP_ALERT_PCT = 0.01                  # 1%
GAP_ALERT_TV_THRESHOLD = 450_000_000  # 45 Crore, in rupees

# Watchdog: Dhan's feed can go quiet without throwing an exception (socket
# looks alive but stops delivering ticks). If nothing arrives for this long
# during market hours, force a reconnect rather than sit there silently
# missing every crossing.
STALL_TIMEOUT_SEC = 30

# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------

os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("footprint_bot")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s")
_fh = logging.FileHandler(APP_LOG_PATH)
_fh.setFormatter(_fmt)
logger.addHandler(_fh)
_sh = logging.StreamHandler()
_sh.setFormatter(_fmt)
logger.addHandler(_sh)


def log(msg, level="info"):
    getattr(logger, level)(msg)


if not os.path.exists(SIGNAL_LOG_PATH):
    with open(SIGNAL_LOG_PATH, "w", newline="") as f:
        csv.writer(f).writerow(
            [
                "timestamp",
                "candle_minute",
                "security_id",
                "symbol",
                "signal",
                "ltp_at_signal",
                "heavy_level_price",
                "gap_pct_from_level",
            ]
        )

_signal_log_lock = threading.Lock()


def log_signal(security_id, symbol, direction, ltp, level_price, candle_minute):
    gap_pct = abs(ltp - level_price) / level_price * 100 if level_price else 0.0
    with _signal_log_lock:
        with open(SIGNAL_LOG_PATH, "a", newline="") as f:
            csv.writer(f).writerow(
                [
                    datetime.now().isoformat(timespec="seconds"),
                    candle_minute,
                    security_id,
                    symbol,
                    direction,
                    round(ltp, 2),
                    round(level_price, 2),
                    round(gap_pct, 3),
                ]
            )


# --------------------------------------------------------------------------
# Telegram alerts (with basic dedup)
# --------------------------------------------------------------------------

_last_alert_cache = {}
_ALERT_DEDUP_WINDOW_SEC = 2


def send_telegram(text):
    now = time.time()
    last = _last_alert_cache.get(text)
    if last and now - last < _ALERT_DEDUP_WINDOW_SEC:
        return
    _last_alert_cache[text] = now

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram not configured, skipping alert", "warning")
        return

    def _send():
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=5)
        except Exception as e:
            log(f"Telegram send failed: {e}", "error")

    threading.Thread(target=_send, daemon=True).start()


# --------------------------------------------------------------------------
# Symbol universe
# --------------------------------------------------------------------------

# Map CSV EXCHANGE_SEGMENT strings to dhanhq MarketFeed segment constants.
# Extend this if you subscribe non-NSE-equity segments too.
SEGMENT_MAP = {
    "NSE_EQ": MarketFeed.NSE,
    "NSE": MarketFeed.NSE,
    "BSE_EQ": MarketFeed.BSE,
    "BSE": MarketFeed.BSE,
}


def load_symbols(path):
    """
    Expects a CSV with columns: SECURITY_ID, TRADING_SYMBOL, EXCHANGE_SEGMENT
    """
    symbols = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            symbols.append(
                {
                    "security_id": str(row["SECURITY_ID"]).strip(),
                    "symbol": row["TRADING_SYMBOL"].strip(),
                    "segment": row.get("EXCHANGE_SEGMENT", "NSE_EQ").strip().upper(),
                }
            )
    log(f"Loaded {len(symbols)} symbols from {path}")
    return symbols


# --------------------------------------------------------------------------
# Per-symbol candle/state machine
# --------------------------------------------------------------------------

class SymbolState:
    __slots__ = ("candle_minute", "price_buckets", "heavy_levels", "buy_signaled", "sell_signaled", "last_price")

    def __init__(self):
        self.candle_minute = None
        self.price_buckets = defaultdict(float)
        self.heavy_levels = set()  # prices that have crossed TV_THRESHOLD this candle
        self.buy_signaled = False
        self.sell_signaled = False
        self.last_price = None  # last tick's price seen in this candle, for gap-split logic

    def reset_for_new_candle(self, minute_key):
        self.candle_minute = minute_key
        self.price_buckets = defaultdict(float)
        self.heavy_levels = set()
        self.buy_signaled = False
        self.sell_signaled = False
        self.last_price = None


symbol_states = {}   # security_id -> SymbolState
security_meta = {}   # security_id -> {"symbol": ..., "segment": ...}
last_cum_volume = {}  # security_id -> last seen cumulative day volume (NOT reset per candle)
_state_lock = threading.Lock()

_last_tick_ts = time.time()  # updated on every processed tick, watched by the stall monitor


def wipe_all_state(reason):
    """
    Called after any reconnect (crash or silent stall). The candle in
    progress at the moment of the gap has incomplete data -- rather than
    let signals fire off a partially-missing bucket, discard all symbol
    state so every symbol starts its next candle clean. Also resets the
    cumulative-volume baseline per symbol, since a single huge delta
    covering the whole outage would otherwise get misattributed entirely
    to whatever price the first post-reconnect tick happens to be at.
    """
    with _state_lock:
        n = len(symbol_states)
        symbol_states.clear()
        last_cum_volume.clear()
    msg = f"\u26A0\uFE0F Feed gap ({reason}) -- discarded in-flight candle state for {n} symbols"
    log(msg, "warning")
    send_telegram(msg)


def handle_tick(security_id, ltp, ltt, volume):
    """
    Core signal logic. Called once per Quote tick.

    Traded value is computed from the DELTA in cumulative day volume
    between this tick and the previous one for this symbol -- not from
    LTQ. Under high trade frequency, Dhan's Quote packets appear to be
    throttled (LTQ only reflects the single most recent trade, not the
    sum of everything since the last delivered packet), so LTQ-based
    accumulation silently underrepresents heavy bursts. The cumulative
    volume field doesn't have that problem: whatever the delivery rate,
    its delta since the last packet is the true quantity traded in
    between.
    """
    global _last_tick_ts
    meta = security_meta.get(security_id)
    if not meta or ltp is None or ltp <= 0 or volume is None:
        return

    _last_tick_ts = time.time()

    # Broker-timestamp-based 1-min candle bucket. LTT arrives as "HH:MM:SS"
    # (IST, exchange time) -- the "HH:MM" prefix alone is a stable per-minute
    # key, so no epoch/timezone math is needed. Falls back to server clock
    # only if LTT is ever missing from a packet.
    if ltt and len(ltt) >= 5:
        minute_key = ltt[:5]
    else:
        minute_key = datetime.now(IST).strftime("%H:%M")

    price = round(ltp, 2)

    with _state_lock:
        state = symbol_states.get(security_id)
        if state is None:
            state = SymbolState()
            symbol_states[security_id] = state

        is_new_candle = state.candle_minute != minute_key
        if is_new_candle:
            state.reset_for_new_candle(minute_key)

        prev_volume = last_cum_volume.get(security_id)
        last_cum_volume[security_id] = volume

        # Don't carry a volume baseline across a candle boundary. If this
        # symbol went quiet for a while and the next tick lands in a new
        # candle, crediting the whole accumulated delta to that new candle
        # would smear volume that likely traded over several past minutes
        # into a single price in the current one. Safer to just re-baseline
        # here (no signal from this tick) and only count delta from here on.
        if prev_volume is None or is_new_candle:
            return
        qty = volume - prev_volume
        if qty <= 0:
            return  # no new trade volume since the last tick, nothing to accumulate

        tv = ltp * qty
        prev_price = state.last_price
        state.last_price = price

        if prev_price is not None and prev_price != price:
            gap_pct = abs(price - prev_price) / prev_price
        else:
            gap_pct = 0.0

        if prev_price is not None and gap_pct >= GAP_ALERT_PCT and tv >= GAP_ALERT_TV_THRESHOLD:
            _fire_gap_alert(security_id, meta["symbol"], prev_price, price, gap_pct, tv, qty, minute_key)

        if prev_price is None or gap_pct < PRICE_SPLIT_GAP_PCT:
            # No prior price this candle, or the move is just feed noise --
            # attribute the whole delta to the current price as before.
            state.price_buckets[price] += tv
            _note_if_heavy(state, meta["symbol"], price, minute_key)
        else:
            # Meaningful price move with real volume in between -- split
            # rather than dump it all on the arrival price.
            half = tv / 2.0
            state.price_buckets[prev_price] += half
            state.price_buckets[price] += half
            _note_if_heavy(state, meta["symbol"], prev_price, minute_key)
            _note_if_heavy(state, meta["symbol"], price, minute_key)

        if not state.heavy_levels or (state.buy_signaled and state.sell_signaled):
            return

        _check_divergence(security_id, meta["symbol"], state, price, minute_key)


def _fire_gap_alert(security_id, symbol, prev_price, price, gap_pct, tv, qty, minute_key):
    msg = (
        f"\u26A1 Gap alert | {symbol} ({security_id}) | {prev_price} -> {price} "
        f"({gap_pct * 100:.2f}%) | qty {qty:,} | TV \u20b9{tv:,.0f} | candle {minute_key}"
    )
    log(msg)
    send_telegram(msg)
    log_signal(security_id, symbol, "GAP_ALERT", price, prev_price, minute_key)


def _note_if_heavy(state, symbol, price, minute_key):
    """First time this price's cumulative TV crosses the threshold this
    candle, note it as a heavy level (no signal yet)."""
    if state.price_buckets[price] > TV_THRESHOLD and price not in state.heavy_levels:
        state.heavy_levels.add(price)
        log(f"[{symbol}] candle {minute_key}: heavy level noted @ {price} "
            f"(TV {state.price_buckets[price]:,.0f})")


def _check_divergence(security_id, symbol, state, ltp, minute_key):
    """
    Compare the current tick's LTP against every heavy level noted so far
    in this candle. At most one BUY and one SELL signal fire per symbol
    per candle overall -- once a side has fired, further levels crossing
    the gap on that same side are ignored; the other side can still fire.
    """
    for level_price in state.heavy_levels:
        if state.buy_signaled and state.sell_signaled:
            return
        min_gap = level_price * MIN_GAP_PCT
        diff = ltp - level_price
        if diff >= min_gap and not state.buy_signaled:
            state.buy_signaled = True
            _fire_signal(security_id, symbol, state, "BUY", level_price, ltp, minute_key)
        elif -diff >= min_gap and not state.sell_signaled:
            state.sell_signaled = True
            _fire_signal(security_id, symbol, state, "SELL", level_price, ltp, minute_key)


def _fire_signal(security_id, symbol, state, direction, level_price, ltp, minute_key):
    msg = (
        f"{direction} signal | {symbol} ({security_id}) | ltp {ltp} vs heavy level {level_price} | "
        f"candle {minute_key}"
    )
    log(msg)
    send_telegram(f"\U0001F4CA {msg}")
    log_signal(security_id, symbol, direction, ltp, level_price, minute_key)


# --------------------------------------------------------------------------
# Packet handling (dhanhq SDK gives us parsed dicts, no manual struct work)
# --------------------------------------------------------------------------

def handle_packet(packet: dict):
    raw_sec_id = packet.get("security_id")
    if raw_sec_id is None:
        return
    security_id = str(raw_sec_id)
    if security_id not in security_meta:
        return

    try:
        ltp = float(packet.get("LTP") or packet.get("ltp") or 0.0)
    except (ValueError, TypeError):
        ltp = 0.0

    try:
        volume = int(packet.get("volume") or packet.get("Volume") or 0)
    except (ValueError, TypeError):
        volume = 0

    # LTT arrives as "HH:MM:SS" (broker/exchange time), not epoch seconds --
    # passed through as-is, parsed in handle_tick.
    ltt = packet.get("LTT") or packet.get("ltt")

    if ltp <= 0 or volume <= 0:
        return

    handle_tick(security_id, ltp, ltt, volume)


# --------------------------------------------------------------------------
# Stall watchdog
# --------------------------------------------------------------------------

_force_reconnect_event = threading.Event()
_first_packet_logged = [False]  # mutable single-element list so it's reset per run, not per connect


def is_market_hours():
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    return (9, 15) <= (now.hour, now.minute) <= (15, 30)


def stall_watchdog():
    while True:
        time.sleep(5)
        if not is_market_hours():
            continue
        idle = time.time() - _last_tick_ts
        if idle > STALL_TIMEOUT_SEC and not _force_reconnect_event.is_set():
            log(f"No ticks received for {idle:.0f}s during market hours -- forcing reconnect", "warning")
            _force_reconnect_event.set()


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def main():
    if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
        log("DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN missing from environment", "error")
        return

    symbols = load_symbols(SECURITY_FILE_PATH)
    for s in symbols:
        security_meta[s["security_id"]] = {"symbol": s["symbol"], "segment": s["segment"]}

    instruments = [
        (SEGMENT_MAP.get(s["segment"], MarketFeed.NSE), s["security_id"], MarketFeed.Quote)
        for s in symbols
    ]

    log(f"Starting footprint signal bot for {len(instruments)} instruments")
    send_telegram(f"\u2705 Footprint signal bot starting ({len(instruments)} symbols)")

    dhan_context = DhanContext(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)

    threading.Thread(target=stall_watchdog, name="StallWatchdog", daemon=True).start()

    global _last_tick_ts
    retry_count = 0
    first_connect = True
    while True:
        try:
            log("Connecting Dhan MarketFeed WebSocket...")
            feed = MarketFeed(dhan_context, instruments, version="v2")

            if not first_connect:
                # This is a reconnect (after a crash or a forced stall
                # recovery) -- the candle in progress during the gap has
                # incomplete data, so discard it rather than risk a signal
                # built off partial ticks.
                wipe_all_state(f"reconnect after {retry_count} attempt(s)")
            first_connect = False
            retry_count = 0  # reset backoff only after a fully successful connect
            _last_tick_ts = time.time()
            _force_reconnect_event.clear()
            logged_connected = False

            while True:
                if _force_reconnect_event.is_set():
                    _force_reconnect_event.clear()
                    raise ConnectionError("Stall watchdog: no ticks received, forcing reconnect")

                # Per the SDK's documented usage, run_forever() must be
                # called before every get_data() -- it pumps the async
                # event loop for this cycle rather than blocking forever
                # despite the name. Calling it once outside this loop (as
                # an earlier version of this script did) means the feed
                # never gets pumped again after the first packet, and
                # get_data() silently returns nothing forever after.
                feed.run_forever()
                if not logged_connected:
                    log("WebSocket connected. Streaming ticks...")
                    logged_connected = True

                packet = feed.get_data()
                if not packet:
                    continue

                if not _first_packet_logged[0]:
                    # One-time raw dump so we can confirm the SDK's actual
                    # field names/packet-type string match what this script
                    # expects, without leaving debug noise in every run.
                    log(f"First raw packet received: {packet}")
                    _first_packet_logged[0] = True

                packet_type = packet.get("type", "")
                if "Quote" in packet_type or "Full" in packet_type:
                    handle_packet(packet)

        except Exception as e:
            retry_count += 1
            err_text = str(e)
            if "429" in err_text:
                # Rate limited by Dhan -- fast retries here only make it worse,
                # so give it a dedicated longer wait regardless of retry_count.
                wait_time = 60
                log(f"[WEBSOCKET CRASH] Rate limited (429). Backing off {wait_time}s (attempt #{retry_count}): {e}", "error")
            else:
                wait_time = min(120, 5 * retry_count)
                log(f"[WEBSOCKET CRASH] {e}. Reconnecting in {wait_time}s (attempt #{retry_count})...", "error")
            time.sleep(wait_time)


if __name__ == "__main__":
    main()


# --------------------------------------------------------------------------
# Suggested systemd unit -- save as /etc/systemd/system/footprint-bot.service
#
# All input/output paths (Security_IDs.csv, logs/) resolve relative to
# WorkingDirectory below (os.getcwd() in the script) -- point it at
# wherever you deploy the bot on the droplet, e.g. /root/Bot.
# --------------------------------------------------------------------------
#
# [Unit]
# Description=Dhan Footprint Signal Bot
# After=network.target
#
# [Service]
# Type=simple
# WorkingDirectory=/root/Bot
# ExecStart=/usr/bin/python3 /root/Bot/footprint_signal_bot.py
# Restart=always
# RestartSec=5
# EnvironmentFile=/root/Bot/.env
#
# [Install]
# WantedBy=multi-user.target
#
# Then:
#   systemctl daemon-reload
#   systemctl enable footprint-bot
#   systemctl start footprint-bot
#   journalctl -u footprint-bot -f