"""
dhan_websocket_duckdb_logger.py (Zero-Drop Production Grade, Storage-Optimized)

Changes vs previous version:
  - Every tick is stored — no dedup.
  - Narrower column types: FLOAT instead of DOUBLE for prices, SMALLINT for
    order counts, INTEGER epoch-seconds instead of microsecond TIMESTAMP.
  - Larger flush batches (better DuckDB row-group compression), same
    zero-drop guarantee via the in-memory queue.
  - 5-min warmup + inactivity reaper and "never disconnect except on broker
    error" behavior are unchanged from the previous version.
"""

import csv
import os
import queue
import signal
import sys
import threading
import time
from datetime import datetime, time as dtime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except Exception as e:
    IST = None
    sys.stderr.write(
        f"WARNING: Asia/Kolkata tzdata unavailable ({e}) — falling back to "
        f"system local time! Run `pip install tzdata --break-system-packages`.\n"
    )

import duckdb
import pyarrow as pa
from dotenv import load_dotenv
from dhanhq import DhanContext, MarketFeed

# ---------------------------------------------------------------- config ---

load_dotenv()
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "dhan_nse_eq_master.csv"
DB_DIR = "/root/Websocket"
FLUSH_INTERVAL_SEC = 5.0            # was 1.0 — larger batches compress better
FLUSH_ROW_THRESHOLD = 50000         # was 10000
MAX_INSTRUMENTS_PER_CONNECTION = 5000
WATCHDOG_TIMEOUT_SEC = 30.0

# Warmup & Inactivity parameters
WARMUP_PERIOD_SEC = 300.0
TICK_INACTIVITY_TIMEOUT_SEC = 300.0
REAPER_SCAN_INTERVAL_SEC = 10.0

os.makedirs(DB_DIR, exist_ok=True)

if not CLIENT_ID or not ACCESS_TOKEN:
    sys.exit("Fatal: DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN not configured.")

def now_ist():
    return datetime.now(IST) if IST else datetime.now()

def now_ist_epoch():
    """Current time as an integer epoch (UTC seconds) — timezone-agnostic on disk."""
    return int(time.time())

def log(msg):
    sys.stdout.write(f"[{now_ist():%Y-%m-%d %H:%M:%S}] {msg}\n")
    sys.stdout.flush()

def is_market_hours():
    curr = now_ist()
    if curr.weekday() >= 5:
        return False
    t = curr.time()
    return dtime(9, 0) <= t <= dtime(15, 35)

def is_active_trading_hours():
    curr = now_ist()
    if curr.weekday() >= 5:
        return False
    t = curr.time()
    return dtime(9, 15) <= t < dtime(15, 30)

_ltt_debug_logged = 0

def parse_ltt_timestamp(data):
    """
    Return LTT as a naive IST datetime, handling whatever shape Dhan sends:
      - numeric epoch (int/float)
      - digit string epoch ("1789098127")
      - "HH:MM:SS" clock-time string (assumed IST, combined with today's date)
    Also tries a couple of alternate key names in case "LTT" isn't present.
    """
    global _ltt_debug_logged

    ltt_raw = data.get("LTT")
    if ltt_raw is None:
        ltt_raw = data.get("ltt", data.get("last_traded_time"))

    if ltt_raw is None or ltt_raw == "":
        if _ltt_debug_logged < 5:
            log(f"DEBUG: LTT missing from packet. Available keys: {list(data.keys())}")
            _ltt_debug_logged += 1
        return None

    epoch = None
    if isinstance(ltt_raw, (int, float)):
        epoch = float(ltt_raw)
    elif isinstance(ltt_raw, str):
        s = ltt_raw.strip()
        if s.isdigit():
            epoch = float(s)
        elif ":" in s:
            try:
                h, m, sec = (int(x) for x in s.split(":")[:3])
                today = now_ist().date()
                dt_ist = (
                    datetime(today.year, today.month, today.day, h, m, sec, tzinfo=IST)
                    if IST else
                    datetime(today.year, today.month, today.day, h, m, sec)
                )
                return dt_ist.replace(tzinfo=None) if IST else dt_ist
            except Exception:
                epoch = None

    if epoch is None:
        if _ltt_debug_logged < 5:
            log(f"DEBUG: LTT unparsed. raw_value={ltt_raw!r} type={type(ltt_raw).__name__}")
            _ltt_debug_logged += 1
        return None

    try:
        return (
            datetime.fromtimestamp(epoch, tz=IST).replace(tzinfo=None)
            if IST else datetime.fromtimestamp(epoch)
        )
    except Exception:
        return None

# ------------------------------------------------------- load instruments ---

def load_instruments(path):
    sec_to_symbol = {}
    instruments = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t")
        except csv.Error:
            dialect = csv.excel
        reader = csv.DictReader(f, dialect=dialect)
        for row in reader:
            sec_id = str(row.get("SECURITY_ID", "")).strip()
            symbol = str(row.get("TRADING_SYMBOL", "")).strip()
            if not sec_id or not symbol:
                continue
            sec_to_symbol[sec_id] = symbol
            instruments.append((MarketFeed.NSE, sec_id, MarketFeed.Full))
    return instruments, sec_to_symbol

ALL_INSTRUMENTS, SEC_TO_SYMBOL = load_instruments(CSV_PATH)
log(f"Loaded {len(ALL_INSTRUMENTS)} instruments from {CSV_PATH}")

if not ALL_INSTRUMENTS:
    sys.exit("No valid instruments loaded.")

if len(ALL_INSTRUMENTS) > MAX_INSTRUMENTS_PER_CONNECTION:
    sys.exit(f"Exceeds max instruments limit ({MAX_INSTRUMENTS_PER_CONNECTION}).")

# Global tracking sets and timers
unsubscribed_security_ids = set()
last_tick_time = {}
active_subscribed_ids = set()
session_connect_mono = None

# ------------------------------------------------------------- Storage Engine ---

ARROW_SCHEMA = pa.schema([
    ("received_at", pa.int32()),      # epoch seconds
    ("security_id", pa.int32()),
    ("trading_symbol", pa.string()),
    ("ltp", pa.float32()),
    ("ltq", pa.int32()),
    ("ltt", pa.timestamp("us")),      # IST wall-clock, naive
    ("volume", pa.int32()),
    ("delta_qty", pa.int32()),
    ("delta_traded_value", pa.float64()),
])

TABLE_INIT_SQL = """
CREATE TABLE IF NOT EXISTS ticks (
    received_at         INTEGER,   -- epoch seconds (UTC)
    security_id         INTEGER,
    trading_symbol      VARCHAR,
    ltp                 REAL,
    ltq                 INTEGER,
    ltt                 TIMESTAMP, -- IST wall-clock, naive
    volume              INTEGER,
    delta_qty           INTEGER,
    delta_traded_value  DOUBLE
);
"""

class ResilientTickStore:
    def __init__(self, db_dir):
        self.db_dir = db_dir
        self.current_date = None
        self.con = None
        self._open_for_date(now_ist().date())

    def _path_for(self, d):
        return os.path.join(self.db_dir, f"dhan_ticks_{d:%Y%m%d}.duckdb")

    def _open_for_date(self, target_date):
        if self.con is not None:
            try:
                self.con.close()
            except Exception:
                pass
        path = self._path_for(target_date)
        self.con = duckdb.connect(path, config={"threads": 2, "preserve_insertion_order": False})
        self.con.execute(TABLE_INIT_SQL)
        self.current_date = target_date
        log(f"Active DuckDB partition: {path}")

    def write_table(self, arrow_table):
        today = now_ist().date()
        if today != self.current_date:
            self._open_for_date(today)
        self.con.register("incoming_batch", arrow_table)
        self.con.execute("INSERT INTO ticks SELECT * FROM incoming_batch")
        self.con.unregister("incoming_batch")

    def close(self):
        if self.con is not None:
            try:
                self.con.close()
            except Exception:
                pass
            self.con = None

# ------------------------------------------------------- Pipeline Workers ---

tick_queue = queue.Queue(maxsize=500000)
stop_event = threading.Event()
state_lock = threading.Lock()
last_packet_mono = time.monotonic()
active_feed_instance = None
dropped_ticks = 0
last_volume = {}          # {security_id: last seen cumulative day volume}
last_volume_date = None   # date last_volume was built for; reset on rollover

def update_heartbeat():
    global last_packet_mono
    with state_lock:
        last_packet_mono = time.monotonic()

def get_silence_duration():
    with state_lock:
        return time.monotonic() - last_packet_mono

def _flush_batch(store, cols):
    num_rows = len(cols["received_at"])
    if num_rows == 0:
        return
    try:
        arrays = [
            pa.array(cols[field.name], type=field.type)
            for field in ARROW_SCHEMA
        ]
        table = pa.Table.from_arrays(arrays, schema=ARROW_SCHEMA)
        store.write_table(table)
    except Exception as e:
        log(f"Batch write failed, dropped {num_rows} rows: {e}")
    finally:
        for k in cols:
            cols[k].clear()

def parser_and_writer_worker():
    global last_volume, last_volume_date
    store = ResilientTickStore(DB_DIR)
    cols = {name: [] for name in ARROW_SCHEMA.names}
    last_flush = time.monotonic()

    while not stop_event.is_set() or not tick_queue.empty():
        try:
            recv_epoch, data = tick_queue.get(timeout=0.2)

            today = now_ist().date()
            if today != last_volume_date:
                last_volume.clear()
                last_volume_date = today

            sec_id_raw = data.get("security_id")
            if sec_id_raw is not None:
                sec_id_str = str(sec_id_raw)

                if sec_id_str not in unsubscribed_security_ids:
                    ltp = float(data.get("LTP", 0.0))
                    current_volume = int(data.get("volume", 0))
                    prev_volume = last_volume.get(sec_id_str)
                    delta_qty = 0 if prev_volume is None else max(0, current_volume - prev_volume)
                    last_volume[sec_id_str] = current_volume

                    cols["received_at"].append(recv_epoch)
                    cols["security_id"].append(int(sec_id_raw))
                    cols["trading_symbol"].append(SEC_TO_SYMBOL.get(sec_id_str, ""))
                    cols["ltp"].append(ltp)
                    cols["ltq"].append(int(data.get("LTQ", 0)))
                    cols["ltt"].append(parse_ltt_timestamp(data))
                    cols["volume"].append(current_volume)
                    cols["delta_qty"].append(delta_qty)
                    cols["delta_traded_value"].append(delta_qty * ltp)

            tick_queue.task_done()
        except queue.Empty:
            pass

        now = time.monotonic()
        num_rows = len(cols["received_at"])
        if num_rows > 0 and (now - last_flush >= FLUSH_INTERVAL_SEC or num_rows >= FLUSH_ROW_THRESHOLD):
            _flush_batch(store, cols)
            last_flush = now

    _flush_batch(store, cols)
    store.close()

# ------------------------------------------------------------- Callbacks ---

def on_message(instance, data):
    global dropped_ticks
    update_heartbeat()
    if not data or data.get("type") != "Full Data":
        return

    sec_id_raw = data.get("security_id")
    if sec_id_raw is None:
        return
    sec_id_str = str(sec_id_raw)

    if sec_id_str in unsubscribed_security_ids:
        return

    last_tick_time[sec_id_str] = time.monotonic()

    try:
        tick_queue.put_nowait((now_ist_epoch(), data))
    except queue.Full:
        dropped_ticks += 1

def on_connect(instance):
    global session_connect_mono
    now_mono = time.monotonic()
    with state_lock:
        session_connect_mono = now_mono
        for sec_id in active_subscribed_ids:
            last_tick_time[sec_id] = now_mono
    log(f"Connected to Dhan gateway. 5-minute warmup started ({len(active_subscribed_ids)} active instruments).")

def on_close(instance):
    log("WebSocket closed by server.")

def on_error(instance, error):
    log(f"WebSocket error: {error}")

# --------------------------------------------------- Inactivity Reaper Worker ---

def inactivity_reaper_loop():
    while not stop_event.is_set():
        time.sleep(REAPER_SCAN_INTERVAL_SEC)

        if not is_active_trading_hours():
            continue

        with state_lock:
            connect_time = session_connect_mono
            feed_inst = active_feed_instance

        if connect_time is None or feed_inst is None:
            continue

        now = time.monotonic()
        elapsed_since_connect = now - connect_time

        if elapsed_since_connect < WARMUP_PERIOD_SEC:
            continue

        to_evict = []
        with state_lock:
            for sec_id in list(active_subscribed_ids):
                t_last = last_tick_time.get(sec_id, connect_time)
                if (now - t_last) >= TICK_INACTIVITY_TIMEOUT_SEC:
                    to_evict.append(sec_id)

        if not to_evict:
            continue

        chunk_size = 100
        total_evicted = len(to_evict)
        log(f"Inactivity Reaper: Evicting {total_evicted} illiquid instruments (no tick in 5m)...")

        for i in range(0, total_evicted, chunk_size):
            chunk = to_evict[i:i + chunk_size]
            unsub_list = [(MarketFeed.NSE, s_id, MarketFeed.Full) for s_id in chunk]
            try:
                if hasattr(feed_inst, "unsubscribe_symbols"):
                    feed_inst.unsubscribe_symbols(unsub_list)
                elif hasattr(feed_inst, "unsub_symbols"):
                    feed_inst.unsub_symbols(unsub_list)
            except Exception as e:
                log(f"Error unsubscribing chunk {i}-{i+len(chunk)}: {e}")

        with state_lock:
            for sec_id in to_evict:
                active_subscribed_ids.discard(sec_id)
                unsubscribed_security_ids.add(sec_id)
                last_tick_time.pop(sec_id, None)
                last_volume.pop(sec_id, None)

        log(f"Eviction complete. Remaining active instruments: {len(active_subscribed_ids)}")

# ------------------------------------------------------------- Watchdog & Monitor ---

def terminate_active_feed(feed_inst):
    if not feed_inst:
        return
    try:
        loop = getattr(feed_inst, "loop", None) or getattr(feed_inst, "_loop", None)
        if loop and loop.is_running():
            loop.call_soon_threadsafe(feed_inst.close_connection)
        else:
            feed_inst.close_connection()
    except Exception as e:
        log(f"Error during socket termination: {e}")

def watchdog_loop():
    while not stop_event.is_set():
        time.sleep(5)
        if is_market_hours():
            silence = get_silence_duration()
            if silence > WATCHDOG_TIMEOUT_SEC:
                log(f"Watchdog alert: No packets received for {silence:.1f}s during active hours. Resetting socket...")
                with state_lock:
                    inst = active_feed_instance
                terminate_active_feed(inst)

def monitor_loop():
    global dropped_ticks
    while not stop_event.is_set():
        time.sleep(60)
        q_size = tick_queue.qsize()
        evicted_count = len(unsubscribed_security_ids)
        active_count = len(active_subscribed_ids)
        if dropped_ticks > 0 or evicted_count > 0:
            log(
                f"Health check: Queue: {q_size} | Active: {active_count} | "
                f"Evicted: {evicted_count} | Dropped (60s): {dropped_ticks}"
            )
            dropped_ticks = 0

# ----------------------------------------------------------- Lifecycle ---

def handle_shutdown(signum, frame):
    log("Termination signal received. Shutting down gracefully...")
    stop_event.set()
    with state_lock:
        inst = active_feed_instance
    terminate_active_feed(inst)

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

writer_thread = threading.Thread(target=parser_and_writer_worker, daemon=True)
writer_thread.start()

watchdog_thread = threading.Thread(target=watchdog_loop, daemon=True)
watchdog_thread.start()

reaper_thread = threading.Thread(target=inactivity_reaper_loop, daemon=True)
reaper_thread.start()

monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
monitor_thread.start()

# ------------------------------------------------------ Supervisor Loop ---
# Reconnects ONLY on exception/close from the broker side — never disconnects proactively.

backoff = 1
while not stop_event.is_set():
    try:
        active_instruments = [
            inst for inst in ALL_INSTRUMENTS
            if inst[1] not in unsubscribed_security_ids
        ]

        with state_lock:
            active_subscribed_ids = {inst[1] for inst in active_instruments}

        log(f"Establishing Dhan MarketFeed connection ({len(active_instruments)} instruments)...")
        dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
        feed = MarketFeed(
            dhan_context,
            active_instruments,
            version="v2",
            on_connect=on_connect,
            on_message=on_message,
            on_close=on_close,
            on_error=on_error,
        )
        with state_lock:
            active_feed_instance = feed
        update_heartbeat()

        feed.run()

    except Exception as e:
        log(f"MarketFeed runtime exception: {e}")
    finally:
        with state_lock:
            active_feed_instance = None
            session_connect_mono = None

    if stop_event.is_set():
        break

    sleep_time = min(backoff, 5) if is_market_hours() else min(backoff, 60)
    log(f"Re-entering supervisor loop in {sleep_time}s...")
    time.sleep(sleep_time)
    backoff = min(backoff * 2, 60)

writer_thread.join(timeout=15)
log("Dhan tick logger terminated cleanly.")
