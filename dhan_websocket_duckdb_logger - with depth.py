"""
dhan_websocket_duckdb_logger.py (Production Grade - Zero-Drop Architecture)
"""

import csv
import os
import queue
import signal
import sys
import threading
import time
from datetime import datetime, time as dtime, timezone

try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except Exception:
    IST = None

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
FLUSH_INTERVAL_SEC = 1.0
MAX_INSTRUMENTS_PER_CONNECTION = 5000
PRUNE_WARMUP_SEC = 300.0  # 5 minutes liquidity inspection window

os.makedirs(DB_DIR, exist_ok=True)

if not CLIENT_ID or not ACCESS_TOKEN:
    sys.exit("DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN not set in environment.")

def now_ist():
    return datetime.now(IST) if IST else datetime.now()

def log(msg):
    print(f"[{now_ist():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)

def is_market_hours():
    curr = now_ist()
    if curr.weekday() >= 5:
        return False
    t = curr.time()
    return dtime(9, 0) <= t <= dtime(15, 35)

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
    sys.exit("No instruments loaded. Check CSV path and columns.")

if len(ALL_INSTRUMENTS) > MAX_INSTRUMENTS_PER_CONNECTION:
    sys.exit(f"Exceeds per-connection limit ({MAX_INSTRUMENTS_PER_CONNECTION}).")

# ------------------------------------------------ Dynamic Subscription State ---

instruments_lock = threading.Lock()
active_instruments = list(ALL_INSTRUMENTS)
active_sec_ids = {sec_id for _, sec_id, _ in ALL_INSTRUMENTS}
last_tick_per_sec = {}
first_connection_time = None
pruning_completed = False

# ------------------------------------------------------------- Storage Engine ---

DEPTH_COLS = (
    [f"bp{i}" for i in range(1, 6)] +
    [f"bq{i}" for i in range(1, 6)] +
    [f"bo{i}" for i in range(1, 6)] +
    [f"ap{i}" for i in range(1, 6)] +
    [f"aq{i}" for i in range(1, 6)] +
    [f"ao{i}" for i in range(1, 6)]
)

ARROW_FIELDS = [
    ("received_at", pa.int64()),  # Unix epoch microseconds
    ("trading_symbol", pa.string()),
    ("ltp", pa.float64()),
    ("ltq", pa.int32()),
    ("ltt", pa.int64()),          # Unix epoch microseconds
    ("volume", pa.int64()),
] + [(col, pa.float64()) for col in DEPTH_COLS]

ARROW_SCHEMA = pa.schema(ARROW_FIELDS)

depth_sql_cols = ",\n    ".join([f"{col} DOUBLE" for col in DEPTH_COLS])
TABLE_INIT_SQL = f"""
CREATE TABLE IF NOT EXISTS ticks (
    received_at     BIGINT,
    trading_symbol  VARCHAR,
    ltp             DOUBLE,
    ltq             INTEGER,
    ltt             BIGINT,
    volume          BIGINT,
    {depth_sql_cols}
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
            self.con.close()
        path = self._path_for(target_date)
        self.con = duckdb.connect(path)
        self.con.execute(TABLE_INIT_SQL)
        self.current_date = target_date
        log(f"Active database: {path}")

    def write_arrow(self, arrow_table):
        today = now_ist().date()
        if today != self.current_date:
            self._open_for_date(today)
        self.con.register("chunk_view", arrow_table)
        self.con.execute("INSERT INTO ticks SELECT * FROM chunk_view")
        self.con.unregister("chunk_view")

    def close(self):
        if self.con is not None:
            self.con.close()
            self.con = None

# ------------------------------------------------------- Pipeline Workers ---

tick_queue = queue.Queue(maxsize=200000)
stop_event = threading.Event()
last_packet_time = time.time()
active_feed_instance = None

def parse_ltt_epoch_us(ltt_str, recv_epoch_us):
    if not ltt_str:
        return 0
    try:
        parts = ltt_str.split(":")
        h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
    except (ValueError, IndexError, AttributeError):
        return 0

    now_utc = datetime.now(timezone.utc)
    midnight_utc_ts = int(datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=timezone.utc).timestamp())
    ltt_ts = midnight_utc_ts + (h * 3600) + (m * 60) + s
    ltt_us = ltt_ts * 1_000_000

    if ltt_us > (recv_epoch_us + 120_000_000):
        ltt_us -= 86_400_000_000

    return ltt_us

def _row_from_packet(recv_epoch_us, data):
    sec_id_str = str(data["security_id"])
    
    row = {
        "received_at": recv_epoch_us,
        "trading_symbol": SEC_TO_SYMBOL.get(sec_id_str, ""),
        "ltp": float(data["LTP"]),
        "ltq": int(data["LTQ"]),
        "ltt": parse_ltt_epoch_us(data.get("LTT"), recv_epoch_us),
        "volume": int(data["volume"]),
    }

    depth = data.get("depth")
    bids = depth.get("buy", []) if isinstance(depth, dict) else (depth if isinstance(depth, list) else [])
    asks = depth.get("sell", []) if isinstance(depth, dict) else []

    for i in range(5):
        idx = i + 1
        b_entry = bids[i] if i < len(bids) else {}
        a_entry = asks[i] if i < len(asks) else {}

        row[f"bp{idx}"] = float(b_entry.get("price", 0.0))
        row[f"bq{idx}"] = float(b_entry.get("quantity", 0.0))
        row[f"bo{idx}"] = float(b_entry.get("orders", 0.0))

        row[f"ap{idx}"] = float(a_entry.get("price", 0.0))
        row[f"aq{idx}"] = float(a_entry.get("quantity", 0.0))
        row[f"ao{idx}"] = float(a_entry.get("orders", 0.0))

    return row

def _flush_cols(store, cols):
    num_rows = len(cols["received_at"])
    if num_rows == 0:
        return
    try:
        batch = pa.RecordBatch.from_arrays(
            [pa.array(cols[n], type=t) for n, t in zip(ARROW_SCHEMA.names, ARROW_SCHEMA.types)],
            schema=ARROW_SCHEMA,
        )
        store.write_arrow(pa.Table.from_batches([batch]))
    except Exception as e:
        log(f"Batch write failed, dropped {num_rows} rows: {e}")
    finally:
        for k in cols:
            cols[k].clear()

def parser_and_writer_worker():
    store = ResilientTickStore(DB_DIR)
    cols = {name: [] for name in ARROW_SCHEMA.names}
    last_flush = time.time()

    while not stop_event.is_set() or not tick_queue.empty():
        try:
            recv_epoch_us, data = tick_queue.get(timeout=0.2)
            try:
                row = _row_from_packet(recv_epoch_us, data)
                for k, v in row.items():
                    cols[k].append(v)
            except (KeyError, ValueError, TypeError) as e:
                log(f"Skipped malformed packet for {data.get('security_id')}: {e}")
            tick_queue.task_done()
        except queue.Empty:
            pass

        now = time.time()
        num_rows = len(cols["received_at"])
        if num_rows > 0 and (now - last_flush >= FLUSH_INTERVAL_SEC or num_rows >= 10000):
            _flush_cols(store, cols)
            last_flush = now

    _flush_cols(store, cols)
    store.close()

# ------------------------------------------------------------- Callbacks ---

def on_message(instance, data):
    global last_packet_time
    last_packet_time = time.time()
    if not data or data.get("type") != "Full Data":
        return

    sec_id = str(data.get("security_id"))
    last_tick_per_sec[sec_id] = last_packet_time

    # Fast O(1) filter drops illiquid ticks without touching the socket
    with instruments_lock:
        if sec_id not in active_sec_ids:
            return

    try:
        recv_epoch_us = time.time_ns() // 1000
        tick_queue.put_nowait((recv_epoch_us, data))
    except queue.Full:
        log("Warning: Ingestion queue full, dropping tick.")

def on_connect(instance):
    global first_connection_time
    if first_connection_time is None:
        first_connection_time = time.time()
    with instruments_lock:
        count = len(active_instruments)
    log(f"Connected to Dhan gateway. Active subscription count: {count}")

def on_close(instance):
    log("WebSocket connection closed by broker endpoint.")

def on_error(instance, error):
    log(f"WebSocket network error: {error}")

# -------------------------------------------------- Liquidity Pruner ---

def liquidity_pruning_worker():
    """
    Waits 5 minutes after first connect, then unregisters contracts that never ticked.
    Modifies in-flight tables and dispatches unsubscribe frames without dropping the socket.
    """
    global pruning_completed, active_instruments, active_sec_ids
    
    while not stop_event.is_set():
        time.sleep(1)
        if first_connection_time is None or pruning_completed:
            continue
        
        elapsed = time.time() - first_connection_time
        if elapsed >= PRUNE_WARMUP_SEC:
            cutoff = time.time() - PRUNE_WARMUP_SEC
            with instruments_lock:
                illiquid_unsub_tuples = []
                liquid_instruments = []
                liquid_sec_ids = set()

                for inst in active_instruments:
                    seg, sec_id, mode = inst
                    last_time = last_tick_per_sec.get(sec_id, 0)
                    if last_time < cutoff:
                        illiquid_unsub_tuples.append((seg, sec_id, mode))
                    else:
                        liquid_instruments.append(inst)
                        liquid_sec_ids.add(sec_id)

                # Hot-swap the active whitelist
                active_instruments = liquid_instruments
                active_sec_ids = liquid_sec_ids
                pruning_completed = True

            log(f"Pruning complete: Removed {len(illiquid_unsub_tuples)} illiquid instruments (no tick in 5m). Retained {len(liquid_instruments)} active.")

            # Send in-flight unsubscription packets without dropping the connection
            if active_feed_instance and illiquid_unsub_tuples:
                try:
                    if hasattr(active_feed_instance, "unsubscribe_symbols"):
                        chunk_size = 100
                        for i in range(0, len(illiquid_unsub_tuples), chunk_size):
                            chunk = illiquid_unsub_tuples[i : i + chunk_size]
                            active_feed_instance.unsubscribe_symbols(chunk)
                        log("Dispatched unsubscribe frames across active socket.")
                    else:
                        log("unsubscribe_symbols API not exposed by SDK; illiquid ticks are suppressed at ingress.")
                except Exception as e:
                    log(f"Notice: Soft-unsubscription encountered an exception (connection kept open): {e}")
            break

# ------------------------------------------------ Passive Watchdog ---

def watchdog_loop():
    """Observes stream health without dropping the connection."""
    while not stop_event.is_set():
        time.sleep(10)
        if is_market_hours():
            silence_duration = time.time() - last_packet_time
            if silence_duration > 60.0:
                log(f"Watchdog Notice: Zero packets received for {silence_duration:.1f}s during market hours. Monitoring connection state...")

# ----------------------------------------------------------- Lifecycle ---

def handle_shutdown(signum, frame):
    log("Termination signal received. Shutting down gracefully...")
    stop_event.set()
    if active_feed_instance:
        try:
            active_feed_instance.close_connection()
        except Exception:
            pass

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# Background pipeline threads
writer_thread = threading.Thread(target=parser_and_writer_worker, daemon=True)
writer_thread.start()

watchdog_thread = threading.Thread(target=watchdog_loop, daemon=True)
watchdog_thread.start()

pruner_thread = threading.Thread(target=liquidity_pruning_worker, daemon=True)
pruner_thread.start()

# Resilience Loop: ONLY acts if connection is severed externally by broker/network
backoff = 1
while not stop_event.is_set():
    try:
        log("Initializing Dhan Feed session...")
        dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
        
        with instruments_lock:
            current_subscription_list = list(active_instruments)

        feed = MarketFeed(
            dhan_context,
            current_subscription_list,
            version="v2",
            on_connect=on_connect,
            on_message=on_message,
            on_close=on_close,
            on_error=on_error,
        )
        active_feed_instance = feed
        last_packet_time = time.time()

        feed.run()

    except Exception as e:
        log(f"MarketFeed session terminated externally: {e}")
    finally:
        active_feed_instance = None

    if stop_event.is_set():
        break

    log(f"Re-establishing connection in {backoff} seconds...")
    time.sleep(backoff)
    backoff = min(backoff * 2, 30)

writer_thread.join(timeout=15)
log("Dhan tick logger terminated cleanly.")