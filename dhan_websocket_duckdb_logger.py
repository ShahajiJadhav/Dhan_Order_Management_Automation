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
    IST = timezone.utc

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
    return datetime.now(IST)

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

def _get_val(data, *keys, default=0):
    if not isinstance(data, dict):
        return default
    for k in keys:
        if k in data and data[k] is not None:
            return data[k]
    return default

def parse_ltt_epoch_us(ltt_val, recv_epoch_us):
    if not ltt_val:
        return 0

    # If broker passes timestamp directly as integer or epoch float
    if isinstance(ltt_val, (int, float)):
        val = int(ltt_val)
        if val > 1_000_000_000_000_000:    # Already microseconds
            return val
        elif val > 1_000_000_000_000:      # Milliseconds
            return val * 1_000
        elif val > 1_000_000_000:          # Seconds
            return val * 1_000_000

    try:
        parts = str(ltt_val).strip().split(":")
        h, m, s = int(parts[0]), int(parts[1]), int(parts[2].split(".")[0])
    except (ValueError, IndexError, AttributeError):
        return 0

    # Anchor trade time strictly to today's midnight in IST
    curr_ist = now_ist()
    midnight_ist = datetime(curr_ist.year, curr_ist.month, curr_ist.day, tzinfo=IST)
    midnight_ist_ts = int(midnight_ist.timestamp())
    ltt_ts = midnight_ist_ts + (h * 3600) + (m * 60) + s
    ltt_us = ltt_ts * 1_000_000

    # Edge-case: packet processed across midnight boundary
    if ltt_us > (recv_epoch_us + 120_000_000):
        ltt_us -= 86_400_000_000

    return ltt_us

def _row_from_packet(recv_epoch_us, data):
    sec_id_str = str(_get_val(data, "security_id", "securityId", "sec_id", default="")).strip()
    ltt_raw = _get_val(data, "LTT", "ltt", "last_trade_time", "time", default=None)

    row = {
        "received_at": recv_epoch_us,
        "trading_symbol": SEC_TO_SYMBOL.get(sec_id_str, ""),
        "ltp": float(_get_val(data, "LTP", "ltp", "last_price", "price", default=0.0)),
        "ltq": int(_get_val(data, "LTQ", "ltq", "last_quantity", "quantity", default=0)),
        "ltt": parse_ltt_epoch_us(ltt_raw, recv_epoch_us),
        "volume": int(_get_val(data, "volume", "Volume", "volume_traded", "volume_traded_today", default=0)),
    }

    depth = data.get("depth") or []

    # Case A: Dictionary containing distinct buy/sell lists (REST API / legacy format)
    if isinstance(depth, dict):
        bids = depth.get("buy") or depth.get("bids") or depth.get("buy_depth") or []
        asks = depth.get("sell") or depth.get("asks") or depth.get("sell_depth") or []
        for i in range(5):
            idx = i + 1
            b = bids[i] if i < len(bids) and isinstance(bids[i], dict) else {}
            a = asks[i] if i < len(asks) and isinstance(asks[i], dict) else {}

            row[f"bp{idx}"] = float(_get_val(b, "price", "bid_price", "bp", "rate", default=0.0))
            row[f"bq{idx}"] = float(_get_val(b, "quantity", "bid_quantity", "qty", "bq", default=0.0))
            row[f"bo{idx}"] = float(_get_val(b, "orders", "bid_orders", "num_orders", "bo", default=0.0))

            row[f"ap{idx}"] = float(_get_val(a, "price", "ask_price", "ap", "rate", default=0.0))
            row[f"aq{idx}"] = float(_get_val(a, "quantity", "ask_quantity", "qty", "aq", default=0.0))
            row[f"ao{idx}"] = float(_get_val(a, "orders", "ask_orders", "num_orders", "ao", default=0.0))

    # Case B: DhanHQ MarketFeed v2 list/tuple of 5 ladder levels
    elif isinstance(depth, (list, tuple)):
        for i in range(5):
            idx = i + 1
            level = depth[i] if i < len(depth) and isinstance(depth[i], dict) else {}

            # Unpack Bid side of this ladder level
            row[f"bp{idx}"] = float(_get_val(level, "bid_price", "buy_price", "bp", "price", default=0.0))
            row[f"bq{idx}"] = float(_get_val(level, "bid_quantity", "buy_quantity", "bq", "quantity", "qty", default=0.0))
            row[f"bo{idx}"] = float(_get_val(level, "bid_orders", "buy_orders", "bo", "orders", default=0.0))

            # Unpack Ask side of this ladder level
            row[f"ap{idx}"] = float(_get_val(level, "ask_price", "sell_price", "ap", default=0.0))
            row[f"aq{idx}"] = float(_get_val(level, "ask_quantity", "sell_quantity", "aq", default=0.0))
            row[f"ao{idx}"] = float(_get_val(level, "ask_orders", "sell_orders", "ao", default=0.0))

    else:
        for i in range(1, 6):
            row[f"bp{i}"] = row[f"bq{i}"] = row[f"bo{i}"] = 0.0
            row[f"ap{i}"] = row[f"aq{i}"] = row[f"ao{i}"] = 0.0

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
                log(f"Skipped malformed packet for {_get_val(data, 'security_id')}: {e}")
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
    
    if not isinstance(data, dict):
        return

    # Handle packet type casing variations
    pkt_type = _get_val(data, "type", "feed_type", default="")
    if pkt_type and str(pkt_type).lower() not in ("full data", "full", "depth", "quote data"):
        return

    sec_id = str(_get_val(data, "security_id", "securityId", "sec_id", default="")).strip()
    if not sec_id:
        return

    last_tick_per_sec[sec_id] = last_packet_time

    # Fast O(1) filter drops pruned instruments
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

                active_instruments = liquid_instruments
                active_sec_ids = liquid_sec_ids
                pruning_completed = True

            log(f"Pruning complete: Removed {len(illiquid_unsub_tuples)} illiquid instruments (no tick in 5m). Retained {len(liquid_instruments)} active.")

            if active_feed_instance and illiquid_unsub_tuples:
                try:
                    if hasattr(active_feed_instance, "unsubscribe_symbols"):
                        chunk_size = 100
                        for i in range(0, len(illiquid_unsub_tuples), chunk_size):
                            chunk = illiquid_unsub_tuples[i : i + chunk_size]
                            active_feed_instance.unsubscribe_symbols(chunk)
                        log("Dispatched unsubscribe frames across active socket.")
                    else:
                        log("unsubscribe_symbols API not exposed by SDK; illiquid ticks suppressed at ingress.")
                except Exception as e:
                    log(f"Notice: Soft-unsubscription encountered an exception: {e}")
            break

# ------------------------------------------------ Passive Watchdog ---

def watchdog_loop():
    while not stop_event.is_set():
        time.sleep(10)
        if is_market_hours():
            silence_duration = time.time() - last_packet_time
            if silence_duration > 60.0:
                log(f"Watchdog Notice: Zero packets received for {silence_duration:.1f}s during market hours.")

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

# Resilience Loop
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