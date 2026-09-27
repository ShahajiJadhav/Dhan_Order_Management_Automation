import os, asyncio, duckdb, pandas as pd, pytz, threading, json, websocket, struct, requests, time, re
from io import StringIO
from collections import deque
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_FILE = "dhan_tracker.duckdb"
IST = pytz.timezone("Asia/Kolkata")

# --- PARAMETERS ---
WHALE_THRESHOLD_CR = 40.0
VOL_5MIN_THRESHOLD_CR = 40.0
COOLDOWN_SECONDS = 600  # 10 Minutes
CR_UNIT = 10_000_000

# --- STATE ---
tick_buffer = deque()
alert_cooldowns = {}
volume_history = {} # {sid: deque([(ts, vol), ...])}
ID_TO_SYMBOL = {}
SIDS_LIST = []

EXCLUDED_SYMBOLS = {
    "M&M","BEL","JISLJALEQS","ABCAPITAL","HDFCLIFE","NSLNISP","ASIANPAINT","HEROMOTOCO",
    "NATIONALUM","NMDC","SAMMAANCAP","NESTLEIND","IDBI","JIOFIN","GAEL","ITC","FMCGIETF",
    "MON100","PSUBNKBEES","SILVERBEES","ITBEES","ITIETF","NIFTYBEES","CONSUMBEES","ALPHA",
    "AUTOBEES","MASPTOP50","SILVERETF","SILVERIETF","LTF","HNGSNGBEES","SOUTHBANK","HINDALCO",
    "IRB","TECHM","SAIL","POWERGRID","CANB","IREDA","IRCON","BEML","AXISBANK","BANKBEES",
    "BANKIETF","BANKNIFTY1","HDFCBANK","ICICIBANK","KOTAKBANK","KTKBANK","PSUBANK","PSUBANKADD",
    "PVTBANKADD","RBLBANK","UTIBANKETF","YESBANK","ETERNAL","SBIN","NTPC","BHEL","RECLTD",
    "WIPRO","INFY","MSUMI","MOTHERSON","ABFRL","DELHIVERY","RELIANCE","TCS","TATACHEM",
    "TATACOMM","TATACONSUM","TATAELXSI","TATAINVEST","TATAMOTORS","TATAPOWER","TATASTEEL",
    "TATATECH","Zerodha Nifty 1D Rate Liquid ETF","ALLCARGO","HDFCSILVER","SILVER1",
    "SILVERADD","SBISILVER","SILVER","SILVERIETF","SILVERETF"
}

# ============================================================= #
#               INSTRUMENT FETCH & FILTERING                    #
# ============================================================= #

def fetch_and_build_list():
    global ID_TO_SYMBOL, SIDS_LIST
    print("⬇️ Fetching live instrument master and leverage data...")
    
    headers = {
        'access-token': DHAN_ACCESS_TOKEN, 
        'client-id': DHAN_CLIENT_ID, 
        'Content-Type': 'application/json', 
        'Accept': 'application/json'
    }
    
    # 1. Get Instrument Master
    resp = requests.get("https://api.dhan.co/v2/instrument/NSE_EQ", headers={'access-token': DHAN_ACCESS_TOKEN})
    inst_df = pd.read_csv(StringIO(resp.text))
    inst_df = inst_df[(inst_df["EXCH_ID"] == "NSE") & (inst_df["SEGMENT"] == "E") & (inst_df["INSTRUMENT_TYPE"] == "ES")]
    inst_df = inst_df[["SECURITY_ID", "UNDERLYING_SYMBOL"]].rename(columns={"UNDERLYING_SYMBOL": "Symbol"})
    inst_df["Symbol"] = inst_df["Symbol"].str.upper().str.strip()

    # 2. Get Leverage Sheet
    sheet_url = "https://docs.google.com/spreadsheets/d/1zqhM3geRNW_ZzEx62y0W5U2ZlaXxG-NDn0V8sJk5TQ4/gviz/tq?tqx=out:csv&gid=1663719548"
    lev_df = pd.read_csv(sheet_url)
    symbol_col = lev_df.columns[list(lev_df.columns).index("Sr.") + 1]
    lev_df = lev_df.rename(columns={symbol_col: "Symbol"})
    lev_df["Symbol"] = lev_df["Symbol"].astype(str).str.upper().str.strip()
    lev_df["MIS"] = pd.to_numeric(lev_df["MIS (Intraday)"].astype(str).str.replace("x","",regex=False).str.replace("X","",regex=False), errors="coerce")
    
    # 3. Filter 5x and Exclusions
    mis_df = lev_df[lev_df["MIS"] >= 5][["Symbol", "MIS"]].copy()
    exclude_pattern = re.compile(r"(BEES|ETF|CASE)", re.IGNORECASE)
    mis_df = mis_df[~mis_df["Symbol"].str.contains(exclude_pattern, na=False)]
    mis_df = mis_df[~mis_df["Symbol"].isin(EXCLUDED_SYMBOLS)]
    
    final_df = mis_df.merge(inst_df, on="Symbol", how="inner")
    potential_sids = final_df["SECURITY_ID"].tolist()
    sid_to_symbol_map = dict(zip(final_df['SECURITY_ID'], final_df['Symbol']))

    # 4. Fetch LTP and Filter (5 to 1500)
    print(f"🔍 Checking prices for {len(potential_sids)} candidates...")
    filtered_data = []
    quote_url = "https://api.dhan.co/v2/marketfeed/ltp"
    
    for i in range(0, len(potential_sids), 1000):
        chunk = potential_sids[i:i+1000]
        try:
            q_resp = requests.post(quote_url, headers=headers, json={"NSE_EQ": chunk}, timeout=10)
            if q_resp.status_code == 200:
                market_data = q_resp.json().get('data', {}).get('NSE_EQ', {})
                for sid_key, details in market_data.items():
                    ltp = details.get('last_price', 0)
                    if 5 <= ltp <= 1500:
                        sid_int = int(sid_key)
                        filtered_data.append({"SECURITY_ID": sid_int, "Symbol": sid_to_symbol_map.get(sid_int)})
            time.sleep(1.1) 
        except Exception as e:
            print(f"⚠️ Quote Error: {e}")

    if filtered_data:
        valid_df = pd.DataFrame(filtered_data)
        ID_TO_SYMBOL = pd.Series(valid_df.Symbol.values, index=valid_df.SECURITY_ID).to_dict()
        SIDS_LIST = [str(x) for x in valid_df["SECURITY_ID"].tolist()]
        print(f"✅ Setup Complete: Monitoring {len(SIDS_LIST)} eligible stocks.")
    else:
        print("❌ No eligible stocks found.")

# ============================================================= #
#                ANALYTICS & ALERTING LOGIC                     #
# ============================================================= #

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try: requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=5)
    except: pass

def process_tick_analytics(sec_id, ltp, cum_vol, bids, asks):
    now = time.time()
    sym = ID_TO_SYMBOL.get(sec_id, f"ID:{sec_id}")

    # 1. Whale Order Logic (Price * Qty)
    max_b_amt = max([p * q for p, q in bids])
    max_a_amt = max([p * q for p, q in asks])
    
    peak_val = max(max_b_amt, max_a_amt)
    peak_cr = peak_val / CR_UNIT

    if peak_cr >= WHALE_THRESHOLD_CR:
        alert_key = f"WHALE_{sec_id}"
        if (now - alert_cooldowns.get(alert_key, 0)) > COOLDOWN_SECONDS:
            alert_cooldowns[alert_key] = now
            side = "BUY (Bid Side)" if max_b_amt >= max_a_amt else "SELL (Ask Side)"
            msg = (f"🐳 *WHALE ORDER DETECTED*\n\n"
                   f"*Stock:* {sym}\n*Side:* {side}\n"
                   f"*Amount:* ₹{peak_cr:.2f} Cr\n*LTP:* {ltp}")
            threading.Thread(target=send_telegram, args=(msg,), daemon=True).start()

    # 2. 5-Min Volume Spike Logic
    if sec_id not in volume_history:
        volume_history[sec_id] = deque()
    volume_history[sec_id].append((now, cum_vol))

    while len(volume_history[sec_id]) > 1 and (now - volume_history[sec_id][0][0] > 300):
        volume_history[sec_id].popleft()

    if len(volume_history[sec_id]) > 1:
        ref_vol = volume_history[sec_id][0][1]
        delta_cr = ((cum_vol - ref_vol) * ltp) / CR_UNIT
        if delta_cr >= VOL_5MIN_THRESHOLD_CR:
            alert_key = f"VOL_{sec_id}"
            if (now - alert_cooldowns.get(alert_key, 0)) > COOLDOWN_SECONDS:
                alert_cooldowns[alert_key] = now
                msg = (f"🔥 *5-MIN VOLUME BREAKOUT*\n\n"
                       f"*Stock:* {sym}\n*Value:* ₹{delta_cr:.2f} Cr\n"
                       f"*LTP:* {ltp}\n*Window:* Rolling 5-Min")
                threading.Thread(target=send_telegram, args=(msg,), daemon=True).start()

    return int(max_b_amt), int(max_a_amt)

# ============================================================= #
#                      CORE PIPELINE                            #
# ============================================================= #

def setup_db():
    con = duckdb.connect(DB_FILE)
    con.execute("CREATE SEQUENCE IF NOT EXISTS tick_id_seq START 1")
    con.execute("""
    CREATE TABLE IF NOT EXISTS ticks (
        id INTEGER PRIMARY KEY DEFAULT nextval('tick_id_seq'),
        event_time TIMESTAMP, 
        security_id INTEGER, 
        ltp DOUBLE, 
        volume BIGINT,
        bid1_p DOUBLE, bid1_q INTEGER, 
        bid2_p DOUBLE, bid2_q INTEGER, 
        bid3_p DOUBLE, bid3_q INTEGER, 
        bid4_p DOUBLE, bid4_q INTEGER, 
        bid5_p DOUBLE, bid5_q INTEGER,
        ask1_p DOUBLE, ask1_q INTEGER, 
        ask2_p DOUBLE, ask2_q INTEGER, 
        ask3_p DOUBLE, ask3_q INTEGER, 
        ask4_p DOUBLE, ask4_q INTEGER, 
        ask5_p DOUBLE, ask5_q INTEGER,
        max_bid_amount BIGINT, 
        max_ask_amount BIGINT
    )
    """)
    return con

def parse_binary(message):
    try:
        if message[0] == 8 and len(message) >= 162:
            sec_id = struct.unpack('<I', message[4:8])[0]
            if str(sec_id) not in SIDS_LIST: return None # Strictly monitor eligible only
            
            ltp = round(struct.unpack('<f', message[8:12])[0], 2)
            cum_vol = struct.unpack('<I', message[24:28])[0]

            bids, asks = [], []
            for i in range(5):
                off = 62 + (i * 20)
                b_q, a_q = struct.unpack('<I', message[off:off+4])[0], struct.unpack('<I', message[off+4:off+8])[0]
                b_p, a_p = round(struct.unpack('<f', message[off+12:off+16])[0], 2), round(struct.unpack('<f', message[off+16:off+20])[0], 2)
                bids.append((b_p, b_q)); asks.append((a_p, a_q))

            max_b, max_a = process_tick_analytics(sec_id, ltp, cum_vol, bids, asks)

            depth_flat = []
            for bp, bq in bids: depth_flat.extend([bp, bq])
            for ap, aq in asks: depth_flat.extend([ap, aq])
            return (datetime.now(IST), sec_id, ltp, int(cum_vol), *depth_flat, max_b, max_a)
    except: return None

async def db_writer(con):
    # Performance tuning for DuckDB
    con.execute("SET wal_autocheckpoint='1GB';") 
    
    # We provide 26 '?' because we are inserting 26 fields 
    # (event_time, security_id, ltp, volume, 10 bid fields, 10 ask fields, max_bid, max_ask)
    # The 'id' column is handled by the sequence automatically.
    
    placeholders = ",".join(["?"] * 26)
    query = f"INSERT INTO ticks (event_time, security_id, ltp, volume, bid1_p, bid1_q, bid2_p, bid2_q, bid3_p, bid3_q, bid4_p, bid4_q, bid5_p, bid5_q, ask1_p, ask1_q, ask2_p, ask2_q, ask3_p, ask3_q, ask4_p, ask4_q, ask5_p, ask5_q, max_bid_amount, max_ask_amount) VALUES ({placeholders})"
    
    while True:
        if tick_buffer:
            batch = []
            # Pull up to 5000 records at a time to keep the loop fast
            while tick_buffer and len(batch) < 5000:
                batch.append(tick_buffer.popleft())
            
            if batch:
                try:
                    con.executemany(query, batch)
                except Exception as e:
                    print(f"❌ DB Write Error: {e}")
                    # If there's a structure mismatch, print one row to debug
                    if len(batch) > 0:
                        print(f"Sample row length: {len(batch[0])}")
        
        # Give the CPU a tiny break
        await asyncio.sleep(0.5)


def on_message(ws, message):
    if isinstance(message, bytes):
        row = parse_binary(message)
        if row: tick_buffer.append(row)

def run_ws():
    url = f"wss://api-feed.dhan.co?version=2&token={DHAN_ACCESS_TOKEN}&clientId={DHAN_CLIENT_ID}&authType=2"
    websocket.WebSocketApp(url, on_message=on_message, on_open=lambda ws: [
        ws.send(json.dumps({"RequestCode": 21, "InstrumentCount": len(chunk), "InstrumentList": [{"ExchangeSegment": "NSE_EQ", "SecurityId": s} for s in chunk]})) 
        for chunk in [SIDS_LIST[i:i+100] for i in range(0, len(SIDS_LIST), 100)]
    ]).run_forever()

if __name__ == "__main__":
    fetch_and_build_list()
    if SIDS_LIST:
        db_con = setup_db()
        threading.Thread(target=run_ws, daemon=True).start()
        asyncio.run(db_writer(db_con))