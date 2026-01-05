import os
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple, List
import pytz
import requests
from dotenv import load_dotenv

# --------- config / env ----------
load_dotenv()
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "")
DHAN_INTRADAY_URL = os.getenv("DHAN_INTRADAY_URL", "https://api.dhan.co/v2/charts/intraday")
KOLKATA = pytz.timezone("Asia/Kolkata")

if not DHAN_ACCESS_TOKEN:
    # optionally raise here; left as a gentle reminder
    # raise RuntimeError("DHAN_ACCESS_TOKEN not found in .env")
    pass

# --------- helpers ----------
def now_kolkata() -> datetime:
    """Return current time as tz-aware datetime in Asia/Kolkata."""
    return datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(KOLKATA)

def previous_candle_range_from_dt(now_dt: Optional[datetime] = None, interval_minutes: int = 5) -> Tuple[str, str]:
    """
    Compute previous completed candle range (fromDate, toDate) formatted as 'YYYY-MM-DD HH:MM:SS'.
    Example: if now_dt is 10:02 (IST) and interval_minutes=5 -> returns 09:55:00 .. 09:59:59
    """
    if now_dt is None:
        now = now_kolkata()
    else:
        now = now_dt.astimezone(KOLKATA) if now_dt.tzinfo else KOLKATA.localize(now_dt)

    total_minutes = now.hour * 60 + now.minute
    floored_total = (total_minutes // interval_minutes) * interval_minutes
    floor_hour = floored_total // 60
    floor_minute = floored_total % 60
    floor = now.replace(hour=floor_hour, minute=floor_minute, second=0, microsecond=0)

    start = floor - timedelta(minutes=interval_minutes)
    end = floor - timedelta(seconds=1)

    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")

def _zip_array_response(d: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
    """
    Convert a 'data' object with parallel arrays into a list of candle dicts.
    Expects keys like 'open','high','low','close','volume','timestamp' (timestamp in seconds or ms).
    """
    opens = d.get("open", [])
    highs = d.get("high", [])
    lows = d.get("low", [])
    closes = d.get("close", [])
    volumes = d.get("volume", [])
    timestamps = d.get("timestamp", [])

    n = max(len(opens), len(highs), len(lows), len(closes), len(volumes), len(timestamps))
    candles = []
    for i in range(n):
        ts = timestamps[i] if i < len(timestamps) else None
        # detect seconds vs ms
        dt = None
        if ts is not None:
            try:
                tnum = float(ts)
                if tnum > 1e12:  # ms
                    dt = datetime.fromtimestamp(tnum / 1000.0, tz=pytz.utc).astimezone(KOLKATA)
                else:              # seconds
                    dt = datetime.fromtimestamp(tnum, tz=pytz.utc).astimezone(KOLKATA)
            except Exception:
                dt = None

        def safe_get(arr, idx):
            try:
                return float(arr[idx]) if idx < len(arr) and arr[idx] is not None else None
            except Exception:
                return None

        candle = {
            "datetime": dt,
            "open": safe_get(opens, i),
            "high": safe_get(highs, i),
            "low": safe_get(lows, i),
            "close": safe_get(closes, i),
            "volume": safe_get(volumes, i),
            "raw_timestamp": ts,
        }
        candles.append(candle)
    # filter out empty ones (no datetime and no price)
    return [c for c in candles if c.get("datetime") is not None]

def _parse_response_to_candles(resp_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Accepts various shapes and returns list of candles. Handles:
      - data as dict of arrays (your sample)
      - data as list-of-lists or list-of-dicts (existing shapes)
    """
    if not isinstance(resp_json, dict):
        return []

    data = resp_json.get("data")
    # case: data is dict of arrays -> zip them
    if isinstance(data, dict):
        # if contains parallel arrays
        array_keys = {"open", "high", "low", "close", "volume", "timestamp"}
        if any(k in data for k in array_keys):
            return _zip_array_response(data)
        # else try nested data arrays
        for key in ("data", "candles", "ohlc", "rows", "series"):
            candidate = data.get(key)
            if isinstance(candidate, list):
                return _parse_list_candles(candidate)
    # case: top-level data is list
    if isinstance(data, list):
        return _parse_list_candles(data)
    # fallback top-level lists/keys
    for key in ("candles","ohlc","rows","series"):
        candidate = resp_json.get(key)
        if isinstance(candidate, list):
            return _parse_list_candles(candidate)
    return []

def _parse_list_candles(lst: List[Any]) -> List[Dict[str, Any]]:
    """
    Parse list-of-lists or list-of-dicts candle shapes into canonical candle dicts.
    """
    out = []
    for item in lst:
        if isinstance(item, list):
            # common order: [timestamp, open, high, low, close, volume, ...]
            if not item:
                continue
            ts = item[0]
            dt = None
            try:
                tnum = float(ts)
                if tnum > 1e12:
                    dt = datetime.fromtimestamp(tnum / 1000.0, tz=pytz.utc).astimezone(KOLKATA)
                else:
                    dt = datetime.fromtimestamp(tnum, tz=pytz.utc).astimezone(KOLKATA)
            except Exception:
                # maybe ISO string at item[0]
                try:
                    dt = datetime.fromisoformat(str(ts))
                    if dt.tzinfo is None:
                        dt = KOLKATA.localize(dt)
                    else:
                        dt = dt.astimezone(KOLKATA)
                except Exception:
                    dt = None
            out.append({
                "datetime": dt,
                "open": float(item[1]) if len(item) > 1 and item[1] is not None else None,
                "high": float(item[2]) if len(item) > 2 and item[2] is not None else None,
                "low": float(item[3]) if len(item) > 3 and item[3] is not None else None,
                "close": float(item[4]) if len(item) > 4 and item[4] is not None else None,
                "volume": float(item[5]) if len(item) > 5 and item[5] is not None else None,
                "raw": item,
            })
        elif isinstance(item, dict):
            # dict-style candle
            dt = None
            for k in ("datetime","date","time","timestamp","dt"):
                if k in item:
                    v = item[k]
                    try:
                        if isinstance(v,(int,float)):
                            if v > 1e12:
                                dt = datetime.fromtimestamp(v/1000.0, tz=pytz.utc).astimezone(KOLKATA)
                            else:
                                dt = datetime.fromtimestamp(v, tz=pytz.utc).astimezone(KOLKATA)
                        else:
                            dt = datetime.fromisoformat(str(v))
                            if dt.tzinfo is None:
                                dt = KOLKATA.localize(dt)
                            else:
                                dt = dt.astimezone(KOLKATA)
                    except Exception:
                        dt = None
                    break
            out.append({
                "datetime": dt,
                "open": float(item.get("open") or item.get("o")) if (item.get("open") or item.get("o")) is not None else None,
                "high": float(item.get("high") or item.get("h")) if (item.get("high") or item.get("h")) is not None else None,
                "low": float(item.get("low") or item.get("l")) if (item.get("low") or item.get("l")) is not None else None,
                "close": float(item.get("close") or item.get("c")) if (item.get("close") or item.get("c")) is not None else None,
                "volume": float(item.get("volume") or item.get("v")) if (item.get("volume") or item.get("v")) is not None else None,
                "raw": item,
            })
    # filter invalid datetimes
    return [c for c in out if c.get("datetime") is not None]


# --------- main function to call ----------
def get_previous_candle_now(
    security_id: str,
    interval_minutes: int = 5,
    exchange_segment: str = "NSE_EQ",
    instrument: str = "EQUITY",
    access_token: Optional[str] = None,
    retries: int = 2,
    timeout: float = 10.0
) -> Optional[Dict[str, Any]]:
    """
    Compute prev interval using current Kolkata time, call Dhan intraday, parse array-shaped responses,
    and return the last candle (dict) or None if not present.
    """
    token = access_token or DHAN_ACCESS_TOKEN
    if not token:
        raise RuntimeError("DHAN_ACCESS_TOKEN missing. Add to .env or pass access_token.")

    now = now_kolkata()
    from_date, to_date = previous_candle_range_from_dt(now, interval_minutes)

    payload = {
        "securityId": str(security_id),
        "exchangeSegment": exchange_segment,
        "instrument": instrument,
        "interval": str(int(interval_minutes)),
        "oi": False,
        "fromDate": from_date,
        "toDate": to_date,
    }
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "access-token": token,
    }

    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(DHAN_INTRADAY_URL, json=payload, headers=headers, timeout=timeout)
            r.raise_for_status()
            j = r.json()
            # parse into candle list
            candles = _parse_response_to_candles(j)
            if not candles:
                return None
            # sort by datetime and return last (most recent)
            candles.sort(key=lambda x: x["datetime"])
            last = candles[-1]
            # convert datetime to ISO string for easy transport (still tz-aware)
            last_out = last.copy()
            last_out["datetime"] = last_out["datetime"].isoformat()
            return last_out
        except Exception as e:
            last_exc = e
            time.sleep(0.2 * attempt)
            continue

    raise RuntimeError("Failed to fetch previous candle") from last_exc


# --------- quick demo ----------
if __name__ == "__main__":
    # example: use current time to compute previous 5-minute candle for security 1333
    try:
        candle = get_previous_candle_now("6066", interval_minutes=5)
        if candle:
            print("prev candle:", candle)
        else:
            print("no candle returned for prev interval (market closed or no data)")
    except Exception as exc:
        print("error:", exc)
