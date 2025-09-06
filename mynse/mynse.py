!pip install --quiet pandas matplotlib requests
%%writefile mynse.py
import requests
import pandas as pd
import time

BASE_URL = "https://www.nseindia.com"

# --- Global session ---
session = requests.Session()
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": f"{BASE_URL}/market-data/live-equity-market",
    "Origin": BASE_URL,
}

# --- Helper to refresh cookies ---
def refresh_cookies(referer=None):
    try:
        session.get(BASE_URL, headers=HEADERS, timeout=10)
        if referer:
            session.get(referer, headers=HEADERS, timeout=10)
        time.sleep(0.5)
    except Exception as e:
        print(f"❌ Cookie refresh failed: {e}")

# --- Core fetcher ---
def mynsefetch(url, retries=5, referer=None):
    for attempt in range(retries):
        try:
            refresh_cookies(referer)
            resp = session.get(url, headers=HEADERS, timeout=10)
            if "application/json" not in resp.headers.get("Content-Type", ""):
                raise RuntimeError(f"Response not JSON (attempt {attempt+1})")
            return resp.json()
        except Exception as e:
            print(f"❌ NSE fetch failed (attempt {attempt+1}): {e}")
            time.sleep(1 + attempt)
    raise RuntimeError(f"NSE fetch failed after {retries} attempts: {url}")

# --- Futures & Options Data (like nsepython) ---
def nse_fno(symbol="NIFTY"):
    """
    Returns dict like nsepython:
    {'stocks': [futures/options data]}
    """
    url = f"{BASE_URL}/api/option-chain-equities?symbol={symbol}"
    data = mynsefetch(url, referer=f"{BASE_URL}/option-chain")
    return {"stocks": data.get("records", {}).get("data", [])}

# --- Clean Index Futures DataFrame ---
def index_futures_snapshot(symbol="NIFTY"):
    data = nse_fno(symbol)
    records = data["stocks"]

    # Filter index futures only
    fut_recs = [
        r for r in records
        if r.get("metadata", {}).get("instrumentType", "").lower().startswith("index futures")
    ]

    rows = []
    for r in fut_recs:
        md = r.get("metadata", {})
        mkt = r.get("marketDeptOrderBook", {})
        ti = mkt.get("tradeInfo", {})
        oi = mkt.get("otherInfo", {})

        expiry = md.get("expiryDate")
        last_price = md.get("lastPrice") or oi.get("lastPrice") or oi.get("ltp")
        volume = ti.get("tradedVolume") or ti.get("totalTradedVolume") or oi.get("totalTradedVolume")
        vwap = ti.get("vmap") or ti.get("vwap")

        rows.append({
            "expiry": expiry,
            "lastPrice": last_price,
            "volume": volume,
            "vwap": vwap
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df["expiry_dt"] = pd.to_datetime(df["expiry"], format="%d-%b-%Y")
        df = df.sort_values("expiry_dt").reset_index(drop=True)
    return df
