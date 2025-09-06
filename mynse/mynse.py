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
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

# --- Refresh cookies ---
def refresh_cookies():
    try:
        session.get(BASE_URL, headers=HEADERS, timeout=10)
        time.sleep(0.5)
    except Exception as e:
        print(f"❌ Cookie refresh failed: {e}")

# --- Fetch JSON from NSE ---
def mynsefetch(url, retries=5):
    for attempt in range(retries):
        try:
            refresh_cookies()
            resp = session.get(url, headers=HEADERS, timeout=10)
            if "application/json" not in resp.headers.get("Content-Type", ""):
                raise RuntimeError(f"Response not JSON (attempt {attempt+1})")
            return resp.json()
        except Exception as e:
            print(f"❌ NSE fetch failed (attempt {attempt+1}): {e}")
            time.sleep(1 + attempt)
    raise RuntimeError(f"NSE fetch failed after {retries} attempts: {url}")

# --- F&O Futures parser ---
def nse_fno(symbol="NIFTY"):
    url = f"{BASE_URL}/api/fno-derivatives?symbol={symbol}"
    data = mynsefetch(url)

    records = data.get("stocks", [])

    fut_rows = []
    for r in records:
        md = r.get("metadata", {})
        mkt = r.get("marketDeptOrderBook", {})
        ti  = mkt.get("tradeInfo", {})
        oi  = mkt.get("otherInfo", {})

        if not md.get("instrumentType", "").lower().startswith("index futures"):
            continue  # skip options

        expiry = md.get("expiryDate")
        last_price = md.get("lastPrice") or oi.get("lastPrice") or oi.get("ltp")
        volume = ti.get("tradedVolume") or ti.get("totalTradedVolume") or oi.get("totalTradedVolume")
        vwap = ti.get("vmap") or ti.get("vwap")

        fut_rows.append({
            "expiry": expiry,
            "lastPrice": last_price,
            "volume": volume,
            "vwap": vwap
        })

    df = pd.DataFrame(fut_rows)
    if not df.empty:
        df["expiry_dt"] = pd.to_datetime(df["expiry"], format="%d-%b-%Y")
        df = df.sort_values("expiry_dt").reset_index(drop=True)
    return df
