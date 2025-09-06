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

# --- Option Chain ---
def nse_optionchain_scrapper(symbol="NIFTY"):
    url = f"{BASE_URL}/api/option-chain-indices?symbol={symbol}"
    data = mynsefetch(url, referer=f"{BASE_URL}/option-chain")
    records = data.get("records", {}).get("data", [])
    df = pd.json_normalize(records, sep="_") if records else pd.DataFrame()

    # Broadcast underlying value
    underlying = data.get("records", {}).get("underlyingValue", None)
    if not df.empty:
        df['_underlying'] = underlying

    # Keep expiry dates separate
    expiry_dates = data.get("records", {}).get("expiryDates", [])
    df.attrs['_expiryDates'] = expiry_dates  # store as DataFrame attribute

    return df

# --- Index Data ---
def nse_index():
    url = f"{BASE_URL}/api/allIndices"
    data = mynsefetch(url, referer=f"{BASE_URL}/market-data/live-equity-market")
    return pd.DataFrame(data.get("data", []))

# --- Futures Data (F&O) ---
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

# --- PCR & OI Analysis ---
def calculate_pcr(df):
    if df.empty or 'CE_openInterest' not in df.columns or 'PE_openInterest' not in df.columns:
        return 0, 0
    total_ce_oi = df['CE_openInterest'].sum()
    total_pe_oi = df['PE_openInterest'].sum()
    pcr = round(total_pe_oi / total_ce_oi, 2) if total_ce_oi != 0 else 0
    oi_diff = total_pe_oi - total_ce_oi
    return pcr, oi_diff

def max_oi_strikes(df):
    if df.empty:
        return None, None, None, None
    max_call = df.loc[df['CE_openInterest'].idxmax()]['strikePrice'] if 'CE_openInterest' in df.columns else None
    max_put = df.loc[df['PE_openInterest'].idxmax()]['strikePrice'] if 'PE_openInterest' in df.columns else None
    max_call_chg = df.loc[df['CE_changeinOpenInterest'].idxmax()]['strikePrice'] if 'CE_changeinOpenInterest' in df.columns else None
    max_put_chg = df.loc[df['PE_changeinOpenInterest'].idxmax()]['strikePrice'] if 'PE_changeinOpenInterest' in df.columns else None
    return max_call, max_put, max_call_chg, max_put_chg

def nearest_expiry_df(df):
    if df.empty or '_expiryDates' not in df.attrs:
        return df
    nearest_expiry = df.attrs['_expiryDates'][0]
    return df[df['expiryDate'] == nearest_expiry]
    
def index_futures_snapshot(symbol="NIFTY"):
    """
    Returns a DataFrame of Index Futures with nearest, next, and far expiries,
    columns: expiry, lastPrice, volume, vwap
    """
    df = nse_fno(symbol)
    if df.empty:
        return df

    # Sort by expiry (already done in nse_fno)
    # Take nearest 3 expiries
    snapshot = df.head(3).copy()

    # Keep only useful columns
    snapshot = snapshot[["expiry", "lastPrice", "volume", "vwap"]]
    snapshot.reset_index(drop=True, inplace=True)
    return snapshot
