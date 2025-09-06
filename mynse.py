# mynse.py
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
}

# --- Internal helper to refresh cookies ---
def refresh_cookies(referer):
    """
    Visit homepage + referer page to set cookies
    """
    try:
        session.get(BASE_URL, headers={**HEADERS, "Referer": BASE_URL}, timeout=10)
        session.get(referer, headers={**HEADERS, "Referer": referer}, timeout=10)
        time.sleep(0.5)
    except Exception as e:
        print(f"❌ Cookie refresh failed: {e}")

# --- Core fetcher ---
def mynsefetch(url, referer=None, retries=5):
    if referer is None:
        referer = BASE_URL

    for attempt in range(retries):
        try:
            refresh_cookies(referer)
            resp = session.get(url, headers={**HEADERS, "Referer": referer}, timeout=10)
            content_type = resp.headers.get("Content-Type", "")
            if "application/json" not in content_type:
                raise RuntimeError(f"Response not JSON (attempt {attempt+1})")
            return resp.json()
        except Exception as e:
            print(f"❌ NSE fetch failed (attempt {attempt+1}): {e}")
            time.sleep(1 + attempt)
    raise RuntimeError(f"NSE fetch failed after {retries} attempts for URL: {url}")

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
    url = f"{BASE_URL}/api/option-chain-equities?symbol={symbol}"
    data = mynsefetch(url, referer=f"{BASE_URL}/market-data/live-equity-market")
    records = data.get("records", {}).get("data", [])
    return pd.json_normalize(records, sep="_") if records else pd.DataFrame()

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

