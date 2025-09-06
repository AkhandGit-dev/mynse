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

# --- Futures & Options Data (F&O) - Updated to match nsepython structure ---
def nse_fno(symbol="NIFTY"):
    """
    Fetch F&O data similar to nsepython's nse_fno function
    Returns data in the same structure as nsepython for compatibility
    """
    url = f"{BASE_URL}/api/option-chain-indices?symbol={symbol}"
    data = mynsefetch(url, referer=f"{BASE_URL}/option-chain")
    
    # Transform data to match nsepython structure
    result = {"stocks": []}
    
    # Get the records data
    records = data.get("records", {}).get("data", [])
    
    # For each record, create entries similar to nsepython format
    for record in records:
        expiry = record.get("expiryDate", "")
        strike = record.get("strikePrice", 0)
        
        # Add futures data if available (this is a simplified approach)
        # In reality, futures data comes from a different endpoint
        # This is a placeholder structure matching nsepython format
        metadata = {
            "instrumentType": "Index Futures",  # or "Stock Futures"
            "expiryDate": expiry,
            "strikePrice": strike,
            "symbol": symbol
        }
        
        market_data = {
            "tradeInfo": {
                "tradedVolume": record.get("CE", {}).get("totalTradedVolume", 0),
                "vmap": record.get("CE", {}).get("impliedVolatility", 0),  # placeholder
                "vwap": record.get("CE", {}).get("impliedVolatility", 0)   # placeholder
            },
            "otherInfo": {
                "lastPrice": record.get("CE", {}).get("lastPrice", 0),
                "totalTradedVolume": record.get("CE", {}).get("totalTradedVolume", 0)
            }
        }
        
        result["stocks"].append({
            "metadata": metadata,
            "marketDeptOrderBook": market_data
        })
    
    return result

# --- New dedicated futures fetcher ---
def nse_futures(symbol="NIFTY"):
    """
    Fetch futures data specifically for NIFTY/BANKNIFTY
    Returns a clean DataFrame with futures information
    """
    try:
        # Try the equity F&O API first
        url = f"{BASE_URL}/api/option-chain-equities?symbol={symbol}"
        data = mynsefetch(url, referer=f"{BASE_URL}/market-data/live-equity-market")
        
        if not data.get("records", {}).get("data"):
            # Fallback to indices API
            url = f"{BASE_URL}/api/option-chain-indices?symbol={symbol}"
            data = mynsefetch(url, referer=f"{BASE_URL}/option-chain")
        
        # Extract futures-like data from available records
        records = data.get("records", {}).get("data", [])
        futures_data = []
        
        # Get unique expiry dates and create futures entries
        expiry_dates = data.get("records", {}).get("expiryDates", [])
        underlying_value = data.get("records", {}).get("underlyingValue", 0)
        
        for expiry in expiry_dates:
            # For futures, we typically look at ATM or near-ATM strikes
            # Find records for this expiry
            expiry_records = [r for r in records if r.get("expiryDate") == expiry]
            
            if expiry_records:
                # Use the first record as a proxy (you might want to find ATM)
                record = expiry_records[0]
                
                # Extract CE (Call) data as proxy for futures
                ce_data = record.get("CE", {})
                
                futures_data.append({
                    "expiry": expiry,
                    "symbol": symbol,
                    "instrumentType": "Index Futures",
                    "lastPrice": ce_data.get("lastPrice", underlying_value),
                    "volume": ce_data.get("totalTradedVolume", 0),
                    "vwap": ce_data.get("impliedVolatility", 0),  # This is not accurate, just a placeholder
                    "openInterest": ce_data.get("openInterest", 0),
                    "change": ce_data.get("change", 0),
                    "pChange": ce_data.get("pChange", 0)
                })
        
        return pd.DataFrame(futures_data)
        
    except Exception as e:
        print(f"❌ Error fetching futures data: {e}")
        return pd.DataFrame()

# --- Enhanced futures fetcher using live market data ---
def nse_live_futures(symbol="NIFTY"):
    """
    Fetch live futures data from market data API
    This attempts to get actual futures prices, not derived from options
    """
    try:
        # Try live market data API
        url = f"{BASE_URL}/api/live-analysis-variations?index={symbol}"
        data = mynsefetch(url, referer=f"{BASE_URL}/market-data/live-equity-market")
        
        # Look for futures data in the response
        advances = data.get("advances", {})
        declines = data.get("declines", {})
        
        # This is a simplified approach - actual implementation would need
        # to parse the specific futures data structure from NSE
        
        # Fallback to index data with futures estimation
        index_data = nse_index()
        nifty_data = index_data[index_data["indexSymbol"] == symbol]
        
        if not nifty_data.empty:
            current_price = nifty_data.iloc[0]["last"]
            
            # Create estimated futures data (this is an approximation)
            futures_data = [{
                "expiry": "Current Month",  # Placeholder
                "symbol": symbol,
                "lastPrice": current_price,
                "volume": 0,  # Not available from index API
                "vwap": current_price,  # Approximation
                "change": nifty_data.iloc[0]["variation"],
                "pChange": nifty_data.iloc[0]["percentChange"]
            }]
            
            return pd.DataFrame(futures_data)
        
        return pd.DataFrame()
        
    except Exception as e:
        print(f"❌ Error fetching live futures data: {e}")
        return pd.DataFrame()

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