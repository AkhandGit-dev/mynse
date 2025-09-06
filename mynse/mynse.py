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

# --- CORRECT F&O Data Function ---
def nse_fno(symbol="NIFTY"):
    """
    Fetch F&O data - the REAL API endpoint that returns actual futures data
    """
    # This is the correct API endpoint that returns both futures and options
    url = f"{BASE_URL}/api/option-chain-equities?symbol={symbol}"
    
    try:
        # Try equity derivatives first
        data = mynsefetch(url, referer=f"{BASE_URL}/get-quotes/derivatives?symbol={symbol}")
    except:
        # Fallback to indices derivatives
        url = f"{BASE_URL}/api/option-chain-indices?symbol={symbol}" 
        data = mynsefetch(url, referer=f"{BASE_URL}/get-quotes/derivatives?symbol={symbol}")
    
    # The response should already be in the correct format
    # NSE returns data with 'stocks' containing both futures and options
    if 'stocks' not in data:
        # If direct API doesn't work, we need to construct the response
        # This happens when NSE returns different format
        
        # Get the records from option chain
        records_data = data.get("records", {})
        
        result = {
            'info': records_data,
            'filter': {},
            'underlyingValue': records_data.get('underlyingValue'),
            'vfq': 0,
            'fut_timestamp': records_data.get('timestamp'),
            'opt_timestamp': records_data.get('timestamp'), 
            'stocks': [],
            'strikePrices': records_data.get('strikePrices', []),
            'expiryDates': records_data.get('expiryDates', []),
            'allSymbol': [],
            'underlyingInfo': {},
            'expiryDatesByInstrument': {}
        }
        
        # The key insight: We need to call the market-data API to get actual futures
        try:
            # Try the market data API for live derivatives
            market_url = f"{BASE_URL}/api/market-data-pre-open?key=FUTIDX&symbol={symbol}"
            market_data = mynsefetch(market_url, referer=f"{BASE_URL}/market-data/pre-open-market")
            
            # Parse futures from market data
            if 'data' in market_data:
                for item in market_data['data']:
                    metadata = item.get('metadata', {})
                    if 'FUTIDX' in metadata.get('identifier', ''):
                        # This is a futures record
                        futures_record = {
                            'metadata': {
                                'instrumentType': 'Index Futures',
                                'expiryDate': metadata.get('expiryDate'),
                                'symbol': symbol,
                                'identifier': metadata.get('identifier'),
                                'lastPrice': metadata.get('lastPrice'),
                                'change': metadata.get('change'),
                                'pChange': metadata.get('pChange'),
                                'openPrice': metadata.get('openPrice'),
                                'highPrice': metadata.get('highPrice'), 
                                'lowPrice': metadata.get('lowPrice'),
                                'closePrice': metadata.get('closePrice'),
                                'prevClose': metadata.get('prevClose'),
                                'numberOfContractsTraded': 0,
                                'totalTurnover': 0
                            },
                            'underlyingValue': records_data.get('underlyingValue'),
                            'volumeFreezeQuantity': 0,
                            'marketDeptOrderBook': {
                                'totalBuyQuantity': 0,
                                'totalSellQuantity': 0,
                                'bid': [],
                                'ask': [],
                                'carryOfCost': 0,
                                'tradeInfo': {
                                    'tradedVolume': item.get('tradedVolume', 0),
                                    'totalTradedVolume': item.get('totalTradedVolume', 0),
                                    'vmap': item.get('vwap', metadata.get('lastPrice')),
                                    'vwap': item.get('vwap', metadata.get('lastPrice'))
                                },
                                'otherInfo': {
                                    'lastPrice': metadata.get('lastPrice'),
                                    'ltp': metadata.get('lastPrice'),
                                    'totalTradedVolume': item.get('totalTradedVolume', 0)
                                }
                            }
                        }
                        result['stocks'].append(futures_record)
            
        except Exception as e:
            print(f"Warning: Could not fetch futures data from market API: {e}")
        
        return result
    
    return data

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