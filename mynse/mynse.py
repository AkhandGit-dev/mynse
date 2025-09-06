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

# --- FIXED F&O Data Function ---
def nse_fno(symbol="NIFTY"):
    """
    Fetch F&O data similar to nsepython's nse_fno function
    Returns data in the same structure as nsepython for compatibility
    """
    # The key insight: nsepython likely calls a different API that returns both futures and options
    # Let's try the most likely candidates for the actual F&O API endpoint
    
    possible_endpoints = [
        # Most likely candidates based on NSE API patterns
        f"{BASE_URL}/api/option-chain-indices?symbol={symbol}",  # Current - but needs transformation
        f"{BASE_URL}/api/quote-derivative?symbol={symbol}",
        f"{BASE_URL}/api/quote-derivative?symbol={symbol}&segment=FUTIDX&identifier={symbol}",
        f"{BASE_URL}/api/derivatives?symbol={symbol}",
        f"{BASE_URL}/api/market-data-pre-open?key=FUTIDX&symbol={symbol}",
    ]
    
    # Try to find the endpoint that returns the correct structure
    for endpoint in possible_endpoints:
        try:
            if "option-chain-indices" in endpoint:
                # Special handling for option-chain API to transform into nse_fno format
                data = mynsefetch(endpoint, referer=f"{BASE_URL}/option-chain")
                
                # Transform the option-chain data to match nsepython's nse_fno structure
                result = {
                    'info': data.get('records', {}),
                    'filter': {},
                    'underlyingValue': data.get('records', {}).get('underlyingValue'),
                    'vfq': 0,
                    'fut_timestamp': data.get('records', {}).get('timestamp'),
                    'opt_timestamp': data.get('records', {}).get('timestamp'),
                    'stocks': [],
                    'strikePrices': data.get('records', {}).get('strikePrices', []),
                    'expiryDates': data.get('records', {}).get('expiryDates', []),
                    'allSymbol': [],
                    'underlyingInfo': {},
                    'expiryDatesByInstrument': {}
                }
                
                # Transform option chain records to stocks format
                records = data.get('records', {}).get('data', [])
                expiry_dates = data.get('records', {}).get('expiryDates', [])
                underlying_value = data.get('records', {}).get('underlyingValue', 0)
                
                # Create futures entries (one per expiry date)
                for expiry in expiry_dates:
                    # Create a futures record for this expiry
                    futures_record = {
                        'metadata': {
                            'instrumentType': 'Index Futures',
                            'expiryDate': expiry,
                            'strikePrice': 0,  # Futures don't have strike prices
                            'identifier': f'FUTIDX{symbol}{expiry.replace("-", "")}',
                            'symbol': symbol,
                            'openPrice': underlying_value,
                            'highPrice': underlying_value,
                            'lowPrice': underlying_value,
                            'closePrice': underlying_value,
                            'prevClose': underlying_value,
                            'lastPrice': underlying_value,
                            'change': 0,
                            'pChange': 0,
                            'numberOfContractsTraded': 0,
                            'totalTurnover': 0
                        },
                        'underlyingValue': underlying_value,
                        'volumeFreezeQuantity': 0,
                        'marketDeptOrderBook': {
                            'totalBuyQuantity': 0,
                            'totalSellQuantity': 0,
                            'bid': [],
                            'ask': [],
                            'carryOfCost': 0,
                            'tradeInfo': {
                                'tradedVolume': 0,
                                'totalTradedVolume': 0,
                                'vmap': underlying_value,  # VWAP as vmap
                                'vwap': underlying_value
                            },
                            'otherInfo': {
                                'lastPrice': underlying_value,
                                'ltp': underlying_value,
                                'totalTradedVolume': 0
                            }
                        }
                    }
                    result['stocks'].append(futures_record)
                
                # Add option records as well (transformed from option chain data)
                for record in records:
                    ce_data = record.get('CE', {})
                    pe_data = record.get('PE', {})
                    
                    # Add Call option
                    if ce_data:
                        call_record = {
                            'metadata': {
                                'instrumentType': 'Index Options',
                                'expiryDate': record.get('expiryDate'),
                                'optionType': 'Call',
                                'strikePrice': record.get('strikePrice'),
                                'identifier': f'OPTIDX{symbol}{record.get("expiryDate", "").replace("-", "")}CE{record.get("strikePrice", 0)}.00',
                                'symbol': symbol,
                                'openPrice': ce_data.get('openPrice', 0),
                                'highPrice': ce_data.get('highPrice', 0),
                                'lowPrice': ce_data.get('lowPrice', 0),
                                'closePrice': ce_data.get('closePrice', 0),
                                'prevClose': ce_data.get('prevClose', 0),
                                'lastPrice': ce_data.get('lastPrice', 0),
                                'change': ce_data.get('change', 0),
                                'pChange': ce_data.get('pChange', 0),
                                'numberOfContractsTraded': ce_data.get('totalTradedVolume', 0),
                                'totalTurnover': ce_data.get('totalTurnover', 0)
                            },
                            'underlyingValue': underlying_value,
                            'volumeFreezeQuantity': 0,
                            'marketDeptOrderBook': {
                                'totalBuyQuantity': ce_data.get('totalBuyQuantity', 0),
                                'totalSellQuantity': ce_data.get('totalSellQuantity', 0),
                                'bid': [],
                                'ask': [],
                                'carryOfCost': 0,
                                'tradeInfo': {
                                    'tradedVolume': ce_data.get('totalTradedVolume', 0),
                                    'totalTradedVolume': ce_data.get('totalTradedVolume', 0),
                                    'vmap': ce_data.get('impliedVolatility', 0),
                                    'vwap': ce_data.get('impliedVolatility', 0)
                                },
                                'otherInfo': {
                                    'lastPrice': ce_data.get('lastPrice', 0),
                                    'ltp': ce_data.get('lastPrice', 0),
                                    'totalTradedVolume': ce_data.get('totalTradedVolume', 0)
                                }
                            }
                        }
                        result['stocks'].append(call_record)
                    
                    # Add Put option
                    if pe_data:
                        put_record = {
                            'metadata': {
                                'instrumentType': 'Index Options',
                                'expiryDate': record.get('expiryDate'),
                                'optionType': 'Put',
                                'strikePrice': record.get('strikePrice'),
                                'identifier': f'OPTIDX{symbol}{record.get("expiryDate", "").replace("-", "")}PE{record.get("strikePrice", 0)}.00',
                                'symbol': symbol,
                                'openPrice': pe_data.get('openPrice', 0),
                                'highPrice': pe_data.get('highPrice', 0),
                                'lowPrice': pe_data.get('lowPrice', 0),
                                'closePrice': pe_data.get('closePrice', 0),
                                'prevClose': pe_data.get('prevClose', 0),
                                'lastPrice': pe_data.get('lastPrice', 0),
                                'change': pe_data.get('change', 0),
                                'pChange': pe_data.get('pChange', 0),
                                'numberOfContractsTraded': pe_data.get('totalTradedVolume', 0),
                                'totalTurnover': pe_data.get('totalTurnover', 0)
                            },
                            'underlyingValue': underlying_value,
                            'volumeFreezeQuantity': 0,
                            'marketDeptOrderBook': {
                                'totalBuyQuantity': pe_data.get('totalBuyQuantity', 0),
                                'totalSellQuantity': pe_data.get('totalSellQuantity', 0),
                                'bid': [],
                                'ask': [],
                                'carryOfCost': 0,
                                'tradeInfo': {
                                    'tradedVolume': pe_data.get('totalTradedVolume', 0),
                                    'totalTradedVolume': pe_data.get('totalTradedVolume', 0),
                                    'vmap': pe_data.get('impliedVolatility', 0),
                                    'vwap': pe_data.get('impliedVolatility', 0)
                                },
                                'otherInfo': {
                                    'lastPrice': pe_data.get('lastPrice', 0),
                                    'ltp': pe_data.get('lastPrice', 0),
                                    'totalTradedVolume': pe_data.get('totalTradedVolume', 0)
                                }
                            }
                        }
                        result['stocks'].append(put_record)
                
                return result
            
            else:
                # Try other endpoints directly
                data = mynsefetch(endpoint, referer=f"{BASE_URL}/get-quotes/derivatives?symbol={symbol}")
                
                # Check if this returns the expected structure
                if isinstance(data, dict) and 'stocks' in data:
                    return data
                    
        except Exception as e:
            print(f"❌ Failed endpoint {endpoint}: {e}")
            continue
    
    # If we get here, none of the endpoints worked
    raise RuntimeError(f"❌ Unable to fetch F&O data for {symbol}. All API endpoints failed.")

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