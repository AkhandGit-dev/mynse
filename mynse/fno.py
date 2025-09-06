# mynse/fno.py
import requests
import pandas as pd

def nse_fno(symbol="NIFTY"):
    """
    Fetch NSE F&O data (Futures & Options) for index/stock.
    Returns DataFrame with metadata + marketDeptOrderBook fields.
    """
    url = (
        f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
        if symbol in ["NIFTY", "BANKNIFTY"]
        else f"https://www.nseindia.com/api/option-chain-equities?symbol={symbol}"
    )

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com"
    }

    session = requests.Session()
    # Warm-up request to set cookies
    session.get("https://www.nseindia.com", headers=headers, timeout=10)

    res = session.get(url, headers=headers, timeout=10)
    res.raise_for_status()
    data = res.json()

    # Extract rows
    if "records" in data and "data" in data["records"]:
        rows = []
        for rec in data["records"]["data"]:
            # Futures data often appears here
            if "CE" in rec:
                rows.append({**rec["CE"], **rec.get("metadata", {})})
            if "PE" in rec:
                rows.append({**rec["PE"], **rec.get("metadata", {})})
        return pd.DataFrame(rows)

    return pd.DataFrame()  # empty fallback
