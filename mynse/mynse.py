# mynse.py
import pandas as pd
from nsepython import nse_fno as nse_fno_real

def nse_fno(symbol="NIFTY"):
    """
    Wrapper around nsepython.nse_fno to mimic mynse interface.
    Returns dict with 'stocks' key containing futures/options data.
    """
    data = nse_fno_real(symbol)
    return {"stocks": data["stocks"]}  # keep same interface

def index_futures_df(symbol="NIFTY"):
    """
    Returns a clean DataFrame of Index Futures (near/next/far)
    with columns: expiry, lastPrice, volume, vwap
    """
    data = nse_fno(symbol)
    records = data["stocks"]

    # Keep ONLY Index Futures rows
    fut_recs = [r for r in records
                if r.get("metadata", {}).get("instrumentType", "").lower().startswith("index futures")]

    rows = []
    for r in fut_recs:
        md = r.get("metadata", {})
        mkt = r.get("marketDeptOrderBook", {})
        ti  = mkt.get("tradeInfo", {})
        oi  = mkt.get("otherInfo", {})

        expiry = md.get("expiryDate")

        # Last Price
        last_price = (
            md.get("lastPrice")
            or oi.get("lastPrice")
            or oi.get("ltp")
            or mkt.get("lastPrice")
            or r.get("lastPrice")
        )

        # Volume & VWAP
        volume = ti.get("tradedVolume") or ti.get("totalTradedVolume") or oi.get("totalTradedVolume")
        vwap   = ti.get("vmap") or ti.get("vwap")

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
