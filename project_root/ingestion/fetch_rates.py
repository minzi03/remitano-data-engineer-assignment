import pandas as pd
import requests
import time
from pathlib import Path

BASE_URL = "https://api.binance.com"
TRANSACTIONS_PATH = Path("input/transactions.csv")
OUTPUT_DIR = Path("output/raw_rates")


# Convert datetime → milliseconds
def to_millis(dt):
    return int(dt.timestamp() * 1000)


# Map currency → symbolUSDT
def currency_to_symbol(currency: str):
    if currency.upper() in ["USDT", "USD"]:  # Không cần tỷ giá USDT
        return None
    return f"{currency.upper()}USDT"


# Fetch klines with retry and pagination
def fetch_klines(symbol, start_dt, end_dt, interval="1h"):
    all_rows = []
    start_ms = to_millis(start_dt)
    end_ms = to_millis(end_dt)

    while start_ms < end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 1000
        }

        # Retry loop
        success = False
        for attempt in range(5):
            try:
                resp = requests.get(
                    f"{BASE_URL}/api/v3/klines",
                    params=params,
                    timeout=20
                )
                resp.raise_for_status()
                data = resp.json()
                success = True
                break
            except Exception as e:
                print(f"[{symbol}] Retry {attempt+1}/5 failed: {e}")
                time.sleep(1)

        if not success:
            print(f"[{symbol}] Failed after multiple retries → SKIP THIS SYMBOL")
            return all_rows

        if not data:
            break

        all_rows.extend(data)

        # Move to next hour
        last_open_time = data[-1][0]
        start_ms = last_open_time + 1

        # Avoid rate limit
        time.sleep(0.3)

    return all_rows


def normalize_klines(symbol, raw_klines):
    rows = []
    for k in raw_klines:
        rows.append({
            "symbol": symbol,
            "open_time": pd.to_datetime(k[0], unit="ms", utc=True),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "close_time": pd.to_datetime(k[6], unit="ms", utc=True),
            "quote_asset_volume": float(k[7]),
            "number_of_trades": int(k[8]),
            "taker_buy_base_asset_volume": float(k[9]),
            "taker_buy_quote_asset_volume": float(k[10]),
        })
    return pd.DataFrame(rows)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Load transactions
    df_tx = pd.read_csv(TRANSACTIONS_PATH)
    df_tx["created_at"] = pd.to_datetime(df_tx["created_at"])

    # Time range
    start_ts = df_tx["created_at"].min().floor("h")
    end_ts = df_tx["created_at"].max().ceil("h")

    # Unique destination currencies
    dest_currencies = sorted(df_tx["destination_currency"].unique().tolist())

    print("Destination currencies:", dest_currencies)
    print("Time range:", start_ts, "→", end_ts)

    # 2. Loop through each currency
    for cur in dest_currencies:
        symbol = currency_to_symbol(cur)

        if not symbol:
            print(f"Skip {cur} (no rate needed)")
            continue

        # Check if already downloaded
        fname = f"{symbol.lower()}_1h_{start_ts.date()}_{end_ts.date()}.csv"
        out_path = OUTPUT_DIR / fname
        if out_path.exists():
            print(f"Skip {symbol}, file already exists.")
            continue

        print(f"Fetching klines for {symbol}...")

        raw_klines = fetch_klines(symbol, start_ts, end_ts)

        if not raw_klines:
            print(f"No data for {symbol} → skip.")
            continue

        df_rates = normalize_klines(symbol, raw_klines)
        df_rates.to_csv(out_path, index=False)

        print(f"Saved {len(df_rates)} rows for {symbol} → {out_path}")


if __name__ == "__main__":
    main()
