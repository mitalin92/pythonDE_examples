from __future__ import annotations
import pandas as pd

def load_data(filename:str) -> pd.DataFrame:
    try:
        df = pd.read_csv(filename)
    except FileNotFoundError:
        print("File is missing")
        return pd.DataFrame()
    return df

def get_missing_values(df: pd.DataFrame) -> pd.DataFrame:

    if df.empty:
        print("Dataframe is Empty")
        return pd.DataFrame()

    missing = df.isnull().sum()
    missing_percentage = (missing / len(df)) * 100
    return pd.DataFrame({
        "missing_values" : missing,
        "missing_percentage" : missing_percentage.round(2)
    })

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    if 'Volume' in df.columns:
        df['Volume'] = df['Volume'].fillna(0)
    
    price_cols = ["Date", "Open", "High", "Low", "Close", "Adj Close"]
    df = df.dropna(subset=price_cols)

    return df


def safe_types_conversion(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['Date'] = pd.to_datetime(df['Date'], errors="coerce")

    price_cols = ["Open", "High", "Low", "Close"]
    for col in price_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

def remove_duplicates(df: pd.DataFrame, subset:list[str]) -> pd.DataFrame:

    initial_count = len(df)
    df = df.drop_duplicates(subset=subset, keep="first")
    print(f"Removed {initial_count - len(df)} duplicates")

    return df

def validate_prices(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    invalid = df['High'] < df['Low']
    df_invalid_num = invalid.sum()

    if df_invalid_num>0:
        print(f"high<low violations {df_invalid_num}")
    
    df = df[~invalid]

    return df

def summary_stock(df: pd.DataFrame) -> pd.DataFrame:

    summary = (df.groupby('symbol')['Close']
        .agg(avg_close="mean", volatility="std")
        .reset_index()
    )

    summary["volatility"] = summary["volatility"].fillna(0)
    summary = summary.sort_values("volatility", ascending=False)

    if not summary.empty:
        most_volatile_symbol = summary.iloc[0]["symbol"]
    else:
        most_volatile_symbol = None

    print(f"most volatile symbol is {most_volatile_symbol}")

    return summary

if __name__ == "__main__":
    df = load_data("data/AAPL.csv")
    print(df.head())
    print(get_missing_values(df))

    if not df.empty and "symbol" not in df.columns:
        df["symbol"] = "AAPL"

    df_pipeline = (
        df
        .pipe(handle_missing_values)
        .pipe(safe_types_conversion)
        .pipe(remove_duplicates, subset= ['Date'])
        .pipe(validate_prices)
        .pipe(summary_stock)
    )


