import pandas as pd
import yfinance as yf
from prefect import flow


@task
def fetch_stock_data(
    ticker: str, start_date: str, end_date: str, period: str = "1d"
) -> pd.DataFrame:
    df = yf.download(ticker, start=start_date, end=end_date, period=period)
    return df


@task
def save_raw_stock_data(df: pd.DataFrame, filename: str):
    df.to_csv(filename)


@task
def transform_stock_data(df: pd.DataFrame) -> pd.DataFrame:
    df["Moving Average"] = df["Close"].rolling(window=3).mean()
    return df


@task
def save_transformed_stock_data(df: pd.DataFrame, filename: str):
    df.to_csv(filename)


@flow
def fetch_and_save_stock_data(
    ticker: str = "AAPL",
    start_date: str = "2025-02-01",
    end_date: str = "2025-02-28",
    period: str = "1d",
):
    df_raw = fetch_stock_data(ticker, start_date, end_date, period)
    save_stock_data(df_raw, f"{ticker}_stock_data.csv")
    df_transformed = transform_stock_data(df)
    save_transformed_stock_data(df_transformed, f"{ticker}_transformed_stock_data.csv")


if __name__ == "__main__":
    fetch_and_save_stock_data()
