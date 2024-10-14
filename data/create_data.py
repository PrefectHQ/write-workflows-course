import pandas as pd
from datetime import datetime, timedelta
import random


def create_data():
    """
    Create sample data that will be saved to a CSV file and stored in GitHub.
    """
    print("Extracting data...")
    data = []
    for _ in range(100):
        data.append(
            {
                "transaction_id": f"TRX{random.randint(1000, 9999)}",
                "ship_id": f"SHIP{random.randint(1, 50)}",
                "transaction_amount": round(random.uniform(1000, 100000), 2),
                "transaction_date": (
                    datetime.now() - timedelta(days=random.randint(0, 30))
                ).strftime("%Y-%m-%d"),
                "port": random.choice(
                    ["Rotterdam", "Singapore", "Shanghai", "Los Angeles", "New York"]
                ),
                "currency": random.choice(["EUR", "USD", "GBP", "JPY", "CNY"]),
            }
        )
    return pd.DataFrame(data)


def load_raw_data(df: pd.DataFrame, file_name: str):
    """
    Load the raw, untransformed data into a CSV file.
    """
    print("Loading raw data into CSV...")
    df.to_csv(file_name, index=False)
    print(f"Raw data saved to {file_name}")
    return


def main(file_name: str):
    df = create_data()
    print(df)
    load_raw_data(df, file_name)
    return


if __name__ == "__main__":
    main(f"raw_maritime_transactions_{datetime.now().strftime('%Y-%m-%d')}.csv")
