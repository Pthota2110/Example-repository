"""
Generate realistic synthetic financial transaction data for local testing.

Usage:
    python data/generate_sample_data.py --records 10000 --output data/transactions.parquet
    python data/generate_sample_data.py --records 100000 --output s3://bucket/raw/
"""

import argparse
import hashlib
import random
import uuid
from datetime import date, datetime, timedelta, timezone

import pandas as pd

TRANSACTION_TYPES = ["DEBIT", "CREDIT", "TRANSFER", "REFUND"]
STATUSES = ["PENDING", "SETTLED", "FLAGGED", "REVERSED", "FAILED"]
STATUS_WEIGHTS = [0.05, 0.88, 0.04, 0.02, 0.01]

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD"]
CURRENCY_WEIGHTS = [0.60, 0.15, 0.10, 0.05, 0.05, 0.05]

CATEGORIES = [
    "Groceries",
    "Restaurants",
    "Gas Stations",
    "Transit",
    "Hotels",
    "Airlines",
    "Retail",
    "ATM",
    "Pharmacy",
    "Software",
    "Electronics",
    "Healthcare",
    "Uncategorized",
]

COUNTRY_CODES = ["US", "GB", "DE", "FR", "CA", "AU", "JP", "NG", "RO"]
COUNTRY_WEIGHTS = [0.65, 0.08, 0.06, 0.05, 0.06, 0.04, 0.03, 0.02, 0.01]

MERCHANTS = [f"MERCHANT_{i:04d}" for i in range(1, 501)]


def hash_id(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def random_amount(transaction_type: str) -> float:
    if transaction_type == "REFUND":
        return round(random.uniform(5, 500), 2)
    if transaction_type == "TRANSFER":
        return round(random.uniform(100, 50000), 2)
    # Power-law distribution: most transactions small, a few very large
    base = random.lognormvariate(4.5, 1.5)
    return round(min(base, 99999.99), 2)


def generate_transaction(
    account_ids: list[str],
    start_date: date,
    end_date: date,
) -> dict:
    txn_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    txn_hour = random.choices(
        range(24),
        weights=[
            1,
            2,
            3,
            2,
            1,
            1,
            2,
            5,
            8,
            10,
            12,
            14,
            15,
            14,
            13,
            12,
            11,
            10,
            9,
            8,
            7,
            5,
            3,
            2,
        ],
    )[0]
    txn_minute = random.randint(0, 59)
    txn_second = random.randint(0, 59)

    txn_type = random.choice(TRANSACTION_TYPES)
    account_id = random.choice(account_ids)
    currency = random.choices(CURRENCIES, weights=CURRENCY_WEIGHTS)[0]
    country = random.choices(COUNTRY_CODES, weights=COUNTRY_WEIGHTS)[0]
    amount = random_amount(txn_type)
    created_ts = datetime(
        txn_date.year,
        txn_date.month,
        txn_date.day,
        txn_hour,
        txn_minute,
        txn_second,
        tzinfo=timezone.utc,
    )

    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": hash_id(account_id),
        "transaction_date": txn_date.isoformat(),
        "transaction_time": f"{txn_hour:02d}:{txn_minute:02d}:{txn_second:02d}",
        "amount": amount,
        "currency": currency,
        "transaction_type": txn_type,
        "merchant_id": random.choice(MERCHANTS) if txn_type != "TRANSFER" else None,
        "merchant_name": None,
        "category": random.choice(CATEGORIES),
        "status": random.choices(STATUSES, weights=STATUS_WEIGHTS)[0],
        "country_code": country,
        "device_id": (hash_id(f"device-{random.randint(1, 10000)}")[:16] if random.random() > 0.05 else None),
        "ip_address": hash_id(
            f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"
        ),
        "created_at": created_ts.isoformat(),
        "updated_at": created_ts.isoformat(),
    }


def generate_dataset(n: int, n_accounts: int = 1000, days_back: int = 90) -> pd.DataFrame:
    account_ids = [f"ACC-{i:06d}" for i in range(1, n_accounts + 1)]
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)

    print(f"Generating {n:,} transactions for {n_accounts} accounts over {days_back} days...")
    records = [generate_transaction(account_ids, start_date, end_date) for _ in range(n)]
    df = pd.DataFrame(records)
    df["amount"] = df["amount"].astype("float64")
    df["transaction_date"] = pd.to_datetime(df["transaction_date"]).dt.date
    print(f"Generated {len(df):,} records.")
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=10000)
    parser.add_argument("--accounts", type=int, default=1000)
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--output", default="data/transactions.parquet")
    args = parser.parse_args()

    df = generate_dataset(args.records, args.accounts, args.days)

    if args.output.startswith("s3://"):
        import io

        import boto3

        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        bucket, key = args.output.replace("s3://", "").split("/", 1)
        key = key.rstrip("/") + "/transactions.parquet"
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=buf.read())
        print(f"Uploaded to s3://{bucket}/{key}")
    else:
        df.to_parquet(args.output, index=False)
        print(f"Saved to {args.output}")

    print(df.describe())


if __name__ == "__main__":
    main()
