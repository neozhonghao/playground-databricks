"""
Databricks Autoloader Tutorial - Step 1
========================================
Generates synthetic order events locally and uploads them as JSON files to S3.

Each run creates one batch file — run it multiple times to simulate
incremental file arrivals (which Autoloader will detect in real time).

Requirements:
    pip install boto3 faker

Usage:
    python 01_generate_and_upload_to_s3.py

Configuration:
    Set the variables in the CONFIG section below, or export them as
    environment variables:
        AWS_ACCESS_KEY_ID
        AWS_SECRET_ACCESS_KEY
        S3_BUCKET_NAME
        S3_PREFIX
"""

import json
import os
import random
import uuid
from datetime import datetime, timezone

import boto3
from faker import Faker

# ─────────────────────────────────────────────
# CONFIG — edit these or set as env variables
# ─────────────────────────────────────────────
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID", "YOUR_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "YOUR_SECRET_KEY")
AWS_REGION            = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET             = os.getenv("S3_BUCKET_NAME", "nzh-playground-databricks")
S3_PREFIX             = os.getenv("S3_PREFIX", "autoloader-tutorial/orders-raw/")

# Number of records per batch file
RECORDS_PER_BATCH = 50
# ─────────────────────────────────────────────

fake = Faker()

PRODUCTS = [
    {"product_id": "P001", "name": "The Great Gatsby",       "price": 12.99},
    {"product_id": "P002", "name": "1984",                   "price": 9.99},
    {"product_id": "P003", "name": "To Kill a Mockingbird",  "price": 11.50},
    {"product_id": "P004", "name": "Dune",                   "price": 14.99},
    {"product_id": "P005", "name": "Foundation",             "price": 13.25},
]

STATUSES = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]


def generate_order() -> dict:
    """Generate a single synthetic order record."""
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    return {
        "order_id":    str(uuid.uuid4()),
        "customer_id": str(uuid.uuid4()),
        "customer_name": fake.name(),
        "email":       fake.email(),
        "product_id":  product["product_id"],
        "product_name": product["name"],
        "quantity":    quantity,
        "unit_price":  product["price"],
        "total_price": round(product["price"] * quantity, 2),
        "status":      random.choice(STATUSES),
        "order_date":  fake.date_time_between(start_date="-30d", end_date="now").isoformat(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def generate_batch(n: int) -> list[dict]:
    return [generate_order() for _ in range(n)]


def upload_to_s3(data: list[dict], bucket: str, key: str) -> None:
    """Upload a list of records as newline-delimited JSON (NDJSON) to S3."""
    s3 = boto3.client("s3")
    body = "\n".join(json.dumps(record) for record in data)
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
    print(f"  Uploaded {len(data)} records → s3://{bucket}/{key}")


def main():
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    batch_id  = str(uuid.uuid4())[:8]
    s3_key    = f"{S3_PREFIX}orders_{timestamp}_{batch_id}.json"

    print(f"Generating {RECORDS_PER_BATCH} synthetic order records...")
    batch = generate_batch(RECORDS_PER_BATCH)

    print(f"Uploading to s3://{S3_BUCKET}/{s3_key}")
    upload_to_s3(batch, S3_BUCKET, s3_key)

    print("\nDone! Run this script again to upload another batch.")
    print("Databricks Autoloader will automatically detect the new file.")

    # Preview first record
    print("\n── Sample record ──────────────────────────────────")
    print(json.dumps(batch[0], indent=2))


if __name__ == "__main__":
    main()
