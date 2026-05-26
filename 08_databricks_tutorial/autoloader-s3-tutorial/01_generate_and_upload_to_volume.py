"""
Databricks Autoloader Tutorial - Step 1 (Volumes Edition)
==========================================================
Generates synthetic order events locally and saves them as JSON files.
Files are then uploaded to a Databricks Unity Catalog Volume using the
Databricks REST API — no S3 IAM config needed.

Each run creates one batch file. Run it multiple times to simulate
incremental arrivals that Autoloader will pick up.

Requirements:
    pip install requests faker

Usage:
    python 01_generate_and_upload_to_volume.py

Configuration (set as environment variables or edit the CONFIG block below):
    DATABRICKS_HOST          e.g. https://adb-1234567890.azuredatabricks.net
    DATABRICKS_TOKEN         Personal Access Token from Databricks Settings
    VOLUME_PATH              UC volume path, e.g. /Volumes/catalog/schema/volume_name/orders-raw
"""

import json
import os
import random
import uuid
from datetime import datetime, timezone

import requests
from faker import Faker

# ─────────────────────────────────────────────
# CONFIG — edit these or set as env variables
# ─────────────────────────────────────────────
DATABRICKS_HOST  = os.getenv("DATABRICKS_HOST",  "https://your-workspace.azuredatabricks.net")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")

# Unity Catalog Volume path — must start with /Volumes/
# Format: /Volumes/<catalog>/<schema>/<volume_name>/<subfolder>/
VOLUME_PATH = os.getenv(
    "VOLUME_PATH",
    "/Volumes/main/default/autoloader_demo/orders-raw"
)

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
    product  = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    return {
        "order_id":     str(uuid.uuid4()),
        "customer_id":  str(uuid.uuid4()),
        "customer_name": fake.name(),
        "email":        fake.email(),
        "product_id":   product["product_id"],
        "product_name": product["name"],
        "quantity":     quantity,
        "unit_price":   product["price"],
        "total_price":  round(product["price"] * quantity, 2),
        "status":       random.choice(STATUSES),
        "order_date":   fake.date_time_between(start_date="-30d", end_date="now").isoformat(),
        "ingested_at":  datetime.now(timezone.utc).isoformat(),
    }


def generate_batch(n: int) -> list[dict]:
    return [generate_order() for _ in range(n)]


def upload_to_volume(data: list[dict], volume_path: str, filename: str) -> None:
    """
    Upload NDJSON content to a Databricks Volume using the Files API.
    Docs: https://docs.databricks.com/api/workspace/files/upload
    """
    host  = DATABRICKS_HOST.rstrip("/")
    token = DATABRICKS_TOKEN

    # Encode the volume path for the URL
    # Files API endpoint: PUT /api/2.0/fs/files<volume_path>/<filename>
    full_path = f"{volume_path.rstrip('/')}/{filename}"
    url = f"{host}/api/2.0/fs/files{full_path}"

    body = "\n".join(json.dumps(record) for record in data).encode("utf-8")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream",
    }

    response = requests.put(url, headers=headers, data=body)

    if response.status_code in (200, 201, 204):
        print(f"  Uploaded {len(data)} records → dbfs:{full_path}")
    else:
        print(f"  ERROR {response.status_code}: {response.text}")
        response.raise_for_status()


def main():
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    batch_id  = str(uuid.uuid4())[:8]
    filename  = f"orders_{timestamp}_{batch_id}.json"

    print(f"Generating {RECORDS_PER_BATCH} synthetic order records...")
    batch = generate_batch(RECORDS_PER_BATCH)

    print(f"Uploading to Volume: {VOLUME_PATH}/{filename}")
    upload_to_volume(batch, VOLUME_PATH, filename)

    print("\nDone! Run this script again to upload another batch.")
    print("Databricks Autoloader will automatically detect the new file.")

    print("\n── Sample record ──────────────────────────────────")
    print(json.dumps(batch[0], indent=2))


if __name__ == "__main__":
    main()
