"""
Databricks Autoloader Tutorial - Step 1 (Databricks CLI Edition)
=================================================================
Generates synthetic order events locally, saves them to a temp file,
then uploads to a Databricks Unity Catalog Volume using the Databricks CLI.

Prerequisites:
    databricks CLI configured (databricks configure --token)
    pip install faker

Usage:
    python 01_generate_and_upload_cli.py

Configuration:
    VOLUME_PATH  UC volume path, e.g. /Volumes/main/autoloader_demo/autoloader_demo/orders-raw
"""

import json
import os
import random
import subprocess
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

from faker import Faker

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
# Unity Catalog Volume path — must start with /Volumes/
VOLUME_PATH = os.getenv(
    "VOLUME_PATH",
    "/Volumes/main/autoloader_demo/autoloader_demo/orders-raw"
)

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
        "order_id":      str(uuid.uuid4()),
        "customer_id":   str(uuid.uuid4()),
        "customer_name": fake.name(),
        "email":         fake.email(),
        "product_id":    product["product_id"],
        "product_name":  product["name"],
        "quantity":      quantity,
        "unit_price":    product["price"],
        "total_price":   round(product["price"] * quantity, 2),
        "status":        random.choice(STATUSES),
        "order_date":    fake.date_time_between(start_date="-30d", end_date="now").isoformat(),
        "ingested_at":   datetime.now(timezone.utc).isoformat(),
    }


def upload_via_cli(local_path: Path, volume_path: str, filename: str) -> None:
    """Upload a local file to a Databricks Volume using the CLI."""
    dest = f"{volume_path.rstrip('/')}/{filename}"
    # cmd  = ["databricks", "fs", "cp", str(local_path), f"dbfs:{dest}"]

    print(f"  Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"  Uploaded → dbfs:{dest}")
    else:
        print(f"  ERROR:\n{result.stderr}")
        raise RuntimeError(f"databricks fs cp failed: {result.stderr}")


def main():
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    batch_id  = str(uuid.uuid4())[:8]
    filename  = f"orders_{timestamp}_{batch_id}.json"

    print(f"Generating {RECORDS_PER_BATCH} synthetic order records...")
    batch = [generate_order() for _ in range(RECORDS_PER_BATCH)]

    # Write to a temp file then upload
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, prefix="orders_"
    ) as tmp:
        tmp.write("\n".join(json.dumps(r) for r in batch))
        tmp_path = Path(tmp.name)

    try:
        print(f"Uploading {filename} to Volume...")
        upload_via_cli(tmp_path, VOLUME_PATH, filename)
    finally:
        tmp_path.unlink(missing_ok=True)

    print("\nDone! Run again to upload another batch.")
    print("Autoloader will detect the new file on the next trigger.")

    print("\n── Sample record ──────────────────────────────────")
    print(json.dumps(batch[0], indent=2))


if __name__ == "__main__":
    main()
