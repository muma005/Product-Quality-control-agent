import os
import sys
import json
import csv
from datetime import datetime, timezone
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# --- CONFIG ---
PROJECT_ID = "proj-product-qc-gmumabigq"  # <-- Replace with your GCP project ID
DATASET_ID = "product_qc"
TABLE_ID = f"{DATASET_ID}.products"

# --- SCHEMA ---
SCHEMA = [
    bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("sku", "STRING"),
    bigquery.SchemaField("brand", "STRING"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("specs", "JSON"),
    bigquery.SchemaField("price", "FLOAT64"),
    bigquery.SchemaField("rating", "FLOAT64"),
    bigquery.SchemaField("review_count", "INT64"),
    bigquery.SchemaField("reviews", "STRING", mode="REPEATED"),
    bigquery.SchemaField(
        "image_refs", "RECORD", mode="REPEATED",
        fields=[
            bigquery.SchemaField("gcs_uri", "STRING"),
            bigquery.SchemaField("object_ref", "STRING"),
        ]
    ),
    bigquery.SchemaField("ingest_ts", "TIMESTAMP"),
]

# --- UTILS ---
def create_dataset_if_not_exists(client, dataset_id):
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset '{dataset_id}' already exists.")
    except NotFound:
        dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Created dataset '{dataset_id}'.")

def create_table_if_not_exists(client, dataset_id, table_id, schema):
    try:
        client.get_table(table_id)
        print(f"Table '{table_id}' already exists.")
    except NotFound:
        table = bigquery.Table(f"{client.project}.{table_id}", schema=schema)
        client.create_table(table)
        print(f"Created table '{table_id}'.")

def parse_amazon_file(filepath):
    ext = os.path.splitext(filepath)[1].lower()
    records = []
    now_ts = datetime.now(timezone.utc).isoformat()
    with open(filepath, encoding="utf-8") as f:
        if ext == ".json":
            for line in f:
                if not line.strip():
                    continue
                raw = json.loads(line)
                record = {
                    "product_id": raw.get("asin"),
                    "sku": raw.get("sku", None),
                    "brand": raw.get("brand", None),
                    "category": (raw.get("categories") or [None])[0],
                    "title": raw.get("title", None),
                    "description": raw.get("description", None),
                    "specs": json.dumps({k: v for k, v in raw.items() if k not in ["asin", "sku", "brand", "categories", "title", "description", "price", "rating", "reviewCount", "reviews", "image_refs"]}),
                    "price": float(raw.get("price", 0)) if raw.get("price") else None,
                    "rating": float(raw.get("rating", 0)) if raw.get("rating") else None,
                    "review_count": int(raw.get("reviewCount", 0)) if raw.get("reviewCount") else None,
                    "reviews": [],
                    "image_refs": [],
                    "ingest_ts": now_ts,
                }
                records.append(record)
        elif ext == ".csv":
            reader = csv.DictReader(f)
            for raw in reader:
                record = {
                    "product_id": raw.get("asin"),
                    "sku": raw.get("sku", None),
                    "brand": raw.get("brand", None),
                    "category": (json.loads(raw["categories"])[0] if raw.get("categories") else None),
                    "title": raw.get("title", None),
                    "description": raw.get("description", None),
                    "specs": json.dumps({k: v for k, v in raw.items() if k not in ["asin", "sku", "brand", "categories", "title", "description", "price", "rating", "reviewCount", "reviews", "image_refs"]}),
                    "price": float(raw.get("price", 0)) if raw.get("price") else None,
                    "rating": float(raw.get("rating", 0)) if raw.get("rating") else None,
                    "review_count": int(raw.get("reviewCount", 0)) if raw.get("reviewCount") else None,
                    "reviews": [],
                    "image_refs": [],
                    "ingest_ts": now_ts,
                }
                records.append(record)
        else:
            print("Unsupported file type. Please provide a .json or .csv file.")
            sys.exit(1)
    return records

def main():
    if len(sys.argv) != 2:
        print("Usage: python setup_products_table.py <amazon_metadata_file.json|csv>")
        sys.exit(1)
    filepath = sys.argv[1]
    client = bigquery.Client(project=PROJECT_ID)
    create_dataset_if_not_exists(client, DATASET_ID)
    create_table_if_not_exists(client, DATASET_ID, "products", SCHEMA)
    records = parse_amazon_file(filepath)
    table_ref = f"{PROJECT_ID}.{TABLE_ID}"
    errors = client.insert_rows_json(table_ref, records)
    if not errors:
        print(f"Successfully inserted {len(records)} rows into {table_ref}.")
    else:
        print(f"Encountered errors while inserting rows: {errors}")

if __name__ == "__main__":
    main()
