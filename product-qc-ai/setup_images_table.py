import os
import glob
from datetime import datetime, timezone
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# --- CONFIG ---
PROJECT_ID = "my_project_id"  # <-- Replace with your GCP project ID
DATASET_ID = "product_qc"
PRODUCTS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.products"
IMAGES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.product_images"
IMAGES_ROOT = os.path.join("data", "images")

# --- 1. Organize local product images ---
def collect_image_paths(images_root):
    image_records = []
    product_to_images = {}
    now_ts = datetime.now(timezone.utc).isoformat()
    if not os.path.exists(images_root):
        print(f"Image root directory '{images_root}' does not exist.")
        return image_records, product_to_images
    for product_id in os.listdir(images_root):
        product_dir = os.path.join(images_root, product_id)
        if not os.path.isdir(product_dir):
            continue
        image_files = glob.glob(os.path.join(product_dir, "*"))
        if not image_files:
            continue
        product_to_images[product_id] = []
        for img_path in image_files:
            rel_path = os.path.relpath(img_path)
            image_records.append({
                "product_id": product_id,
                "local_path": rel_path,
                "ingest_ts": now_ts
            })
            product_to_images[product_id].append({"local_path": rel_path})
    return image_records, product_to_images

# --- 2. Create BigQuery table for image references ---
def create_images_table(client, table_id):
    schema = [
        bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("local_path", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ingest_ts", "TIMESTAMP", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    try:
        client.get_table(table_id)
        print(f"Table '{table_id}' already exists.")
    except NotFound:
        client.create_table(table)
        print(f"Created table '{table_id}'.")

# --- 3. Update products table with image_refs ---
def update_products_image_refs(client, products_table, product_to_images):
    # Fetch all product_ids
    query = f"SELECT product_id FROM `{products_table}`"
    products = [row.product_id for row in client.query(query).result()]
    rows_to_update = []
    for product_id in products:
        image_refs = product_to_images.get(product_id, [])
        if not image_refs:
            continue
        rows_to_update.append({
            "product_id": product_id,
            "image_refs": image_refs
        })
    if not rows_to_update:
        print("No products to update with image_refs.")
        return
    # Patch rows in BigQuery
    errors = client.insert_rows_json(products_table, rows_to_update, row_ids=[row["product_id"] for row in rows_to_update])
    if not errors:
        print(f"Updated image_refs for {len(rows_to_update)} products in '{products_table}'.")
    else:
        print(f"Errors updating image_refs: {errors}")

# --- MAIN ---
def main():
    client = bigquery.Client(project=PROJECT_ID)
    # 1. Collect image file paths
    image_records, product_to_images = collect_image_paths(IMAGES_ROOT)
    if not image_records:
        print("No images found. Exiting.")
        return
    # 2. Create and populate product_images table
    create_images_table(client, IMAGES_TABLE)
    errors = client.insert_rows_json(IMAGES_TABLE, image_records)
    if not errors:
        print(f"Inserted {len(image_records)} image records into '{IMAGES_TABLE}'.")
    else:
        print(f"Errors inserting image records: {errors}")
    # 3. Update products table with image_refs
    update_products_image_refs(client, PRODUCTS_TABLE, product_to_images)

if __name__ == "__main__":
    main()
