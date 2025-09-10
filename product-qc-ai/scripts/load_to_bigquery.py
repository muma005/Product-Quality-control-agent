# Product CSV → BigQuery ingestion and normalization script
import os
import pandas as pd
from google.cloud import bigquery
import re

# --- CONFIG ---
PROJECT_ID = "my_project_id"  # <-- Replace with your GCP project ID
DATASET_ID = "product_qc"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.products"
CSV_PATH = "./data/processed/products.csv"  # <-- Update path as needed

# --- LOAD CSV ---
def load_and_normalize(csv_path):
	df = pd.read_csv(csv_path)
	# Normalize text fields
	for col in ["title", "description", "brand", "category"]:
		if col in df.columns:
			df[col] = df[col].astype(str).str.strip().str.lower()
			df[col] = df[col].apply(lambda x: re.sub(r'<.*?>', '', x))  # Remove HTML tags
	# Flatten common fields, put variable specs into 'specs' JSON
	common_fields = ["product_id", "sku", "brand", "category", "title", "description", "price", "rating", "review_count"]
	variable_fields = [col for col in df.columns if col not in common_fields]
	df["specs"] = df[variable_fields].to_dict(orient="records")
	# Fill missing columns
	for col in common_fields:
		if col not in df.columns:
			df[col] = None
	# Add empty reviews and image_refs
	df["reviews"] = [[] for _ in range(len(df))]
	df["image_refs"] = [[] for _ in range(len(df))]
	# Add ingest_ts
	from datetime import datetime, timezone
	now_ts = datetime.now(timezone.utc).isoformat()
	df["ingest_ts"] = now_ts
	# Select and order columns
	out_cols = ["product_id", "sku", "brand", "category", "title", "description", "specs", "price", "rating", "review_count", "reviews", "image_refs", "ingest_ts"]
	return df[out_cols]

# --- LOAD TO BIGQUERY ---
def load_to_bigquery(df, table_id):
	client = bigquery.Client(project=PROJECT_ID)
	job = client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
	job.result()
	print(f"Loaded {len(df)} rows to {table_id}.")

if __name__ == "__main__":
	df = load_and_normalize(CSV_PATH)
	load_to_bigquery(df, TABLE_ID)
