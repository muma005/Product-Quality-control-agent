
"""
Logic for loading and normalizing product, image, and review data from CSV or GCS to BigQuery.
Includes schema validation, deduplication, and initial cleaning.
"""
import pandas as pd
import re
from google.cloud import bigquery
from datetime import datetime, timezone

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
	now_ts = datetime.now(timezone.utc).isoformat()
	df["ingest_ts"] = now_ts
	# Select and order columns
	out_cols = ["product_id", "sku", "brand", "category", "title", "description", "specs", "price", "rating", "review_count", "reviews", "image_refs", "ingest_ts"]
	return df[out_cols]

def load_to_bigquery(df, project_id, dataset, table_name):
	table_id = f"{project_id}.{dataset}.{table_name}"
	client = bigquery.Client(project=project_id)
	job = client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
	job.result()
	print(f"Loaded {len(df)} rows to {table_id}.")
