from google.cloud import bigquery
from google.api_core.exceptions import Conflict

PROJECT_ID = "proj-product-qc-gmumabigq"  # <-- Replace with your GCP project ID
DATASET = "product_qc"

TABLES = [
    {
        "table_id": f"{PROJECT_ID}.{DATASET}.text_embeddings",
        "schema": [
            bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("text_vector", "FLOAT64", mode="REPEATED"),
            bigquery.SchemaField("embed_ts", "TIMESTAMP"),
        ],
    },
    {
        "table_id": f"{PROJECT_ID}.{DATASET}.image_embeddings",
        "schema": [
            bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("image_gcs_uri", "STRING"),
            bigquery.SchemaField("image_vector", "FLOAT64", mode="REPEATED"),
            bigquery.SchemaField("embed_ts", "TIMESTAMP"),
        ],
    },
]

def create_table_if_not_exists(client, table_id, schema):
    try:
        client.get_table(table_id)
        print(f"Table '{table_id}' already exists.")
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        print(f"Created table '{table_id}'.")

def main():
    client = bigquery.Client(project=PROJECT_ID)
    for tbl in TABLES:
        create_table_if_not_exists(client, tbl["table_id"], tbl["schema"])

if __name__ == "__main__":
    main()
