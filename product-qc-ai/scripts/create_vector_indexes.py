"""
Automate the creation of vector indexes in BigQuery for fast similarity search.
"""
import os
from google.cloud import bigquery
from pipeline import vector_index

def main():
    PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "your-gcp-project")
    DATASET = os.environ.get("BQ_DATASET", "product_qc")
    client = bigquery.Client(project=PROJECT_ID)
    vector_index.create_default_indexes(client, PROJECT_ID, DATASET)

if __name__ == "__main__":
    main()
