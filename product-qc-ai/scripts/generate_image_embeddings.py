"""
Generate image embeddings locally and upload them to BigQuery.
"""
import os
from google.cloud import bigquery
from pipeline import embeddings

def main():
    PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "your-gcp-project")
    DATASET = os.environ.get("BQ_DATASET", "product_qc")
    IMAGE_DIR = "data/images"  # Update as needed
    EMBEDDINGS_TABLE = os.environ.get("BQ_EMBEDDINGS_TABLE", "embeddings")
    client = bigquery.Client(project=PROJECT_ID)
    embeddings.generate_image_embeddings(IMAGE_DIR, PROJECT_ID, DATASET, EMBEDDINGS_TABLE, client)

if __name__ == "__main__":
    main()
