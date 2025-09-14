"""
Script to generate and store text embeddings (descriptions, specs, reviews) in BigQuery embedding tables using ML.GENERATE_EMBEDDING.
"""
import os
from google.cloud import bigquery
from pipeline import embeddings

def main():
    PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "your-gcp-project")
    DATASET = os.environ.get("BQ_DATASET", "product_qc")
    MODEL = os.environ.get("BQ_TEXT_EMBED_MODEL", "bq_model.textembedding")
    EMBEDDINGS_TABLE = os.environ.get("BQ_EMBEDDINGS_TABLE", "embeddings")
    client = bigquery.Client(project=PROJECT_ID)
    embeddings.generate_text_embeddings(client, PROJECT_ID, DATASET, MODEL, EMBEDDINGS_TABLE)

if __name__ == "__main__":
    main()
