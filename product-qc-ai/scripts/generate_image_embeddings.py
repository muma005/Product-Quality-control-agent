"""
Generate image embeddings using BigQuery ML or BigFrames and store them in the image_embeddings table.
"""
import os
from google.cloud import bigquery

PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "your-gcp-project")
DATASET = os.environ.get("BQ_DATASET", "product_qc")
MODEL = os.environ.get("BQ_IMAGE_EMBED_MODEL", "bq_model.imageembedding")

client = bigquery.Client(project=PROJECT_ID)

def run_query(query, description):
    print(f"Running: {description}")
    job = client.query(query)
    job.result()
    print(f"Done: {description}\n")

def main():
    # Image embeddings
    image_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.image_embeddings` AS
    SELECT
      product_id,
      image_gcs_uri,
      ML.GENERATE_EMBEDDING(
        MODEL `{MODEL}`,
        image_gcs_uri
      ) AS image_vector,
      CURRENT_TIMESTAMP() AS embed_ts
    FROM `{PROJECT_ID}.{DATASET}.product_images`
    WHERE image_gcs_uri IS NOT NULL;
    """
    run_query(image_query, "Image embeddings")

if __name__ == "__main__":
    main()
