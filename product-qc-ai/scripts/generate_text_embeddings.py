"""
Script to generate and store text embeddings (descriptions, specs, reviews) in BigQuery embedding tables using ML.GENERATE_EMBEDDING.
"""
import os
from google.cloud import bigquery

# Set your GCP project and dataset
PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "your-gcp-project")
DATASET = os.environ.get("BQ_DATASET", "product_qc")
MODEL = os.environ.get("BQ_TEXT_EMBED_MODEL", "bq_model.textembedding")

client = bigquery.Client(project=PROJECT_ID)

def run_query(query, description):
    print(f"Running: {description}")
    job = client.query(query)
    job.result()
    print(f"Done: {description}\n")

def main():
    # 1. Description embeddings
    desc_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.text_embeddings` AS
    SELECT
      product_id,
      description,
      ML.GENERATE_EMBEDDING(
        MODEL `{MODEL}`,
        description
      ) AS text_vector,
      CURRENT_TIMESTAMP() AS embed_ts
    FROM `{PROJECT_ID}.{DATASET}.products`
    WHERE description IS NOT NULL;
    """
    run_query(desc_query, "Description embeddings")

    # 2. Specs embeddings
    specs_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.spec_embeddings` AS
    SELECT
      product_id,
      TO_JSON_STRING(specs) AS spec_text,
      ML.GENERATE_EMBEDDING(MODEL `{MODEL}`, TO_JSON_STRING(specs)) AS spec_vector,
      CURRENT_TIMESTAMP() AS embed_ts
    FROM `{PROJECT_ID}.{DATASET}.products`
    WHERE specs IS NOT NULL;
    """
    run_query(specs_query, "Specs embeddings")

    # 3. Reviews embeddings (optional)
    reviews_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.review_embeddings` AS
    SELECT
      p.product_id,
      review,
      ML.GENERATE_EMBEDDING(MODEL `{MODEL}`, review) AS review_vector,
      CURRENT_TIMESTAMP() AS embed_ts
    FROM `{PROJECT_ID}.{DATASET}.products` p,
    UNNEST(p.reviews) AS review
    WHERE review IS NOT NULL;
    """
    run_query(reviews_query, "Reviews embeddings")

if __name__ == "__main__":
    main()
