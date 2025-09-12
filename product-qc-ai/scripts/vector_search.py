"""
Run vector similarity search queries on the embeddings tables to find similar products or detect mismatches.
"""
import os
from google.cloud import bigquery

PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "your-gcp-project")
DATASET = os.environ.get("BQ_DATASET", "product_qc")

client = bigquery.Client(project=PROJECT_ID)

def run_query(query, description):
    print(f"Running: {description}")
    job = client.query(query)
    results = job.result()
    print(f"Done: {description}\n")
    return results

def main():
    # Example: Find top-5 similar products by text embedding (cosine similarity)
    product_id = os.environ.get("QUERY_PRODUCT_ID", "example_id")
    query = f"""
    SELECT
      t1.product_id,
      t2.product_id AS similar_product_id,
      ML.DOT_PRODUCT(t1.text_vector, t2.text_vector) / (ML.NORM(t1.text_vector) * ML.NORM(t2.text_vector)) AS cosine_similarity
    FROM `{PROJECT_ID}.{DATASET}.text_embeddings` t1
    JOIN `{PROJECT_ID}.{DATASET}.text_embeddings` t2
      ON t1.product_id != t2.product_id
    WHERE t1.product_id = '{product_id}'
    ORDER BY cosine_similarity DESC
    LIMIT 5;
    """
    results = run_query(query, f"Vector search for product {product_id}")
    for row in results:
        print(row)

if __name__ == "__main__":
    main()
