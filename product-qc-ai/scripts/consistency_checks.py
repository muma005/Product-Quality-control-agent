"""
Implement logic to detect mismatches or inconsistencies between different modalities (e.g., description vs. image embeddings).
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
    # Example: Flag products where text and image embeddings are not similar (low cosine similarity)
    threshold = float(os.environ.get("CONSISTENCY_THRESHOLD", 0.3))
    query = f"""
    SELECT
      t.product_id,
      ML.DOT_PRODUCT(t.text_vector, i.image_vector) / (ML.NORM(t.text_vector) * ML.NORM(i.image_vector)) AS cosine_similarity
    FROM `{PROJECT_ID}.{DATASET}.text_embeddings` t
    JOIN `{PROJECT_ID}.{DATASET}.image_embeddings` i
      ON t.product_id = i.product_id
    WHERE ML.DOT_PRODUCT(t.text_vector, i.image_vector) / (ML.NORM(t.text_vector) * ML.NORM(i.image_vector)) < {threshold}
    ORDER BY cosine_similarity ASC;
    """
    results = run_query(query, "Consistency check: text vs. image embeddings")
    for row in results:
        print(row)

if __name__ == "__main__":
    main()
