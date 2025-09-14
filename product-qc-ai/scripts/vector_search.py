"""
Run vector similarity search queries on the embeddings tables to find similar products or detect mismatches.
"""
import os
from google.cloud import bigquery
from pipeline import vector_search

def main():
  PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "your-gcp-project")
  DATASET = os.environ.get("BQ_DATASET", "product_qc")
  product_id = os.environ.get("QUERY_PRODUCT_ID", "example_id")
  client = bigquery.Client(project=PROJECT_ID)
  results = vector_search.text_vector_search(client, PROJECT_ID, DATASET, product_id, top_k=5)
  for row in results:
    print(row)

if __name__ == "__main__":
  main()
