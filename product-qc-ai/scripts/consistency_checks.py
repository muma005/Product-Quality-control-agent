"""
Run consistency checks using pipeline.consistency module.
"""
import os
from google.cloud import bigquery
from pipeline import consistency

def main():
  PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "your-gcp-project")
  DATASET = os.environ.get("BQ_DATASET", "product_qc")
  threshold = float(os.environ.get("CONSISTENCY_THRESHOLD", 0.3))
  client = bigquery.Client(project=PROJECT_ID)
  results = consistency.check_text_image_consistency(client, PROJECT_ID, DATASET, threshold)
  for row in results:
    print(row)

if __name__ == "__main__":
  main()
