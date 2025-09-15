# Product CSV → BigQuery ingestion and normalization script
from pipeline.ingestion import load_and_normalize, load_to_bigquery

# --- CONFIG ---
PROJECT_ID = "proj-product-qc-gmumabigq"  # <-- Replace with your GCP project ID
DATASET_ID = "product_qc"
TABLE_NAME = "products"
CSV_PATH = r"C:\Users\ADMIN\Desktop\quality control\Product-Quality-control-agent\product-qc-ai\data\processed\quality_control.csv"

if __name__ == "__main__":
    df = load_and_normalize(CSV_PATH)
    load_to_bigquery(df, PROJECT_ID, DATASET_ID, TABLE_NAME)
