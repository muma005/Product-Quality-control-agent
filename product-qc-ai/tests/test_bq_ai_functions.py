from google.cloud import bigquery

PROJECT_ID = "proj-product-qc-gmumabigq"  # <-- Replace with your GCP project ID
DATASET = "product_qc"
PRODUCTS_TABLE = f"{PROJECT_ID}.{DATASET}.products"

# 1. Test ML.GENERATE_EMBEDDING on 5 descriptions
TEST_EMBEDDING_SQL = f"""
SELECT
  product_id,
  description,
  ML.GENERATE_EMBEDDING(MODEL `bq_model.textembedding`, description) AS emb
FROM `{PRODUCTS_TABLE}`
LIMIT 5;
"""

# 2. Test AI.GENERATE_BOOL on one row (example: does description mention 'red'?)
TEST_BOOL_SQL = f"""
SELECT
  product_id,
  description,
  AI.GENERATE_BOOL('Does the description mention the color red? Answer strictly true or false.', description) AS mentions_red
FROM `{PRODUCTS_TABLE}`
LIMIT 1;
"""

def main():
    client = bigquery.Client(project=PROJECT_ID)
    print("\n--- Test: ML.GENERATE_EMBEDDING on descriptions ---")
    try:
        df_emb = client.query(TEST_EMBEDDING_SQL).result().to_dataframe()
        print(df_emb)
    except Exception as e:
        print(f"Error running embedding test: {e}")
    print("\n--- Test: AI.GENERATE_BOOL on one row ---")
    try:
        df_bool = client.query(TEST_BOOL_SQL).result().to_dataframe()
        print(df_bool)
    except Exception as e:
        print(f"Error running bool test: {e}")

if __name__ == "__main__":
    main()
