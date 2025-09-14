"""
Fusion logic for product alignment across datasets using BigQuery SQL.
Aligns products by title+brand embedding similarity and creates a crosswalk table.
"""
from google.cloud import bigquery

def align_products_by_embedding(client, project_id, dataset, embeddings_table, snap_table, kaggle_table, crosswalk_table, field="title_brand", top_k=1, similarity_threshold=0.8):
    """
    Aligns products from SNAP and Kaggle datasets by cosine similarity of title+brand embeddings.
    Writes best matches to crosswalk_table.
    """
    # Assumes embeddings_table has: product_id, field, embedding, embed_ts
    # snap_table and kaggle_table have: product_id, title, brand
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset}.{crosswalk_table}` AS
    WITH snap_emb AS (
      SELECT product_id AS snap_id, embedding AS snap_embedding
      FROM `{project_id}.{dataset}.{embeddings_table}`
      WHERE field = '{field}' AND product_id IN (SELECT product_id FROM `{project_id}.{dataset}.{snap_table}`)
    ),
    kaggle_emb AS (
      SELECT product_id AS kaggle_id, embedding AS kaggle_embedding
      FROM `{project_id}.{dataset}.{embeddings_table}`
      WHERE field = '{field}' AND product_id IN (SELECT product_id FROM `{project_id}.{dataset}.{kaggle_table}`)
    ),
    matches AS (
      SELECT
        s.snap_id,
        k.kaggle_id,
        ML.DOT_PRODUCT(s.snap_embedding, k.kaggle_embedding) / (ML.NORM(s.snap_embedding) * ML.NORM(k.kaggle_embedding)) AS cosine_similarity,
        ROW_NUMBER() OVER (PARTITION BY s.snap_id ORDER BY ML.DOT_PRODUCT(s.snap_embedding, k.kaggle_embedding) / (ML.NORM(s.snap_embedding) * ML.NORM(k.kaggle_embedding)) DESC) AS rank
      FROM snap_emb s
      JOIN kaggle_emb k
      ON TRUE
    )
    SELECT snap_id, kaggle_id, cosine_similarity
    FROM matches
    WHERE rank <= {top_k} AND cosine_similarity >= {similarity_threshold};
    """
    job = client.query(query)
    job.result()
    print(f"Created/updated crosswalk table {crosswalk_table} with aligned products.")
