"""
Unified mismatch scoring logic for product validation pipeline.
Combines vector-based and rule-based mismatches into a single score and writes to mismatch_scores table.
"""
from google.cloud import bigquery

def compute_mismatch_scores(client, project_id, dataset, embeddings_table, bool_checks_table, output_table="mismatch_scores"):
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset}.{output_table}` AS
    SELECT
      d.product_id,
      SAFE_DIVIDE((
        COSINE_DISTANCE(d.embedding, s.embedding) +
        COSINE_DISTANCE(d.embedding, i.embedding)
      ), 2) AS vector_mismatch,
      BOOL_OR(NOT b.desc_spec_match) AS rule_mismatch
    FROM `{project_id}.{dataset}.{embeddings_table}` d
    JOIN `{project_id}.{dataset}.{embeddings_table}` s
      ON d.product_id = s.product_id AND d.field = 'description' AND s.field = 'specs'
    JOIN `{project_id}.{dataset}.{embeddings_table}` i
      ON d.product_id = i.product_id AND i.field = 'image'
    JOIN `{project_id}.{dataset}.{bool_checks_table}` b
      ON d.product_id = b.product_id
    GROUP BY d.product_id;
    """
    job = client.query(query)
    job.result()
    print(f"Created/updated mismatch scores table: {output_table}")
