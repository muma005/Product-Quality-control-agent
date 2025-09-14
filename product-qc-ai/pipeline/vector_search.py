"""
Reusable vector search logic for finding similar products or mismatches using embeddings in BigQuery.
"""
from google.cloud import bigquery

def text_vector_search(client, project_id, dataset, product_id, top_k=5):
    query = f"""
    SELECT
      t1.product_id,
      t2.product_id AS similar_product_id,
      ML.DOT_PRODUCT(t1.text_vector, t2.text_vector) / (ML.NORM(t1.text_vector) * ML.NORM(t2.text_vector)) AS cosine_similarity
    FROM `{project_id}.{dataset}.text_embeddings` t1
    JOIN `{project_id}.{dataset}.text_embeddings` t2
      ON t1.product_id != t2.product_id
    WHERE t1.product_id = '{product_id}'
    ORDER BY cosine_similarity DESC
    LIMIT {top_k};
    """
    job = client.query(query)
    return list(job.result())
