"""
Reusable consistency check logic for comparing embeddings across modalities.
"""
from google.cloud import bigquery


def check_text_image_consistency(client, project_id, dataset, threshold=0.3):
    query = f"""
    SELECT
      t.product_id,
      ML.DOT_PRODUCT(t.text_vector, i.image_vector) / (ML.NORM(t.text_vector) * ML.NORM(i.image_vector)) AS cosine_similarity
    FROM `{project_id}.{dataset}.text_embeddings` t
    JOIN `{project_id}.{dataset}.image_embeddings` i
      ON t.product_id = i.product_id
    WHERE ML.DOT_PRODUCT(t.text_vector, i.image_vector) / (ML.NORM(t.text_vector) * ML.NORM(i.image_vector)) < {threshold}
    ORDER BY cosine_similarity ASC;
    """
    job = client.query(query)
    results = job.result()
    return list(results)

def ai_generate_bool_desc_spec(client, project_id, dataset, limit=10):
    query = f"""
    SELECT
      p.product_id,
      AI.GENERATE_BOOL(
        'Check if description is consistent with specs',
        STRUCT(p.description AS description, TO_JSON_STRING(p.specs) AS specs)
      ) AS desc_spec_match
    FROM `{project_id}.{dataset}.products` p
    LIMIT {limit};
    """
    job = client.query(query)
    return list(job.result())

def ai_generate_text_correction(client, project_id, dataset, product_id):
    query = f"""
    SELECT
      product_id,
      AI.GENERATE_TEXT(
        'Generate a corrected product description based on these specs: ' || TO_JSON_STRING(specs)
      ) AS suggested_description
    FROM `{project_id}.{dataset}.products`
    WHERE product_id = '{product_id}';
    """
    job = client.query(query)
    return list(job.result())
