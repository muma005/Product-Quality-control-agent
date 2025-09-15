"""
Phase 3: Intelligent Recommendations & Business Insights
- Corrective Suggestions (AI.GENERATE_TEXT)
- Image-Text Alignment Alerts (AI.GENERATE_TEXT)
- Customer Review Alignment (AI.GENERATE_BOOL)

This module provides functions to orchestrate BigQuery AI-powered business logic for actionable insights.
"""

from google.cloud import bigquery
from typing import Optional

# --- CONFIG ---
PROJECT_ID = "proj-product-qc-gmumabigq"
DATASET = "product_qc"

# --- 1.1 Corrective Suggestions ---
def generate_corrected_descriptions(client: Optional[bigquery.Client] = None, min_mismatch_score: float = 0.5, dest_table: Optional[str] = None):
    """
    Generate corrected product descriptions using AI.GENERATE_TEXT for products with high mismatch_score.
    Writes results to dest_table if provided, else returns DataFrame.
    """
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    query = f'''
    SELECT
      p.product_id,
      AI.GENERATE_TEXT(
        'Correct this product description based on the following specs: ' || TO_JSON_STRING(p.specs)
      ) AS corrected_description
    FROM `{PROJECT_ID}.{DATASET}.products` p
    JOIN `{PROJECT_ID}.{DATASET}.mismatch_scores` ms
      ON p.product_id = ms.product_id
    WHERE ms.mismatch_score > {min_mismatch_score}
    '''
    job = client.query(query)
    df = job.to_dataframe()
    if dest_table:
        df.to_gbq(dest_table, project_id=PROJECT_ID, if_exists='replace')
    return df

# --- 1.2 Image-Text Alignment Checks ---
def generate_image_text_alerts(client: Optional[bigquery.Client] = None, min_vector_mismatch: float = 0.7, dest_table: Optional[str] = None):
    """
    Generate alert text for image-text mismatches using AI.GENERATE_TEXT.
    Writes results to dest_table if provided, else returns DataFrame.
    """
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    query = f'''
    SELECT
      ms.product_id,
      AI.GENERATE_TEXT(
        'Explain why this product image does not match the description. Description: ' || p.description || ' | Specs: ' || TO_JSON_STRING(p.specs)
      ) AS image_text_alert
    FROM `{PROJECT_ID}.{DATASET}.mismatch_scores` ms
    JOIN `{PROJECT_ID}.{DATASET}.products` p
      ON ms.product_id = p.product_id
    WHERE ms.vector_mismatch > {min_vector_mismatch}
    '''
    job = client.query(query)
    df = job.to_dataframe()
    if dest_table:
        df.to_gbq(dest_table, project_id=PROJECT_ID, if_exists='replace')
    return df

# --- 1.3 Customer Review Alignment ---
def generate_review_alignment_flags(client: Optional[bigquery.Client] = None, dest_table: Optional[str] = None):
    """
    Use AI.GENERATE_BOOL to flag reviews that contradict product specs.
    Writes results to dest_table if provided, else returns DataFrame.
    """
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    query = f'''
    SELECT
      r.product_id,
      r.review,
      AI.GENERATE_BOOL(
        'Do the reviews mention that this product fails to meet its specs? Specs: ' || TO_JSON_STRING(p.specs),
        r.review
      ) AS review_flag
    FROM `{PROJECT_ID}.{DATASET}.reviews` r
    JOIN `{PROJECT_ID}.{DATASET}.products` p
      ON r.product_id = p.product_id
    '''
    job = client.query(query)
    df = job.to_dataframe()
    if dest_table:
        df.to_gbq(dest_table, project_id=PROJECT_ID, if_exists='replace')
    return df

# --- 2.1 Mismatch Heatmaps ---
def get_mismatch_heatmap(client: Optional[bigquery.Client] = None):
    """
    Aggregate mismatches across categories and return mismatch rates.
    """
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    query = f'''
    SELECT
      p.category,
      COUNTIF(ms.vector_mismatch > 0.7 OR ms.rule_mismatch = TRUE) AS mismatch_count,
      COUNT(*) AS total_products,
      SAFE_DIVIDE(COUNTIF(ms.vector_mismatch > 0.7 OR ms.rule_mismatch = TRUE), COUNT(*)) AS mismatch_rate
    FROM `{PROJECT_ID}.{DATASET}.mismatch_scores` ms
    JOIN `{PROJECT_ID}.{DATASET}.products` p
      ON ms.product_id = p.product_id
    GROUP BY p.category
    '''
    return client.query(query).to_dataframe()

# --- 2.2 Revenue / Risk Impact ---
def get_revenue_risk_impact(client: Optional[bigquery.Client] = None):
    """
    Approximate financial cost of mismatches by category.
    """
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    query = f'''
    SELECT
      category,
      SUM(estimated_sales * mismatch_rate) AS potential_loss
    FROM (
      SELECT
        category,
        product_id,
        sales_volume AS estimated_sales,
        mismatch_score > 0.7 AS mismatch_flag,
        SAFE_CAST(mismatch_score AS FLOAT64) AS mismatch_rate
      FROM `{PROJECT_ID}.{DATASET}.sales_joined`
    )
    GROUP BY category
    '''
    return client.query(query).to_dataframe()

# --- 2.3 Trend Forecasting ---
def get_mismatch_trend_forecast(client: Optional[bigquery.Client] = None, horizon: int = 30, confidence_level: float = 0.9):
    """
    Predict mismatch rates for the next period using ML.FORECAST.
    """
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    query = f'''
    SELECT *
    FROM ML.FORECAST(MODEL `{PROJECT_ID}.{DATASET}.mismatch_forecast_model`,
                     STRUCT({horizon} AS horizon, {confidence_level} AS confidence_level))
    '''
    return client.query(query).to_dataframe()

def get_product_details(product_id, client: Optional[bigquery.Client] = None):
    """
    Returns product details for a given product_id (or SKU/name if extended).
    """
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    query = f'''
    SELECT * FROM `{PROJECT_ID}.{DATASET}.products`
    WHERE product_id = @product_id
    '''
    job = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("product_id", "STRING", product_id)]
    ))
    return job.to_dataframe()

def get_product_reviews(product_id, client: Optional[bigquery.Client] = None):
    """
    Returns reviews and review flags for a given product_id.
    """
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    query = f'''
    SELECT r.review, raf.review_flag
    FROM `{PROJECT_ID}.{DATASET}.reviews` r
    LEFT JOIN `{PROJECT_ID}.{DATASET}.review_alignment_flags` raf
      ON r.product_id = raf.product_id AND r.review = raf.review
    WHERE r.product_id = @product_id
    '''
    job = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("product_id", "STRING", product_id)]
    ))
    return job.to_dataframe()

def get_ai_suggestions(product_id, client: Optional[bigquery.Client] = None):
    """
    Returns AI-generated suggestions for a given product_id (corrected description, alert message, confidence).
    """
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    query = f'''
    SELECT
      cd.corrected_description,
      ita.image_text_alert,
      NULL AS confidence
    FROM `{PROJECT_ID}.{DATASET}.corrected_descriptions` cd
    LEFT JOIN `{PROJECT_ID}.{DATASET}.image_text_alerts` ita
      ON cd.product_id = ita.product_id
    WHERE cd.product_id = @product_id
    '''
    job = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("product_id", "STRING", product_id)]
    ))
    df = job.to_dataframe()
    return df.iloc[0].to_dict() if not df.empty else {}

def get_product_reviews(product_id, client: Optional[bigquery.Client] = None):
    """
    Returns reviews for a specific product with review flags.
    """
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    query = f'''
    SELECT
      p.product_id,
      review,
      COALESCE(raf.review_flag, false) AS review_flag
    FROM `{PROJECT_ID}.{DATASET}.products` p,
    UNNEST(p.reviews) AS review
    LEFT JOIN `{PROJECT_ID}.{DATASET}.review_alignment_flags` raf
      ON p.product_id = raf.product_id AND review = raf.review
    WHERE p.product_id = @product_id
    '''
    job = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("product_id", "STRING", product_id)]
    ))
    return job.to_dataframe()

def get_ai_suggestions(product_id, client: Optional[bigquery.Client] = None):
    """
    Returns AI-generated suggestions and corrections for a specific product.
    """
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    query = f'''
    SELECT
      p.product_id,
      cd.corrected_description,
      ita.image_text_alert,
      ms.mismatch_score AS confidence
    FROM `{PROJECT_ID}.{DATASET}.products` p
    LEFT JOIN `{PROJECT_ID}.{DATASET}.corrected_descriptions` cd
      ON p.product_id = cd.product_id
    LEFT JOIN `{PROJECT_ID}.{DATASET}.image_text_alerts` ita
      ON p.product_id = ita.product_id
    LEFT JOIN `{PROJECT_ID}.{DATASET}.mismatch_scores` ms
      ON p.product_id = ms.product_id
    WHERE p.product_id = @product_id
    '''
    job = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("product_id", "STRING", product_id)]
    ))
    df = job.to_dataframe()
    return df.iloc[0].to_dict() if not df.empty else {}

def get_review_alignment_summary(client: Optional[bigquery.Client] = None):
    """
    Returns review alignment summary for all products (for dashboard tab 3).
    """
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    query = f'''
    SELECT
      p.product_id,
      review,
      COALESCE(raf.review_flag, false) AS review_flag,
      -- Optionally extract contradiction type/keywords if available
      NULL AS contradiction_type
    FROM `{PROJECT_ID}.{DATASET}.products` p,
    UNNEST(p.reviews) AS review
    LEFT JOIN `{PROJECT_ID}.{DATASET}.review_alignment_flags` raf
      ON p.product_id = raf.product_id AND review = raf.review
    WHERE ARRAY_LENGTH(p.reviews) > 0
    '''
    return client.query(query).to_dataframe()
