"""
Phase 3: Intelligent Recommendations & Business Insights
- Corrective Suggestions (AI.GENERATE_TEXT)
- Image-Text Alignment Alerts (AI.GENERATE_TEXT)
- Customer Review Alignment (AI.GENERATE_BOOL)

Enhanced Phase 4: Advanced Business Intelligence
- Confidence-scored corrections with risk assessment
- Enhanced mismatch explanations with root cause analysis
- Business action plans with priority rankings
- Multi-dimensional suggestion scoring

This module provides functions to orchestrate BigQuery AI-powered business logic for actionable insights.
"""

from google.cloud import bigquery
from typing import Optional, Dict, List
import pandas as pd
import json

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

def generate_enhanced_corrected_descriptions(client: Optional[bigquery.Client] = None, min_mismatch_score: float = 0.5):
    """
    Enhanced correction generation with confidence scoring and detailed explanations.
    Uses sophisticated AI prompts for better correction quality and business intelligence.
    """
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    
    query = f'''
    SELECT
      p.product_id,
      p.description AS original_description,
      p.specs,
      ms.mismatch_score,
      -- Enhanced correction with confidence scoring
      AI.GENERATE_TEXT(
        'You are a professional product content manager. Analyze this product and provide corrections.
         
         ORIGINAL DESCRIPTION: ' || p.description || '
         TECHNICAL SPECS: ' || TO_JSON_STRING(p.specs) || '
         MISMATCH SCORE: ' || CAST(ms.mismatch_score AS STRING) || '/100
         
         Provide your response in this exact JSON format:
         {{
           "corrected_description": "Write a corrected, professional product description that accurately reflects the specs",
           "confidence_score": 85,
           "changes_made": ["List specific changes made", "Another change"],
           "root_cause": "Explain why the original description was problematic",
           "business_impact": "Explain the business risk of not fixing this",
           "priority": "High/Medium/Low based on mismatch severity"
         }}'
      ) AS enhanced_correction_json,
      -- Alternative correction approach
      AI.GENERATE_TEXT(
        'Create 3 different corrected descriptions for this product, ranked by quality.
         Specs: ' || TO_JSON_STRING(p.specs) || '
         Format as: "Option 1 (Best): [description] | Option 2: [description] | Option 3: [description]"'
      ) AS alternative_corrections,
      -- Assess correction difficulty
      AI.GENERATE_TEXT(
        'Rate the difficulty of correcting this product description on 1-10 scale and explain why.
         Original: ' || p.description || '
         Specs: ' || TO_JSON_STRING(p.specs) || '
         Format: "Difficulty: X/10. Reason: [explanation]"'
      ) AS correction_difficulty
    FROM `{PROJECT_ID}.{DATASET}.products` p
    JOIN `{PROJECT_ID}.{DATASET}.mismatch_scores` ms
      ON p.product_id = ms.product_id
    WHERE ms.mismatch_score > {min_mismatch_score}
    ORDER BY ms.mismatch_score DESC
    '''
    
    return client.query(query).to_dataframe()

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

def generate_enhanced_image_text_alerts(client: Optional[bigquery.Client] = None, min_vector_mismatch: float = 0.7):
    """
    Enhanced image-text mismatch alerts with detailed visual analysis and confidence scoring.
    Provides actionable insights for resolving image-text inconsistencies.
    """
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    
    query = f'''
    SELECT
      ms.product_id,
      p.description,
      ms.vector_mismatch,
      -- Enhanced visual mismatch analysis
      AI.GENERATE_TEXT(
        'You are a visual quality control expert. Analyze this image-text mismatch:
         
         PRODUCT DESCRIPTION: ' || p.description || '
         VECTOR MISMATCH SCORE: ' || CAST(ms.vector_mismatch AS STRING) || '
         
         Provide analysis in this JSON format:
         {{
           "primary_visual_issues": ["List main visual problems", "Another issue"],
           "specific_mismatches": {{
             "color": "Expected vs Actual color discrepancy",
             "size": "Size representation issues", 
             "style": "Style/design inconsistencies",
             "branding": "Brand element problems"
           }},
           "confidence_assessment": 85,
           "business_impact": "Customer confusion/return risk level",
           "recommended_action": "Replace image/Update description/Other",
           "urgency_level": "Critical/High/Medium/Low"
         }}'
      ) AS detailed_visual_analysis,
      -- Generate corrected image requirements
      AI.GENERATE_TEXT(
        'Based on this product description, specify exactly what the replacement image should show:
         Description: ' || p.description || '
         
         Format: "REQUIRED IMAGE SPECS: [detailed visual requirements for photographer/designer]"'
      ) AS image_replacement_specs,
      -- Assess fix complexity and cost
      AI.GENERATE_TEXT(
        'Estimate the effort and cost to fix this image-text mismatch:
         Vector Mismatch: ' || CAST(ms.vector_mismatch AS STRING) || '
         
         Format: "EFFORT: [Low/Medium/High] | ESTIMATED COST: [Low/Medium/High] | TIMELINE: [days/weeks] | PRIORITY: [1-5]"'
      ) AS fix_assessment
    FROM `{PROJECT_ID}.{DATASET}.mismatch_scores` ms
    JOIN `{PROJECT_ID}.{DATASET}.products` p
      ON ms.product_id = p.product_id
    WHERE ms.vector_mismatch > {min_vector_mismatch}
    ORDER BY ms.vector_mismatch DESC
    '''
    
    return client.query(query).to_dataframe()

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

# --- Enhanced Phase 4 Functions ---

def generate_business_action_plan(unified_scores_df: pd.DataFrame, client: Optional[bigquery.Client] = None):
    """
    Convert technical validation findings into actionable business plans with priority rankings.
    """
    if unified_scores_df.empty:
        return pd.DataFrame()
    
    # Focus on high-risk products for action planning
    high_risk_products = unified_scores_df[unified_scores_df['risk_category'] == 'High'].head(10)
    
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    
    action_plans = []
    
    for _, product in high_risk_products.iterrows():
        # Generate detailed action plan using AI
        action_plan_query = f'''
        SELECT
          '{product["product_id"]}' AS product_id,
          AI.GENERATE_TEXT(
            'You are a business operations manager. Create an action plan for this quality issue:
             
             PRODUCT ID: {product["product_id"]}
             MISMATCH SCORE: {product["unified_mismatch_score"]}/100
             DESCRIPTION SCORE: {product["description_spec_score"]}/100
             IMAGE SCORE: {product["image_alignment_score"]}/100
             REVIEW SCORE: {product["review_alignment_score"]}/100
             CONFIDENCE: {product["confidence_score"]}/100
             
             Provide response in this JSON format:
             {{
               "immediate_actions": ["Action 1", "Action 2", "Action 3"],
               "responsible_teams": ["Team 1", "Team 2"],
               "estimated_timeline": "X weeks",
               "estimated_cost": "Low/Medium/High",
               "expected_roi": "Revenue impact if fixed",
               "risk_if_ignored": "Consequences of not acting",
               "success_metrics": ["Metric 1", "Metric 2"]
             }}'
          ) AS action_plan_json
        '''
        
        result = client.query(action_plan_query).to_dataframe()
        if not result.empty:
            action_plans.append({
                'product_id': product['product_id'],
                'mismatch_score': product['unified_mismatch_score'],
                'confidence_score': product['confidence_score'],
                'action_plan': result.iloc[0]['action_plan_json']
            })
    
    return pd.DataFrame(action_plans)

def assess_correction_priority(unified_scores_df: pd.DataFrame, business_metrics: Optional[Dict] = None):
    """
    Rank corrections by business impact and implementation feasibility.
    Combines technical scores with business metrics for priority ranking.
    """
    if unified_scores_df.empty:
        return pd.DataFrame()
    
    # Default business weights if not provided
    if business_metrics is None:
        business_metrics = {
            'revenue_weight': 0.4,
            'customer_impact_weight': 0.3,
            'brand_risk_weight': 0.2,
            'implementation_cost_weight': 0.1
        }
    
    priority_scores = []
    
    for _, product in unified_scores_df.iterrows():
        # Calculate priority score based on multiple factors
        mismatch_severity = product['unified_mismatch_score'] / 100
        confidence_factor = product['confidence_score'] / 100
        
        # Simulate business impact factors (in real implementation, these would come from business data)
        revenue_impact = min(1.0, mismatch_severity * 1.2)  # Higher mismatch = higher revenue risk
        customer_impact = mismatch_severity
        brand_risk = max(0.3, mismatch_severity) if product['risk_category'] == 'High' else mismatch_severity * 0.5
        implementation_cost = 1 - (product['confidence_score'] / 200)  # Lower confidence = higher implementation risk
        
        # Weighted priority score
        priority_score = (
            revenue_impact * business_metrics['revenue_weight'] +
            customer_impact * business_metrics['customer_impact_weight'] +
            brand_risk * business_metrics['brand_risk_weight'] +
            (1 - implementation_cost) * business_metrics['implementation_cost_weight']  # Invert cost (lower cost = higher priority)
        ) * confidence_factor
        
        # Determine priority category
        if priority_score > 0.8:
            priority_category = 'Critical'
        elif priority_score > 0.6:
            priority_category = 'High'
        elif priority_score > 0.4:
            priority_category = 'Medium'
        else:
            priority_category = 'Low'
        
        priority_scores.append({
            'product_id': product['product_id'],
            'unified_mismatch_score': product['unified_mismatch_score'],
            'priority_score': round(priority_score * 100, 2),
            'priority_category': priority_category,
            'revenue_impact_score': round(revenue_impact * 100, 2),
            'customer_impact_score': round(customer_impact * 100, 2),
            'brand_risk_score': round(brand_risk * 100, 2),
            'implementation_difficulty': round(implementation_cost * 100, 2),
            'confidence_score': product['confidence_score']
        })
    
    # Sort by priority score (highest first)
    priority_df = pd.DataFrame(priority_scores).sort_values('priority_score', ascending=False)
    
    return priority_df

def generate_executive_summary(unified_scores_df: pd.DataFrame, priority_df: pd.DataFrame):
    """
    Generate executive summary of quality issues and recommended actions.
    """
    if unified_scores_df.empty:
        return {}
    
    summary = {
        'overview': {
            'total_products_analyzed': len(unified_scores_df),
            'average_quality_score': round(100 - unified_scores_df['unified_mismatch_score'].mean(), 2),
            'products_requiring_attention': len(unified_scores_df[unified_scores_df['risk_category'].isin(['High', 'Medium'])]),
            'high_confidence_issues': len(unified_scores_df[unified_scores_df['confidence_score'] > 80])
        },
        'risk_breakdown': {
            'critical_issues': len(priority_df[priority_df['priority_category'] == 'Critical']) if not priority_df.empty else 0,
            'high_priority': len(priority_df[priority_df['priority_category'] == 'High']) if not priority_df.empty else 0,
            'medium_priority': len(priority_df[priority_df['priority_category'] == 'Medium']) if not priority_df.empty else 0,
            'low_priority': len(priority_df[priority_df['priority_category'] == 'Low']) if not priority_df.empty else 0
        },
        'quality_dimensions': {
            'description_spec_avg': round(unified_scores_df['description_spec_score'].mean(), 2),
            'image_alignment_avg': round(unified_scores_df['image_alignment_score'].mean(), 2),
            'review_alignment_avg': round(unified_scores_df['review_alignment_score'].mean(), 2)
        },
        'recommendations': {
            'immediate_focus': 'Address Critical and High priority items first',
            'resource_allocation': f"Focus on top {min(10, len(priority_df))} highest priority items",
            'success_metrics': ['Reduction in mismatch scores', 'Improved customer satisfaction', 'Decreased return rates']
        }
    }
    
    return summary
