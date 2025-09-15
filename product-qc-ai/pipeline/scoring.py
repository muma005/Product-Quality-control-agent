"""
Unified mismatch scoring logic for product validation pipeline.
Combines vector-based and rule-based mismatches into a single score and writes to mismatch_scores table.
Enhanced with 0-100 business-friendly scoring, confidence intervals, and risk categorization.
"""
from google.cloud import bigquery
import pandas as pd
import numpy as np

# Configuration
PROJECT_ID = 'proj-product-qc-gmumabigq'
DATASET = 'product_qc'

# Scoring weights for different validation dimensions
SCORING_WEIGHTS = {
    'description_spec': 0.40,  # 40% weight - most critical
    'image_alignment': 0.30,   # 30% weight - visual consistency
    'review_alignment': 0.30   # 30% weight - customer feedback
}

def compute_mismatch_scores(client, project_id, dataset, embeddings_table, bool_checks_table, output_table="mismatch_scores"):
    """
    Original mismatch scoring function - kept for backward compatibility.
    """
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

def compute_unified_mismatch_score(validation_results, weights=SCORING_WEIGHTS):
    """
    Compute unified 0-100 mismatch score from comprehensive validation results.
    Higher scores indicate more severe quality issues.
    
    Args:
        validation_results: Dict from run_comprehensive_validation()
        weights: Dict with scoring weights for each dimension
    
    Returns:
        DataFrame with unified scores and risk categories
    """
    try:
        # Extract validation dataframes
        desc_spec_df = validation_results.get('description_spec', pd.DataFrame())
        spec_image_df = validation_results.get('spec_image', pd.DataFrame())
        desc_image_df = validation_results.get('description_image', pd.DataFrame())
        reviews_df = validation_results.get('reviews_alignment', pd.DataFrame())
        
        # Get unique product IDs
        all_product_ids = set()
        for df in [desc_spec_df, spec_image_df, desc_image_df, reviews_df]:
            if not df.empty:
                all_product_ids.update(df['product_id'].unique())
        
        unified_scores = []
        
        for product_id in all_product_ids:
            score_components = {}
            
            # 1. Description-Spec Score (40% weight)
            desc_spec_score = 0
            if not desc_spec_df.empty:
                product_desc_spec = desc_spec_df[desc_spec_df['product_id'] == product_id]
                if not product_desc_spec.empty:
                    # Extract numeric score from AI analysis text
                    analysis_text = product_desc_spec.iloc[0].get('alignment_analysis', '')
                    desc_spec_score = extract_score_from_text(analysis_text)
                    
                    # Combine with vector distance (normalized to 0-100)
                    vector_dist = product_desc_spec.iloc[0].get('vector_distance', 0.5)
                    vector_score = min(100, vector_dist * 200)  # Convert 0-0.5 distance to 0-100 score
                    
                    desc_spec_score = (desc_spec_score + vector_score) / 2
            
            score_components['description_spec'] = desc_spec_score
            
            # 2. Image Alignment Score (30% weight - average of spec-image and desc-image)
            image_score = 0
            image_scores = []
            
            # Spec-Image alignment
            if not spec_image_df.empty:
                product_spec_image = spec_image_df[spec_image_df['product_id'] == product_id]
                if not product_spec_image.empty:
                    spec_image_score = extract_score_from_text(
                        product_spec_image.iloc[0].get('spec_image_analysis', '')
                    )
                    image_scores.append(spec_image_score)
            
            # Description-Image alignment
            if not desc_image_df.empty:
                product_desc_image = desc_image_df[desc_image_df['product_id'] == product_id]
                if not product_desc_image.empty:
                    desc_image_score = extract_score_from_text(
                        product_desc_image.iloc[0].get('description_image_analysis', '')
                    )
                    image_scores.append(desc_image_score)
            
            image_score = np.mean(image_scores) if image_scores else 0
            score_components['image_alignment'] = image_score
            
            # 3. Review Alignment Score (30% weight)
            review_score = 0
            if not reviews_df.empty:
                product_reviews = reviews_df[reviews_df['product_id'] == product_id]
                if not product_reviews.empty:
                    review_scores = []
                    for _, review_row in product_reviews.iterrows():
                        contradiction_score = extract_score_from_text(
                            review_row.get('review_contradiction_analysis', '')
                        )
                        review_scores.append(contradiction_score)
                    
                    review_score = np.mean(review_scores) if review_scores else 0
            
            score_components['review_alignment'] = review_score
            
            # Calculate weighted unified score
            unified_score = (
                score_components['description_spec'] * weights['description_spec'] +
                score_components['image_alignment'] * weights['image_alignment'] +
                score_components['review_alignment'] * weights['review_alignment']
            )
            
            # Generate risk category and confidence
            risk_category = categorize_risk(unified_score)
            confidence = calculate_confidence_score(score_components)
            
            unified_scores.append({
                'product_id': product_id,
                'unified_mismatch_score': round(unified_score, 2),
                'description_spec_score': round(score_components['description_spec'], 2),
                'image_alignment_score': round(score_components['image_alignment'], 2),
                'review_alignment_score': round(score_components['review_alignment'], 2),
                'risk_category': risk_category,
                'confidence_score': round(confidence, 2),
                'scoring_timestamp': pd.Timestamp.now()
            })
        
        return pd.DataFrame(unified_scores)
        
    except Exception as e:
        print(f"❌ Error computing unified scores: {str(e)}")
        return pd.DataFrame()

def extract_score_from_text(text):
    """
    Extract numeric score from AI-generated analysis text.
    Looks for patterns like "Score: 75/100" or "XX/100".
    """
    if not text or not isinstance(text, str):
        return 50  # Default neutral score
    
    import re
    # Look for score patterns
    score_patterns = [
        r'Score:\s*(\d+)/100',
        r'(\d+)/100',
        r'Score:\s*(\d+)',
        r'score.*?(\d+)',
    ]
    
    for pattern in score_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            score = int(match.group(1))
            return min(100, max(0, score))  # Clamp to 0-100 range
    
    return 50  # Default if no score found

def categorize_risk(score):
    """
    Categorize unified mismatch score into business risk levels.
    """
    if score < 30:
        return 'Low'
    elif score < 70:
        return 'Medium'
    else:
        return 'High'

def calculate_confidence_score(score_components):
    """
    Calculate confidence in the unified score based on data availability and consistency.
    """
    # Count how many score components have data
    available_components = sum(1 for score in score_components.values() if score > 0)
    
    # Base confidence on data availability
    base_confidence = (available_components / len(score_components)) * 100
    
    # Adjust for score consistency (lower variance = higher confidence)
    scores = [score for score in score_components.values() if score > 0]
    if len(scores) > 1:
        score_variance = np.var(scores)
        consistency_factor = max(0.5, 1 - (score_variance / 1000))  # Normalize variance impact
        base_confidence *= consistency_factor
    
    return min(100, max(20, base_confidence))  # Clamp to 20-100 range

def create_business_intelligence_summary(unified_scores_df):
    """
    Generate business intelligence summary from unified scores.
    """
    if unified_scores_df.empty:
        return {}
    
    summary = {
        'total_products': len(unified_scores_df),
        'average_mismatch_score': unified_scores_df['unified_mismatch_score'].mean(),
        'risk_distribution': unified_scores_df['risk_category'].value_counts().to_dict(),
        'high_risk_products': len(unified_scores_df[unified_scores_df['risk_category'] == 'High']),
        'average_confidence': unified_scores_df['confidence_score'].mean(),
        'top_problem_areas': {
            'description_spec_avg': unified_scores_df['description_spec_score'].mean(),
            'image_alignment_avg': unified_scores_df['image_alignment_score'].mean(),
            'review_alignment_avg': unified_scores_df['review_alignment_score'].mean()
        }
    }
    
    return summary
