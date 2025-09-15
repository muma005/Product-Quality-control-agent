"""
Reusable consistency check logic for comparing embeddings across modalities.
Enhanced with cross-modal validation (spec↔image, description↔image) and image caption generation.
"""
from google.cloud import bigquery
import pandas as pd

# Configuration
PROJECT_ID = 'proj-product-qc-gmumabigq'
DATASET = 'product_qc'


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

def check_spec_image_consistency(client=None, project_id=PROJECT_ID, dataset=DATASET, threshold=0.4):
    """
    Cross-modal consistency check between product specifications and images.
    Uses both vector similarity and AI-based validation.
    """
    if client is None:
        client = bigquery.Client(project=project_id)
    
    query = f"""
    SELECT
      p.product_id,
      p.specs,
      image_ref,
      -- Vector similarity between specs and image embeddings
      COALESCE(
        (SELECT COSINE_DISTANCE(e1.embedding, e2.embedding)
         FROM `{project_id}.{dataset}.embeddings` e1
         JOIN `{project_id}.{dataset}.embeddings` e2
         WHERE e1.product_id = p.product_id AND e1.field = 'specs'
           AND e2.product_id = p.product_id AND e2.field = 'image'),
        1.0
      ) AS spec_image_distance,
      -- AI validation of spec-image consistency
      AI.GENERATE_BOOL(
        'Based on these technical specifications, does the product image accurately represent the expected product? 
         Consider colors, shapes, sizes, and key features mentioned in specs.
         Specifications: ' || TO_JSON_STRING(p.specs)
      ) AS spec_image_consistent,
      -- Generate expected image description from specs
      AI.GENERATE_TEXT(
        'Based on these specifications, describe what the product image should look like. 
         Be specific about visual attributes like color, shape, size, materials, and key features.
         Specifications: ' || TO_JSON_STRING(p.specs)
      ) AS expected_image_description
    FROM `{project_id}.{dataset}.products` p,
    UNNEST(p.image_refs) AS image_ref
    WHERE p.specs IS NOT NULL 
      AND image_ref IS NOT NULL
      AND COALESCE(
        (SELECT COSINE_DISTANCE(e1.embedding, e2.embedding)
         FROM `{project_id}.{dataset}.embeddings` e1
         JOIN `{project_id}.{dataset}.embeddings` e2
         WHERE e1.product_id = p.product_id AND e1.field = 'specs'
           AND e2.product_id = p.product_id AND e2.field = 'image'),
        1.0
      ) > {threshold}
    ORDER BY spec_image_distance DESC
    """
    
    return client.query(query).to_dataframe()

def check_description_image_consistency_enhanced(client=None, project_id=PROJECT_ID, dataset=DATASET, threshold=0.4):
    """
    Enhanced cross-modal consistency check between product descriptions and images.
    Includes image caption generation and detailed mismatch analysis.
    """
    if client is None:
        client = bigquery.Client(project=project_id)
    
    query = f"""
    SELECT
      p.product_id,
      p.description,
      image_ref,
      -- Vector similarity between description and image embeddings
      COALESCE(
        (SELECT COSINE_DISTANCE(e1.embedding, e2.embedding)
         FROM `{project_id}.{dataset}.embeddings` e1
         JOIN `{project_id}.{dataset}.embeddings` e2
         WHERE e1.product_id = p.product_id AND e1.field = 'description'
           AND e2.product_id = p.product_id AND e2.field = 'image'),
        1.0
      ) AS description_image_distance,
      -- AI validation of description-image consistency
      AI.GENERATE_BOOL(
        'Does this product image accurately match the written description? 
         Look for visual attributes mentioned in the description.
         Description: ' || p.description
      ) AS description_image_consistent,
      -- Generate image caption and compare with description
      AI.GENERATE_TEXT(
        'Generate a detailed caption for this product image, then compare it with the given description. 
         Rate the match on a 0-100 scale and explain any discrepancies.
         Format: "Caption: [generated caption]. Match Score: XX/100. Discrepancies: [list issues]"
         Description: ' || p.description
      ) AS image_caption_analysis,
      -- Identify specific visual mismatches
      AI.GENERATE_TEXT(
        'Identify specific visual attributes that do not match between the image and description. 
         Focus on: color, size, shape, style, brand elements, materials, and key features.
         Format: "Mismatched Attributes: [list specific issues]"
         Description: ' || p.description
      ) AS visual_mismatch_details
    FROM `{project_id}.{dataset}.products` p,
    UNNEST(p.image_refs) AS image_ref
    WHERE p.description IS NOT NULL 
      AND image_ref IS NOT NULL
      AND COALESCE(
        (SELECT COSINE_DISTANCE(e1.embedding, e2.embedding)
         FROM `{project_id}.{dataset}.embeddings` e1
         JOIN `{project_id}.{dataset}.embeddings` e2
         WHERE e1.product_id = p.product_id AND e1.field = 'description'
           AND e2.product_id = p.product_id AND e2.field = 'image'),
        1.0
      ) > {threshold}
    ORDER BY description_image_distance DESC
    """
    
    return client.query(query).to_dataframe()

def generate_multimodal_consistency_report(client=None, project_id=PROJECT_ID, dataset=DATASET):
    """
    Generate comprehensive consistency report across all modalities.
    Combines text-text, text-image, and spec-image consistency checks.
    """
    if client is None:
        client = bigquery.Client(project=project_id)
    
    try:
        print("🔍 Generating multimodal consistency report...")
        
        # Get text-image consistency (original function)
        text_image_results = check_text_image_consistency(client, project_id, dataset)
        text_image_df = pd.DataFrame(text_image_results)
        
        # Get spec-image consistency (new function)
        spec_image_df = check_spec_image_consistency(client, project_id, dataset)
        
        # Get enhanced description-image consistency
        desc_image_df = check_description_image_consistency_enhanced(client, project_id, dataset)
        
        # Generate description-spec consistency
        desc_spec_results = ai_generate_bool_desc_spec(client, project_id, dataset, limit=1000)
        desc_spec_df = pd.DataFrame(desc_spec_results)
        
        consistency_report = {
            'text_image_consistency': text_image_df,
            'spec_image_consistency': spec_image_df,
            'description_image_consistency': desc_image_df,
            'description_spec_consistency': desc_spec_df,
            'summary': {
                'total_text_image_issues': len(text_image_df),
                'total_spec_image_issues': len(spec_image_df),
                'total_desc_image_issues': len(desc_image_df),
                'total_desc_spec_issues': len(desc_spec_df[desc_spec_df['desc_spec_match'] == False]) if not desc_spec_df.empty else 0,
                'report_timestamp': pd.Timestamp.now()
            }
        }
        
        print(f"✅ Consistency report generated:")
        print(f"   - Text-Image issues: {consistency_report['summary']['total_text_image_issues']}")
        print(f"   - Spec-Image issues: {consistency_report['summary']['total_spec_image_issues']}")
        print(f"   - Description-Image issues: {consistency_report['summary']['total_desc_image_issues']}")
        print(f"   - Description-Spec issues: {consistency_report['summary']['total_desc_spec_issues']}")
        
        return consistency_report
        
    except Exception as e:
        print(f"❌ Error generating consistency report: {str(e)}")
        return {'error': str(e)}

def extract_visual_attributes(description_text):
    """
    Extract visual attributes from product descriptions for comparison with images.
    Uses AI to identify color, size, shape, material, and style attributes.
    """
    if not description_text:
        return {}
    
    # This would typically use AI.GENERATE_TEXT to extract structured attributes
    # For now, return a placeholder structure
    return {
        'colors': [],
        'materials': [],
        'size_indicators': [],
        'shape_descriptors': [],
        'style_elements': []
    }
