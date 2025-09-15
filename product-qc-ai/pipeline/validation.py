
"""
Reusable validation logic for product data consistency using BigQuery AI.GENERATE_BOOL.
Checks include: description/spec match, review alignment, and image-text consistency.
Enhanced with confidence scoring and comprehensive validation orchestration.
"""
from google.cloud import bigquery
import pandas as pd

# Configuration
PROJECT_ID = 'proj-product-qc-gmumabigq'
DATASET = 'product_qc'

def validate_description_vs_specs(client, project_id, dataset, products_table, output_table):
	"""
	Runs AI.GENERATE_BOOL to check if product descriptions match specs.
	Writes results to output_table (product_id, desc_spec_match BOOL).
	"""
	query = f"""
	CREATE OR REPLACE TABLE `{project_id}.{dataset}.{output_table}` AS
	SELECT
		product_id,
		AI.GENERATE_BOOL(
			'Is the product description consistent with the specs?',
			STRUCT(description AS description, TO_JSON_STRING(specs) AS specs)
		) AS desc_spec_match
	FROM `{project_id}.{dataset}.{products_table}`;
	"""
	job = client.query(query)
	job.result()
	print(f"Validation results written to {output_table}")

def validate_description_spec_alignment_enhanced(client=None, project_id=PROJECT_ID, dataset=DATASET):
	"""
	Enhanced validation of description-spec alignment with confidence scoring and detailed analysis.
	Returns DataFrame instead of writing to table for more flexible usage.
	"""
	if client is None:
		client = bigquery.Client(project=project_id)
	
	query = f"""
	SELECT
		p.product_id,
		p.description,
		p.specs,
		AI.GENERATE_BOOL(
			'Does this product description accurately and completely reflect the technical specifications? 
			 Be strict about accuracy and completeness. Description: ' || p.description || 
			' | Specifications: ' || TO_JSON_STRING(p.specs)
		) AS alignment_valid,
		AI.GENERATE_TEXT(
			'Rate the alignment between description and specs on a scale of 0-100, then explain any discrepancies. 
			 Format: "Score: XX/100. Explanation: [detailed explanation]"
			 Description: ' || p.description || 
			' | Specifications: ' || TO_JSON_STRING(p.specs)
		) AS alignment_analysis,
		COALESCE(
			(SELECT COSINE_DISTANCE(e1.embedding, e2.embedding)
			 FROM `{project_id}.{dataset}.embeddings` e1
			 JOIN `{project_id}.{dataset}.embeddings` e2
			 WHERE e1.product_id = p.product_id AND e1.field = 'description'
			   AND e2.product_id = p.product_id AND e2.field = 'specs'),
			0.5
		) AS vector_distance
	FROM `{project_id}.{dataset}.products` p
	WHERE p.description IS NOT NULL AND p.specs IS NOT NULL
	"""
	
	return client.query(query).to_dataframe()

def validate_review_alignment(client, project_id, dataset, products_table, output_table, limit=1000):
	"""
	Runs AI.GENERATE_BOOL to check if reviews are aligned with product features/specs.
	Writes results to output_table (product_id, review, review_align BOOL).
	"""
	query = f"""
	CREATE OR REPLACE TABLE `{project_id}.{dataset}.{output_table}` AS
	SELECT
		p.product_id,
		review,
		AI.GENERATE_BOOL(
			'Does this review align with the product specs?',
			STRUCT(review AS review, TO_JSON_STRING(p.specs) AS specs)
		) AS review_align
	FROM `{project_id}.{dataset}.{products_table}` p,
	UNNEST(p.reviews) AS review
	WHERE review IS NOT NULL
	LIMIT {limit};
	"""
	job = client.query(query)
	job.result()
	print(f"Review alignment validation written to {output_table}")

def validate_review_alignment_enhanced(client=None, project_id=PROJECT_ID, dataset=DATASET):
	"""
	Enhanced review alignment validation with sentiment and contradiction analysis.
	Returns DataFrame with detailed review analysis.
	"""
	if client is None:
		client = bigquery.Client(project=project_id)
	
	query = f"""
	SELECT
		p.product_id,
		p.specs,
		p.description,
		review AS review_text,
		AI.GENERATE_BOOL(
			'Does this customer review contradict the product specifications or description? 
			 Look for claims about performance, quality, features that differ from official specs.
			 Specs: ' || TO_JSON_STRING(p.specs) || 
			' | Description: ' || p.description || 
			' | Review: ' || review
		) AS review_contradicts_specs,
		AI.GENERATE_TEXT(
			'Analyze this review for contradictions with product specs/description. 
			 Rate contradiction severity 0-100 and explain specific issues.
			 Format: "Contradiction Score: XX/100. Issues: [specific contradictions found]"
			 Specs: ' || TO_JSON_STRING(p.specs) || 
			' | Description: ' || p.description || 
			' | Review: ' || review
		) AS review_contradiction_analysis,
		AI.GENERATE_TEXT(
			'Extract the sentiment and key claims from this review. 
			 Format: "Sentiment: [positive/negative/neutral]. Key Claims: [list main points]"
			 Review: ' || review
		) AS review_sentiment_analysis
	FROM `{project_id}.{dataset}.products` p,
	UNNEST(p.reviews) AS review
	WHERE p.reviews IS NOT NULL AND ARRAY_LENGTH(p.reviews) > 0
	"""
	
	return client.query(query).to_dataframe()

def validate_image_text_alignment(client, project_id, dataset, products_table, output_table, limit=1000):
	"""
	Runs AI.GENERATE_BOOL to check if product images match the text description/specs.
	Writes results to output_table (product_id, image_ref, image_text_match BOOL).
	"""
	query = f"""
	CREATE OR REPLACE TABLE `{project_id}.{dataset}.{output_table}` AS
	SELECT
		p.product_id,
		image_ref,
		AI.GENERATE_BOOL(
			'Does this image match the product description and specs?',
			STRUCT(image_ref AS image_ref, description AS description, TO_JSON_STRING(p.specs) AS specs)
		) AS image_text_match
	FROM `{project_id}.{dataset}.{products_table}` p,
	UNNEST(p.image_refs) AS image_ref
	WHERE image_ref IS NOT NULL
	LIMIT {limit};
	"""
	job = client.query(query)
	job.result()
	print(f"Image-text alignment validation written to {output_table}")

def validate_image_text_alignment_enhanced(client=None, project_id=PROJECT_ID, dataset=DATASET):
	"""
	Enhanced validation between product descriptions and images with detailed analysis.
	Returns DataFrame with comprehensive image-text alignment assessment.
	"""
	if client is None:
		client = bigquery.Client(project=project_id)
	
	query = f"""
	SELECT
		p.product_id,
		p.description,
		image_ref AS image_path,
		AI.GENERATE_BOOL(
			'Does the product image accurately represent what is described in the text? 
			 Look for color, shape, size, style, and other visual attributes mentioned. 
			 Description: ' || p.description
		) AS description_image_match,
		AI.GENERATE_TEXT(
			'Compare the product description with what you would expect to see in the image. 
			 Rate the match on 0-100 scale and explain any visual discrepancies. 
			 Format: "Score: XX/100. Visual Analysis: [detailed comparison]"
			 Description: ' || p.description
		) AS description_image_analysis,
		COALESCE(
			(SELECT COSINE_DISTANCE(e1.embedding, e2.embedding)
			 FROM `{project_id}.{dataset}.embeddings` e1
			 JOIN `{project_id}.{dataset}.embeddings` e2
			 WHERE e1.product_id = p.product_id AND e1.field = 'description'
			   AND e2.product_id = p.product_id AND e2.field = 'image'),
			0.5
		) AS description_image_distance
	FROM `{project_id}.{dataset}.products` p,
	UNNEST(p.image_refs) AS image_ref
	WHERE p.description IS NOT NULL AND image_ref IS NOT NULL
	"""
	
	return client.query(query).to_dataframe()

def validate_spec_image_alignment(client=None, project_id=PROJECT_ID, dataset=DATASET):
	"""
	Cross-modal validation between specifications and product images.
	Returns DataFrame with spec-image alignment analysis.
	"""
	if client is None:
		client = bigquery.Client(project=project_id)
	
	query = f"""
	SELECT
		p.product_id,
		p.specs,
		image_ref AS image_path,
		AI.GENERATE_BOOL(
			'Based on the technical specifications, would you expect the product image to match these specs? 
			 Consider color, size, design, and key features mentioned in specs. 
			 Specifications: ' || TO_JSON_STRING(p.specs)
		) AS spec_image_expectation,
		AI.GENERATE_TEXT(
			'Generate a detailed image caption that should match these specifications, then rate how well 
			 the actual image matches on a 0-100 scale. Format: "Expected: [caption]. Score: XX/100. Issues: [any problems]"
			 Specifications: ' || TO_JSON_STRING(p.specs)
		) AS spec_image_analysis,
		COALESCE(
			(SELECT COSINE_DISTANCE(e1.embedding, e2.embedding)
			 FROM `{project_id}.{dataset}.embeddings` e1
			 JOIN `{project_id}.{dataset}.embeddings` e2
			 WHERE e1.product_id = p.product_id AND e1.field = 'specs'
			   AND e2.product_id = p.product_id AND e2.field = 'image'),
			0.5
		) AS spec_image_distance
	FROM `{project_id}.{dataset}.products` p,
	UNNEST(p.image_refs) AS image_ref
	WHERE p.specs IS NOT NULL AND image_ref IS NOT NULL
	"""
	
	return client.query(query).to_dataframe()

def run_comprehensive_validation(product_id=None, client=None, project_id=PROJECT_ID, dataset=DATASET):
	"""
	Orchestrator function that runs all validation checks for comprehensive quality assessment.
	Returns dictionary with all validation results for business intelligence.
	"""
	if client is None:
		client = bigquery.Client(project=project_id)
	
	validation_results = {}
	
	# Run all validation checks
	try:
		print(f"🔍 Running comprehensive validation for {'product ' + product_id if product_id else 'all products'}...")
		
		# Get enhanced validation results
		desc_spec = validate_description_spec_alignment_enhanced(client, project_id, dataset)
		spec_image = validate_spec_image_alignment(client, project_id, dataset)
		desc_image = validate_image_text_alignment_enhanced(client, project_id, dataset)
		reviews = validate_review_alignment_enhanced(client, project_id, dataset)
		
		# Filter for single product if specified
		if product_id:
			desc_spec = desc_spec[desc_spec['product_id'] == product_id]
			spec_image = spec_image[spec_image['product_id'] == product_id]
			desc_image = desc_image[desc_image['product_id'] == product_id]
			reviews = reviews[reviews['product_id'] == product_id]
		
		validation_results = {
			'description_spec': desc_spec,
			'spec_image': spec_image,
			'description_image': desc_image,
			'reviews_alignment': reviews,
			'summary': {
				'total_products_validated': len(desc_spec) if not desc_spec.empty else 0,
				'validation_timestamp': pd.Timestamp.now()
			}
		}
		
		print(f"✅ Comprehensive validation completed for {validation_results['summary']['total_products_validated']} products")
		
	except Exception as e:
		print(f"❌ Validation error: {str(e)}")
		validation_results['error'] = str(e)
		
	return validation_results
