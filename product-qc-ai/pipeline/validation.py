
"""
Reusable validation logic for product data consistency using BigQuery AI.GENERATE_BOOL.
Checks include: description/spec match, review alignment, and image-text consistency.
"""
from google.cloud import bigquery

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
