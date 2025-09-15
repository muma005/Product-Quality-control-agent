
"""
Logic for AI-powered product description correction using BigQuery AI.GENERATE_TEXT.
Suggests improved/corrected product descriptions based on specs or detected mismatches.
"""
from google.cloud import bigquery

def suggest_corrected_descriptions(client, project_id, dataset, products_table, output_table):
		"""
		Uses AI.GENERATE_TEXT to generate improved product descriptions based on specs.
		Writes results to output_table (product_id, suggested_description).
		"""
		query = f"""
		CREATE OR REPLACE TABLE `{project_id}.{dataset}.{output_table}` AS
		SELECT
			product_id,
			AI.GENERATE_TEXT(
				'Generate a corrected product description based on these specs: ' || TO_JSON_STRING(specs)
			) AS suggested_description
		FROM `{project_id}.{dataset}.{products_table}`;
		"""
		job = client.query(query)
		job.result()
		print(f"Corrected descriptions written to {output_table}")
