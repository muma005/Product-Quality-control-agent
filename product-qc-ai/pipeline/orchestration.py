
"""
Logic to coordinate end-to-end pipeline steps: ingestion, preprocessing, embedding, validation, fusion, scoring, corrections, forecasting, etc.
Handles dependency management and error handling for repeatable runs.
"""
from google.cloud import bigquery

def run_full_pipeline(config):
	"""
	Orchestrates the full product QC pipeline using provided config dict.
	Calls each pipeline module in order, passing required parameters.
	"""
	# Example config keys: project_id, dataset, csv_path, tables, models, etc.
	from pipeline import ingestion, preprocessing, embeddings, validation, fusion, scoring, corrections, forecasting
	client = bigquery.Client(project=config['project_id'])

	# Ingestion
	df = ingestion.load_and_normalize(config['csv_path'])
	ingestion.load_to_bigquery(df, config['project_id'], config['dataset'], config['tables']['products'])

	# Preprocessing (optional)
	# df = preprocessing.flatten_product_json(df)

	# Embeddings
	embeddings.generate_text_embeddings(client, config['project_id'], config['dataset'], config['models']['embedding'], config['tables']['embeddings'])
	embeddings.generate_image_embeddings(config['image_dir'], config['project_id'], config['dataset'], config['tables']['embeddings'], client)

	# Validation
	validation.validate_description_vs_specs(client, config['project_id'], config['dataset'], config['tables']['products'], config['tables']['desc_spec_flags'])
	validation.validate_review_alignment(client, config['project_id'], config['dataset'], config['tables']['products'], config['tables']['review_alignment_flags'])
	validation.validate_image_text_alignment(client, config['project_id'], config['dataset'], config['tables']['products'], config['tables']['image_text_alerts'])

	# Fusion
	fusion.align_products_by_embedding(client, config['project_id'], config['dataset'], config['tables']['embeddings'], config['tables']['snap'], config['tables']['kaggle'], config['tables']['crosswalk'])

	# Scoring
	scoring.compute_mismatch_scores(client, config['project_id'], config['dataset'], config['tables']['embeddings'], config['tables']['desc_spec_flags'], config['tables']['mismatch_scores'])

	# Corrections
	corrections.suggest_corrected_descriptions(client, config['project_id'], config['dataset'], config['tables']['products'], config['tables']['corrected_descriptions'])

	# Forecasting
	forecasting.forecast_sales(client, config['project_id'], config['dataset'], config['tables']['sales_joined'], config['models']['sales_forecast'], config['tables']['sales_forecast'])
	forecasting.forecast_risk(client, config['project_id'], config['dataset'], config['models']['risk_model'], config['tables']['products'], config['tables']['risk_forecast'])

	print("Pipeline run complete.")
