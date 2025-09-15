
"""
Logic for sales and risk forecasting using BigQuery ML.FORECAST and AI models.
Provides functions to forecast sales, risk, or other business metrics for dashboarding.
"""
from google.cloud import bigquery

def forecast_sales(client, project_id, dataset, sales_table, model_name, output_table, horizon=12):
		"""
		Uses BigQuery ML.FORECAST to predict future sales for each product.
		Writes results to output_table (product_id, forecast_timestamp, forecast_sales).
		"""
		query = f"""
		CREATE OR REPLACE TABLE `{project_id}.{dataset}.{output_table}` AS
		SELECT
			product_id,
			forecast_timestamp,
			forecast_value AS forecast_sales
		FROM ML.FORECAST(
			MODEL `{project_id}.{dataset}.{model_name}`,
			STRUCT({horizon} AS horizon, 0.9 AS confidence_level)
		);
		"""
		job = client.query(query)
		job.result()
		print(f"Sales forecast written to {output_table}")

def forecast_risk(client, project_id, dataset, risk_model_name, input_table, output_table):
		"""
		Uses a BigQuery ML/AI model to predict risk scores for products.
		Writes results to output_table (product_id, risk_score).
		"""
		query = f"""
		CREATE OR REPLACE TABLE `{project_id}.{dataset}.{output_table}` AS
		SELECT
			product_id,
			predicted_risk AS risk_score
		FROM ML.PREDICT(MODEL `{project_id}.{dataset}.{risk_model_name}`,
			(SELECT * FROM `{project_id}.{dataset}.{input_table}`)
		);
		"""
		job = client.query(query)
		job.result()
		print(f"Risk forecast written to {output_table}")
