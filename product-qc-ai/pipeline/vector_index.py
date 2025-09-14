
from google.cloud import bigquery

def create_vector_index(client, project_id, dataset, table, column, index_name, distance_type="COSINE"):
	query = f"""
	CREATE VECTOR INDEX IF NOT EXISTS `{project_id}.{dataset}.{index_name}`
	ON `{project_id}.{dataset}.{table}`({column})
	OPTIONS (distance_type = \"{distance_type}\");
	"""
	job = client.query(query)
	job.result()
	print(f"Created vector index {index_name} on {table}({column}) with distance {distance_type}")

def create_default_indexes(client, project_id, dataset):
	# Spec embeddings index
	create_vector_index(
		client,
		project_id,
		dataset,
		table="spec_embeddings",
		column="spec_vector",
		index_name="text_spec_index",
		distance_type="COSINE"
	)
	# Image embeddings index
	create_vector_index(
		client,
		project_id,
		dataset,
		table="image_embeddings",
		column="image_vector",
		index_name="image_index",
		distance_type="COSINE"
	)
