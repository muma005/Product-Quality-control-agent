
"""
Logic for flattening nested product JSON, normalizing fields, and joining modalities (product, image, review) for unified records.
"""
import pandas as pd

def flatten_product_json(df):
	"""
	Flattens nested product JSON fields and normalizes for downstream embedding/validation.
	"""
	# Example: flatten 'specs' dict into top-level columns
	if 'specs' in df.columns:
		specs_df = pd.json_normalize(df['specs'])
		specs_df.columns = [f'spec_{c}' for c in specs_df.columns]
		df = pd.concat([df.drop(columns=['specs']), specs_df], axis=1)
	return df

def join_modalities(product_df, image_df=None, review_df=None):
	"""
	Joins product, image, and review data into unified records for embedding/validation.
	"""
	df = product_df.copy()
	if image_df is not None:
		df = df.merge(image_df, on='product_id', how='left', suffixes=('', '_image'))
	if review_df is not None:
		df = df.merge(review_df, on='product_id', how='left', suffixes=('', '_review'))
	return df
