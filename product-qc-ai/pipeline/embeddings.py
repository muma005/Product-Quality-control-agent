

from google.cloud import bigquery
import os
import pandas as pd
from datetime import datetime
import torch
from PIL import Image
from transformers import CLIPProcessor, CLIPModel

def generate_text_embeddings(client, project_id, dataset, model, embeddings_table):
		# Description embeddings
		desc_query = f"""
		INSERT INTO `{project_id}.{dataset}.{embeddings_table}` (product_id, field, embedding, embed_ts)
		SELECT
			product_id,
			'description' AS field,
			ML.GENERATE_EMBEDDING(MODEL `{model}`, description) AS embedding,
			CURRENT_TIMESTAMP() AS embed_ts
		FROM `{project_id}.{dataset}.products`
		WHERE description IS NOT NULL;
		"""
		client.query(desc_query).result()

		# Specs embeddings
		specs_query = f"""
		INSERT INTO `{project_id}.{dataset}.{embeddings_table}` (product_id, field, embedding, embed_ts)
		SELECT
			product_id,
			'specs' AS field,
			ML.GENERATE_EMBEDDING(MODEL `{model}`, TO_JSON_STRING(specs)) AS embedding,
			CURRENT_TIMESTAMP() AS embed_ts
		FROM `{project_id}.{dataset}.products`
		WHERE specs IS NOT NULL;
		"""
		client.query(specs_query).result()

		# Reviews embeddings (optional)
		reviews_query = f"""
		INSERT INTO `{project_id}.{dataset}.{embeddings_table}` (product_id, field, embedding, embed_ts)
		SELECT
			p.product_id,
			'review' AS field,
			ML.GENERATE_EMBEDDING(MODEL `{model}`, review) AS embedding,
			CURRENT_TIMESTAMP() AS embed_ts
		FROM `{project_id}.{dataset}.products` p,
		UNNEST(p.reviews) AS review
		WHERE review IS NOT NULL;
		"""
		client.query(reviews_query).result()

def generate_image_embeddings(image_dir, project_id, dataset, embeddings_table, client=None):
		# Load CLIP model and processor
		clip_model_name = "openai/clip-vit-base-patch16"
		device = "cuda" if torch.cuda.is_available() else "cpu"
		model = CLIPModel.from_pretrained(clip_model_name).to(device)
		processor = CLIPProcessor.from_pretrained(clip_model_name)

		image_list = []
		for fname in os.listdir(image_dir):
				if fname.lower().endswith((".jpg", ".jpeg", ".png")):
						product_id = os.path.splitext(fname)[0]
						image_path = os.path.join(image_dir, fname)
						image_list.append((product_id, image_path))

		records = []
		for product_id, image_path in image_list:
				image = Image.open(image_path).convert("RGB")
				inputs = processor(images=image, return_tensors="pt")
				inputs = {k: v.to(device) for k, v in inputs.items()}
				with torch.no_grad():
						image_features = model.get_image_features(**inputs)
				embedding = image_features.cpu().numpy().flatten().tolist()
				records.append({
						"product_id": product_id,
						"field": "image",
						"embedding": embedding,
						"embed_ts": datetime.utcnow()
				})

		df = pd.DataFrame(records)
		if client is None:
				client = bigquery.Client(project=project_id)
		job = client.load_table_from_dataframe(df, f"{project_id}.{dataset}.{embeddings_table}")
		job.result()
		print(f"Uploaded {len(df)} image embeddings to BigQuery table {embeddings_table}.")
