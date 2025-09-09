.PHONY: setup load embed index validate correct demo dashboard test

setup:
	pip install -r requirements.txt

load:
	python scripts/load_to_bigquery.py --config config.yaml

embed:
	python -m pipeline.embeddings --config config.yaml

index:
	python -m pipeline.vector_index --config config.yaml

validate:
	python -m pipeline.validation --config config.yaml

correct:
	python -m pipeline.corrections --config config.yaml

synthetic:
	python scripts/generate_synthetic.py --config config.yaml

demo:
	streamlit run app/streamlit_app.py

test:
	pytest -q
