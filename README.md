# Product Quality Control Agent

## Overview

This project provides a modular, BigQuery AI-powered pipeline for product data quality validation. It includes unified embedding generation, consistency checks, fusion logic, and unified mismatch scoring, all orchestrated for scalable, real-world and synthetic data evaluation.

## Pipeline Modules & Functions

### 1. `pipeline/embeddings.py`
- **Functions:**
  - `generate_text_embeddings(df, ...)` – Generates text embeddings using BigQuery ML.GENERATE_EMBEDDING.
  - `generate_image_embeddings(df, ...)` – Generates image embeddings (local or via BigQuery if supported).
  - `write_embeddings_to_bq(df, table_name)` – Writes unified embeddings to BigQuery.

### 2. `pipeline/consistency.py`
- **Functions:**
  - `run_consistency_checks(df, ...)` – Runs vector similarity and BigQuery AI-based consistency checks.
  - `write_consistency_results_to_bq(df, table_name)` – Stores check results in BigQuery.

### 3. `pipeline/vector_index.py`
- **Functions:**
  - `create_vector_index(table_name, ...)` – Creates vector indexes in BigQuery for fast similarity search.

### 4. `pipeline/fusion.py`
- **Functions:**
  - `align_products(df, ...)` – Aligns/fuses products using embedding similarity and crosswalk logic.
  - `write_crosswalk_to_bq(df, table_name)` – Stores product crosswalks in BigQuery.

### 5. `pipeline/scoring.py`
- **Functions:**
  - `compute_mismatch_scores(df, ...)` – Computes unified mismatch scores combining vector and rule-based checks.
  - `write_scores_to_bq(df, table_name)` – Writes scores to BigQuery.

## Scripts
- `scripts/generate_text_embeddings.py` – Calls embedding functions for text fields.
- `scripts/generate_image_embeddings.py` – Calls embedding functions for images.
- `scripts/consistency_checks.py` – Runs consistency checks.
- `scripts/create_vector_indexes.py` – Creates vector indexes.
- `scripts/vector_search.py` – Runs vector search queries.

## Workflow
1. **Ingest Data:** Load product data into BigQuery.
2. **Generate Embeddings:** Use pipeline/embeddings.py to create and store unified embeddings.
3. **Create Vector Indexes:** Use pipeline/vector_index.py to index embeddings.
4. **Run Consistency Checks:** Use pipeline/consistency.py for AI-based validation.
5. **Product Fusion:** Use pipeline/fusion.py to align products and create crosswalks.
6. **Mismatch Scoring:** Use pipeline/scoring.py to compute and store scores.
7. **Evaluation:** Use the Jupyter notebook to run the end-to-end pipeline and report metrics.

## BigQuery Cost & Runtime Notes
- **Embedding Generation:**
  - Uses ML.GENERATE_EMBEDDING (text) and/or local/image embedding logic.
  - Cost: Standard BigQuery ML prediction costs per row. Large datasets can incur significant costs.
  - Runtime: Scales with data size; expect several minutes for 10k+ rows.
- **Vector Index Creation:**
  - Cost: Standard storage and compute for index creation.
  - Runtime: Fast for small/medium tables; can take longer for millions of rows.
- **Consistency Checks:**
  - Uses AI.GENERATE_BOOL, AI.GENERATE_TEXT, and vector similarity functions.
  - Cost: Each AI function call incurs BigQuery AI costs; vector similarity is standard SQL compute.
  - Runtime: Typically seconds to minutes per 10k rows.
- **Fusion & Scoring:**
  - Cost: Standard SQL compute for joins, scoring, and crosswalk creation.
  - Runtime: Fast for most use cases; scales with data size.
- **General Advice:**
  - Use test/small datasets for development to minimize costs.
  - Monitor BigQuery job history for cost and performance insights.

## References
- [BigQuery ML Pricing](https://cloud.google.com/bigquery-ml/pricing)
- [BigQuery AI Functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-ai)

---
For more details, see the code in the `pipeline/` and `scripts/` directories and the evaluation notebook in `notebooks/`.
