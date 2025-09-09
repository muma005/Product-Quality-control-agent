# Product Data QC with BigQuery AI

A multimodal Product Quality Control system leveraging BigQuery AI, BigFrames, and Streamlit for automated validation, correction, and business impact analysis.

## Minimal Run Steps

1. `make setup`
2. Fill `config.yaml` with your project/buckets.
3. Put sample CSVs in `data/processed/` or run `make synthetic`.
4. `make load && make embed && make index && make validate && make correct`
5. `make demo` (opens Streamlit)
6. (Optional) connect Looker Studio to BigQuery dataset for dashboards.
