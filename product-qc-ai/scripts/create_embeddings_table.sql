-- Unified embeddings table for all modalities
CREATE TABLE IF NOT EXISTS `project.product_qc.embeddings` (
  product_id STRING,
  field STRING, -- e.g., title, description, specs, reviews, image
  embedding ARRAY<FLOAT64>,
  embed_ts TIMESTAMP
);