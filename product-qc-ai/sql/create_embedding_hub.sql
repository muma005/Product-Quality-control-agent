-- =====================================================
-- Embedding Hub Table Creation Script
-- =====================================================
-- This script creates the centralized embedding hub table for storing
-- and managing text and image embeddings with optimized performance.

-- Create the main embedding hub table
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.embedding_hub` (
  -- Primary identifiers
  content_id STRING NOT NULL,
  content_type STRING NOT NULL,  -- 'description', 'specification', 'review', 'image_caption', 'image'
  content_hash STRING NOT NULL,  -- SHA256 hash for deduplication
  
  -- Content and embeddings
  original_content STRING,       -- Store original text/image path for reference
  embedding ARRAY<FLOAT64> NOT NULL,
  embedding_dimension INT64,     -- Store dimension for validation
  
  -- Model and version tracking
  model_name STRING NOT NULL,    -- e.g., 'textembedding-gecko@003', 'clip-vit-b-32'
  model_version STRING,          -- Specific model version
  generation_method STRING,      -- 'bigquery_ai', 'local_processing'
  
  -- Metadata and tracking
  product_id STRING,             -- Link to product if applicable
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  usage_count INT64 DEFAULT 0,   -- Track how often this embedding is used
  
  -- Additional metadata
  metadata JSON,                 -- Store additional context as needed
  quality_score FLOAT64,         -- Optional quality assessment of the embedding
  
  -- Status tracking
  status STRING DEFAULT 'ACTIVE', -- 'ACTIVE', 'DEPRECATED', 'ERROR'
  error_message STRING           -- Store any generation errors
)
PARTITION BY DATE(created_timestamp)
CLUSTER BY content_type, content_hash, model_name
OPTIONS (
  description = "Centralized embedding hub for storing and managing text and image embeddings with deduplication and caching",
  labels = [("environment", "production"), ("component", "embedding_hub")]
);

-- Create indexes for optimal query performance
CREATE OR REPLACE TABLE FUNCTION `{PROJECT_ID}.{DATASET_ID}.get_embedding_by_hash`(content_hash STRING, content_type STRING)
AS (
  SELECT *
  FROM `{PROJECT_ID}.{DATASET_ID}.embedding_hub`
  WHERE content_hash = content_hash AND content_type = content_type AND status = 'ACTIVE'
  ORDER BY created_timestamp DESC
  LIMIT 1
);

-- =====================================================
-- Embedding Similarity Functions
-- =====================================================

-- Function to compute cosine similarity between embeddings
CREATE OR REPLACE FUNCTION `{PROJECT_ID}.{DATASET_ID}.cosine_similarity`(
  embedding1 ARRAY<FLOAT64>, 
  embedding2 ARRAY<FLOAT64>
) 
RETURNS FLOAT64
LANGUAGE js AS """
  if (!embedding1 || !embedding2 || embedding1.length !== embedding2.length) {
    return null;
  }
  
  let dotProduct = 0;
  let norm1 = 0;
  let norm2 = 0;
  
  for (let i = 0; i < embedding1.length; i++) {
    dotProduct += embedding1[i] * embedding2[i];
    norm1 += embedding1[i] * embedding1[i];
    norm2 += embedding2[i] * embedding2[i];
  }
  
  if (norm1 === 0 || norm2 === 0) {
    return 0;
  }
  
  return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
""";

-- Function to compute Euclidean distance between embeddings
CREATE OR REPLACE FUNCTION `{PROJECT_ID}.{DATASET_ID}.euclidean_distance`(
  embedding1 ARRAY<FLOAT64>, 
  embedding2 ARRAY<FLOAT64>
) 
RETURNS FLOAT64
LANGUAGE js AS """
  if (!embedding1 || !embedding2 || embedding1.length !== embedding2.length) {
    return null;
  }
  
  let sum = 0;
  for (let i = 0; i < embedding1.length; i++) {
    let diff = embedding1[i] - embedding2[i];
    sum += diff * diff;
  }
  
  return Math.sqrt(sum);
""";

-- =====================================================
-- Embedding Management Views
-- =====================================================

-- View for active embeddings with usage statistics
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.active_embeddings` AS
SELECT 
  content_id,
  content_type,
  content_hash,
  model_name,
  model_version,
  generation_method,
  product_id,
  created_timestamp,
  usage_count,
  quality_score,
  embedding_dimension
FROM `{PROJECT_ID}.{DATASET_ID}.embedding_hub`
WHERE status = 'ACTIVE'
ORDER BY usage_count DESC, created_timestamp DESC;

-- View for embedding statistics by content type and model
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.embedding_stats` AS
SELECT 
  content_type,
  model_name,
  generation_method,
  COUNT(*) as total_embeddings,
  AVG(usage_count) as avg_usage,
  MAX(usage_count) as max_usage,
  AVG(quality_score) as avg_quality,
  MIN(created_timestamp) as first_created,
  MAX(created_timestamp) as last_created,
  COUNT(DISTINCT product_id) as unique_products
FROM `{PROJECT_ID}.{DATASET_ID}.embedding_hub`
WHERE status = 'ACTIVE'
GROUP BY content_type, model_name, generation_method
ORDER BY total_embeddings DESC;

-- =====================================================
-- Batch Operations and Maintenance
-- =====================================================

-- Procedure to update usage count for an embedding
CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.increment_embedding_usage`(
  IN p_content_hash STRING,
  IN p_content_type STRING
)
BEGIN
  UPDATE `{PROJECT_ID}.{DATASET_ID}.embedding_hub`
  SET 
    usage_count = usage_count + 1,
    updated_timestamp = CURRENT_TIMESTAMP()
  WHERE content_hash = p_content_hash 
    AND content_type = p_content_type 
    AND status = 'ACTIVE';
END;

-- Procedure to clean up old or unused embeddings
CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.cleanup_embeddings`(
  IN days_old INT64 DEFAULT 90,
  IN min_usage_count INT64 DEFAULT 1
)
BEGIN
  -- Mark old, unused embeddings as deprecated
  UPDATE `{PROJECT_ID}.{DATASET_ID}.embedding_hub`
  SET 
    status = 'DEPRECATED',
    updated_timestamp = CURRENT_TIMESTAMP()
  WHERE status = 'ACTIVE'
    AND DATE_DIFF(CURRENT_DATE(), DATE(created_timestamp), DAY) > days_old
    AND usage_count < min_usage_count;
    
  -- Optional: Delete deprecated embeddings older than specified period
  -- DELETE FROM `{PROJECT_ID}.{DATASET_ID}.embedding_hub`
  -- WHERE status = 'DEPRECATED'
  --   AND DATE_DIFF(CURRENT_DATE(), DATE(updated_timestamp), DAY) > days_old * 2;
END;

-- =====================================================
-- Sample Queries for Testing
-- =====================================================

-- Find similar embeddings by cosine similarity
/*
WITH target_embedding AS (
  SELECT embedding
  FROM `{PROJECT_ID}.{DATASET_ID}.embedding_hub`
  WHERE content_id = 'your_target_id'
  LIMIT 1
)
SELECT 
  eh.content_id,
  eh.content_type,
  eh.original_content,
  `{PROJECT_ID}.{DATASET_ID}.cosine_similarity`(eh.embedding, te.embedding) as similarity
FROM `{PROJECT_ID}.{DATASET_ID}.embedding_hub` eh
CROSS JOIN target_embedding te
WHERE eh.status = 'ACTIVE'
  AND eh.content_id != 'your_target_id'
ORDER BY similarity DESC
LIMIT 10;
*/

-- Get embedding statistics
/*
SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.embedding_stats`;
*/

-- Check for duplicate content (same hash)
/*
SELECT 
  content_hash,
  content_type,
  COUNT(*) as duplicate_count,
  ARRAY_AGG(content_id) as content_ids
FROM `{PROJECT_ID}.{DATASET_ID}.embedding_hub`
WHERE status = 'ACTIVE'
GROUP BY content_hash, content_type
HAVING COUNT(*) > 1;
*/