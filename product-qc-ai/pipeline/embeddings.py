
"""
Comprehensive Embedding Management Module

This module provides unified embedding generation, caching, and management
using the centralized embedding hub. Supports both BigQuery AI text embeddings
and local image embeddings with intelligent deduplication and performance optimization.

Key Features:
- Centralized embedding hub with content hash deduplication
- BigQuery AI text embeddings (textembedding-gecko models)
- Local image embeddings (CLIP and other models)
- Intelligent caching and batch processing
- Performance monitoring and optimization
"""

import logging
import hashlib
import json
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass
from datetime import datetime
import base64
import io

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
import numpy as np

# Image processing imports (with graceful fallback)
try:
    from PIL import Image
    import torch
    from transformers import CLIPProcessor, CLIPModel
    from sentence_transformers import SentenceTransformer
    VISION_AVAILABLE = True
except ImportError:
    VISION_AVAILABLE = False
    logging.warning("Vision processing libraries not available. Image embeddings will be disabled.")

logger = logging.getLogger(__name__)

@dataclass
class EmbeddingConfig:
    """Configuration for embedding generation"""
    # BigQuery AI models
    text_model: str = "textembedding-gecko@003"
    multilingual_model: str = "textembedding-gecko-multilingual@001"
    
    # Local image models
    image_model: str = "clip-vit-base-patch32"
    sentence_transformer_model: str = "all-MiniLM-L6-v2"
    
    # Hub configuration
    hub_table: str = "embedding_hub"
    batch_size: int = 100
    cache_enabled: bool = True
    quality_threshold: float = 0.8

class EmbeddingManager:
    """Centralized embedding management with intelligent caching"""
    
    def __init__(self, client, project_id: str, dataset_id: str, config: Optional[EmbeddingConfig] = None):
        """
        Initialize the embedding manager
        
        Args:
            client: BigQuery client instance
            project_id: Google Cloud project ID
            dataset_id: BigQuery dataset ID
            config: Optional embedding configuration
        """
        self.client = client
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.config = config or EmbeddingConfig()
        self.hub_table_id = f"{project_id}.{dataset_id}.{self.config.hub_table}"
        
        # Initialize local models (lazy loading)
        self._clip_model = None
        self._clip_processor = None
        self._sentence_transformer = None
        
        # Performance tracking
        self.cache_hits = 0
        self.cache_misses = 0
        self.generation_count = 0
    
    def _compute_content_hash(self, content: str, content_type: str) -> str:
        """Compute SHA256 hash for content deduplication"""
        hash_input = f"{content_type}:{content}".encode('utf-8')
        return hashlib.sha256(hash_input).hexdigest()
    
    def _get_cached_embedding(self, content_hash: str, content_type: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached embedding from hub"""
        if not self.config.cache_enabled:
            return None
        
        try:
            query = f"""
            SELECT 
                content_id,
                embedding,
                model_name,
                model_version,
                embedding_dimension,
                quality_score,
                usage_count
            FROM `{self.hub_table_id}`
            WHERE content_hash = @content_hash 
              AND content_type = @content_type 
              AND status = 'ACTIVE'
            ORDER BY created_timestamp DESC
            LIMIT 1
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("content_hash", "STRING", content_hash),
                    bigquery.ScalarQueryParameter("content_type", "STRING", content_type)
                ]
            )
            
            result = self.client.query(query, job_config=job_config).result()
            rows = list(result)
            
            if rows:
                self.cache_hits += 1
                # Update usage count
                self._increment_usage_count(content_hash, content_type)
                return dict(rows[0])
            else:
                self.cache_misses += 1
                return None
                
        except Exception as e:
            logger.error(f"Error retrieving cached embedding: {str(e)}")
            self.cache_misses += 1
            return None
    
    def _store_embedding(
        self,
        content_id: str,
        content_type: str,
        content_hash: str,
        original_content: str,
        embedding: List[float],
        model_name: str,
        model_version: str = None,
        generation_method: str = "unknown",
        product_id: str = None,
        quality_score: float = None,
        metadata: Dict[str, Any] = None
    ) -> bool:
        """Store embedding in the centralized hub"""
        try:
            # Prepare the data
            embedding_data = {
                'content_id': content_id,
                'content_type': content_type,
                'content_hash': content_hash,
                'original_content': original_content[:1000],  # Truncate for storage
                'embedding': embedding,
                'embedding_dimension': len(embedding),
                'model_name': model_name,
                'model_version': model_version,
                'generation_method': generation_method,
                'product_id': product_id,
                'quality_score': quality_score,
                'metadata': json.dumps(metadata) if metadata else None,
                'status': 'ACTIVE'
            }
            
            # Insert into BigQuery
            table_ref = self.client.get_table(self.hub_table_id)
            errors = self.client.insert_rows_json(table_ref, [embedding_data])
            
            if errors:
                logger.error(f"Error storing embedding: {errors}")
                return False
            else:
                logger.debug(f"Successfully stored embedding for {content_id}")
                return True
                
        except Exception as e:
            logger.error(f"Error storing embedding in hub: {str(e)}")
            return False
    
    def _increment_usage_count(self, content_hash: str, content_type: str):
        """Increment usage count for cached embedding"""
        try:
            query = f"""
            UPDATE `{self.hub_table_id}`
            SET 
                usage_count = usage_count + 1,
                updated_timestamp = CURRENT_TIMESTAMP()
            WHERE content_hash = @content_hash 
              AND content_type = @content_type 
              AND status = 'ACTIVE'
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("content_hash", "STRING", content_hash),
                    bigquery.ScalarQueryParameter("content_type", "STRING", content_type)
                ]
            )
            
            self.client.query(query, job_config=job_config).result()
            
        except Exception as e:
            logger.warning(f"Could not increment usage count: {str(e)}")
    
    def generate_text_embedding(
        self,
        content: str,
        content_type: str = "text",
        content_id: str = None,
        product_id: str = None,
        model_name: str = None,
        force_regenerate: bool = False
    ) -> Optional[List[float]]:
        """
        Generate text embedding using BigQuery AI with caching
        
        Args:
            content: Text content to embed
            content_type: Type of content (description, specification, review, etc.)
            content_id: Optional unique identifier for the content
            product_id: Optional product ID for linking
            model_name: Optional model override
            force_regenerate: Force regeneration even if cached
            
        Returns:
            List of embedding values or None if failed
        """
        if not content or not content.strip():
            logger.warning("Empty content provided for text embedding")
            return None
        
        # Use provided model or default
        model = model_name or self.config.text_model
        content_hash = self._compute_content_hash(content, content_type)
        content_id = content_id or f"{content_type}_{content_hash[:8]}"
        
        # Check cache first
        if not force_regenerate:
            cached = self._get_cached_embedding(content_hash, content_type)
            if cached and cached.get('model_name') == model:
                logger.debug(f"Using cached embedding for {content_id}")
                return cached['embedding']
        
        # Generate new embedding using BigQuery AI
        try:
            query = f"""
            SELECT ML.GENERATE_EMBEDDING(
                MODEL `{self.project_id}.{self.dataset_id}.{model}`,
                @content
            ) as embedding
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("content", "STRING", content)
                ]
            )
            
            result = self.client.query(query, job_config=job_config).result()
            rows = list(result)
            
            if rows and rows[0]['embedding']:
                embedding = rows[0]['embedding']
                self.generation_count += 1
                
                # Store in hub
                self._store_embedding(
                    content_id=content_id,
                    content_type=content_type,
                    content_hash=content_hash,
                    original_content=content,
                    embedding=embedding,
                    model_name=model,
                    generation_method="bigquery_ai",
                    product_id=product_id,
                    quality_score=self._assess_embedding_quality(embedding)
                )
                
                logger.debug(f"Generated new text embedding for {content_id}")
                return embedding
            else:
                logger.error(f"No embedding returned from BigQuery AI for {content_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error generating text embedding: {str(e)}")
            return None
    
    def generate_image_embedding(
        self,
        image_path: str,
        content_id: str = None,
        product_id: str = None,
        model_name: str = None,
        force_regenerate: bool = False
    ) -> Optional[List[float]]:
        """
        Generate image embedding using local CLIP model with caching
        
        Args:
            image_path: Path to the image file
            content_id: Optional unique identifier
            product_id: Optional product ID for linking
            model_name: Optional model override
            force_regenerate: Force regeneration even if cached
            
        Returns:
            List of embedding values or None if failed
        """
        if not VISION_AVAILABLE:
            logger.error("Vision processing libraries not available")
            return None
        
        # Use provided model or default
        model = model_name or self.config.image_model
        content_hash = self._compute_content_hash(image_path, "image")
        content_id = content_id or f"image_{content_hash[:8]}"
        
        # Check cache first
        if not force_regenerate:
            cached = self._get_cached_embedding(content_hash, "image")
            if cached and cached.get('model_name') == model:
                logger.debug(f"Using cached image embedding for {content_id}")
                return cached['embedding']
        
        # Generate new embedding
        try:
            # Initialize CLIP model if needed
            if self._clip_model is None:
                self._clip_model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32")
                self._clip_processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")
            
            # Load and process image
            image = Image.open(image_path).convert('RGB')
            inputs = self._clip_processor(images=image, return_tensors="pt")
            
            # Generate embedding
            with torch.no_grad():
                image_features = self._clip_model.get_image_features(**inputs)
                embedding = image_features.squeeze().numpy().tolist()
            
            self.generation_count += 1
            
            # Store in hub
            self._store_embedding(
                content_id=content_id,
                content_type="image",
                content_hash=content_hash,
                original_content=image_path,
                embedding=embedding,
                model_name=model,
                generation_method="local_processing",
                product_id=product_id,
                quality_score=self._assess_embedding_quality(embedding)
            )
            
            logger.debug(f"Generated new image embedding for {content_id}")
            return embedding
            
        except Exception as e:
            logger.error(f"Error generating image embedding: {str(e)}")
            return None
    
    def generate_batch_text_embeddings(
        self,
        content_items: List[Dict[str, Any]],
        content_type: str = "text",
        model_name: str = None
    ) -> Dict[str, List[float]]:
        """
        Generate embeddings for multiple text items efficiently
        
        Args:
            content_items: List of dicts with 'content', 'content_id', 'product_id'
            content_type: Type of content
            model_name: Optional model override
            
        Returns:
            Dictionary mapping content_id to embedding
        """
        model = model_name or self.config.text_model
        results = {}
        
        # Separate cached and new items
        new_items = []
        for item in content_items:
            content = item.get('content', '')
            content_id = item.get('content_id', f"{content_type}_{hashlib.sha256(content.encode()).hexdigest()[:8]}")
            content_hash = self._compute_content_hash(content, content_type)
            
            # Check cache
            cached = self._get_cached_embedding(content_hash, content_type)
            if cached and cached.get('model_name') == model:
                results[content_id] = cached['embedding']
            else:
                new_items.append({
                    **item,
                    'content_id': content_id,
                    'content_hash': content_hash
                })
        
        # Process new items in batches
        for i in range(0, len(new_items), self.config.batch_size):
            batch = new_items[i:i + self.config.batch_size]
            batch_results = self._generate_batch_bigquery_embeddings(batch, model, content_type)
            results.update(batch_results)
        
        return results
    
    def _generate_batch_bigquery_embeddings(
        self,
        batch_items: List[Dict[str, Any]],
        model: str,
        content_type: str
    ) -> Dict[str, List[float]]:
        """Generate embeddings for a batch using BigQuery AI"""
        if not batch_items:
            return {}
        
        try:
            # Prepare batch query
            content_values = [f"('{item['content_id']}', '{item['content']}')" 
                            for item in batch_items]
            content_table = f"VALUES {', '.join(content_values)} AS t(content_id, content)"
            
            query = f"""
            WITH content_data AS (
                SELECT * FROM {content_table}
            )
            SELECT 
                content_id,
                content,
                ML.GENERATE_EMBEDDING(
                    MODEL `{self.project_id}.{self.dataset_id}.{model}`,
                    content
                ) as embedding
            FROM content_data
            """
            
            result = self.client.query(query).result()
            batch_results = {}
            
            for row in result:
                if row['embedding']:
                    content_id = row['content_id']
                    embedding = row['embedding']
                    batch_results[content_id] = embedding
                    
                    # Find the original item for metadata
                    original_item = next((item for item in batch_items 
                                       if item['content_id'] == content_id), None)
                    
                    if original_item:
                        # Store in hub
                        self._store_embedding(
                            content_id=content_id,
                            content_type=content_type,
                            content_hash=original_item['content_hash'],
                            original_content=original_item['content'],
                            embedding=embedding,
                            model_name=model,
                            generation_method="bigquery_ai",
                            product_id=original_item.get('product_id'),
                            quality_score=self._assess_embedding_quality(embedding)
                        )
            
            self.generation_count += len(batch_results)
            return batch_results
            
        except Exception as e:
            logger.error(f"Error in batch embedding generation: {str(e)}")
            return {}
    
    def _assess_embedding_quality(self, embedding: List[float]) -> float:
        """Assess the quality of an embedding based on various metrics"""
        if not embedding:
            return 0.0
        
        try:
            # Convert to numpy for calculations
            emb_array = np.array(embedding)
            
            # Calculate various quality metrics
            magnitude = np.linalg.norm(emb_array)
            sparsity = np.count_nonzero(emb_array) / len(emb_array)
            variance = np.var(emb_array)
            
            # Simple quality score (can be made more sophisticated)
            quality_score = min(1.0, (magnitude * sparsity * variance) / 100)
            return max(0.0, quality_score)
            
        except Exception as e:
            logger.warning(f"Error assessing embedding quality: {str(e)}")
            return 0.5  # Default middle quality
    
    def get_embedding_stats(self) -> Dict[str, Any]:
        """Get performance and usage statistics"""
        try:
            query = f"""
            SELECT 
                content_type,
                model_name,
                generation_method,
                COUNT(*) as total_embeddings,
                AVG(usage_count) as avg_usage,
                AVG(quality_score) as avg_quality,
                MIN(created_timestamp) as first_created,
                MAX(created_timestamp) as last_created
            FROM `{self.hub_table_id}`
            WHERE status = 'ACTIVE'
            GROUP BY content_type, model_name, generation_method
            ORDER BY total_embeddings DESC
            """
            
            result = self.client.query(query).result()
            db_stats = [dict(row) for row in result]
            
            return {
                'database_stats': db_stats,
                'session_stats': {
                    'cache_hits': self.cache_hits,
                    'cache_misses': self.cache_misses,
                    'cache_hit_rate': self.cache_hits / (self.cache_hits + self.cache_misses) if (self.cache_hits + self.cache_misses) > 0 else 0,
                    'generation_count': self.generation_count
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting embedding stats: {str(e)}")
            return {'error': str(e)}
    
    def cleanup_old_embeddings(self, days_old: int = 90, min_usage_count: int = 1):
        """Clean up old, unused embeddings"""
        try:
            query = f"""
            CALL `{self.project_id}.{self.dataset_id}.cleanup_embeddings`({days_old}, {min_usage_count})
            """
            
            self.client.query(query).result()
            logger.info(f"Cleanup procedure executed for embeddings older than {days_old} days with usage < {min_usage_count}")
            
        except Exception as e:
            logger.error(f"Error during embedding cleanup: {str(e)}")

# =====================================================
# Legacy function wrappers for backward compatibility
# =====================================================

def generate_text_embeddings(client, project_id, dataset, model, embeddings_table):
    """
    Legacy function for backward compatibility
    Migrates to use the new EmbeddingManager approach
    """
    logger.warning("Using legacy generate_text_embeddings function. Consider migrating to EmbeddingManager.")
    
    manager = EmbeddingManager(client, project_id, dataset)
    
    # Get all products
    query = f"""
    SELECT product_id, description, specs, reviews
    FROM `{project_id}.{dataset}.products`
    WHERE description IS NOT NULL OR specs IS NOT NULL
    """
    
    result = client.query(query).result()
    
    for row in result:
        product_id = row['product_id']
        
        # Process description
        if row['description']:
            manager.generate_text_embedding(
                content=row['description'],
                content_type='description',
                content_id=f"{product_id}_description",
                product_id=product_id
            )
        
        # Process specifications
        if row['specs']:
            specs_content = json.dumps(row['specs']) if isinstance(row['specs'], dict) else str(row['specs'])
            manager.generate_text_embedding(
                content=specs_content,
                content_type='specification',
                content_id=f"{product_id}_specs",
                product_id=product_id
            )
        
        # Process reviews
        if row['reviews']:
            for i, review in enumerate(row['reviews']):
                if review:
                    manager.generate_text_embedding(
                        content=review,
                        content_type='review',
                        content_id=f"{product_id}_review_{i}",
                        product_id=product_id
                    )

def generate_image_embeddings(image_dir, project_id, dataset, embeddings_table, client=None):
    """
    Legacy function for backward compatibility - processes images from directory
    Migrates to use the new EmbeddingManager approach
    """
    logger.warning("Using legacy generate_image_embeddings directory function. Consider migrating to EmbeddingManager.")
    
    if not VISION_AVAILABLE:
        logger.error("Vision processing libraries not available for image embeddings")
        return
    
    if client is None:
        client = bigquery.Client(project=project_id)
    
    manager = EmbeddingManager(client, project_id, dataset)
    
    # Process images from directory
    import os
    image_list = []
    for fname in os.listdir(image_dir):
        if fname.lower().endswith((".jpg", ".jpeg", ".png")):
            product_id = os.path.splitext(fname)[0]
            image_path = os.path.join(image_dir, fname)
            image_list.append((product_id, image_path))
    
    processed_count = 0
    for product_id, image_path in image_list:
        try:
            manager.generate_image_embedding(
                image_path=image_path,
                content_id=f"{product_id}_image",
                product_id=product_id
            )
            processed_count += 1
        except Exception as e:
            logger.error(f"Error processing image for product {product_id}: {e}")
    
    print(f"Processed {processed_count} image embeddings using new EmbeddingManager.")

# =====================================================
# Utility Functions
# =====================================================

def get_embedding_for_content(
    client,
    project_id: str,
    dataset_id: str,
    content: str,
    content_type: str = "text",
    model_name: str = None
) -> Optional[List[float]]:
    """
    Convenience function to get embedding for any content
    
    Args:
        client: BigQuery client
        project_id: Project ID
        dataset_id: Dataset ID
        content: Content to embed
        content_type: Type of content
        model_name: Optional model override
        
    Returns:
        Embedding vector or None
    """
    manager = EmbeddingManager(client, project_id, dataset_id)
    return manager.generate_text_embedding(content, content_type, model_name=model_name)

def batch_embed_products(
    client,
    project_id: str,
    dataset_id: str,
    product_ids: List[str] = None,
    include_images: bool = True
) -> Dict[str, Any]:
    """
    Batch embed all product data (descriptions, specs, reviews, images)
    
    Args:
        client: BigQuery client
        project_id: Project ID
        dataset_id: Dataset ID
        product_ids: Optional list of specific product IDs
        include_images: Whether to process images
        
    Returns:
        Summary of embedding generation results
    """
    manager = EmbeddingManager(client, project_id, dataset_id)
    
    # Build query with optional product filter
    where_clause = ""
    if product_ids:
        product_list = "', '".join(product_ids)
        where_clause = f"WHERE product_id IN ('{product_list}')"
    
    query = f"""
    SELECT product_id, description, specs, reviews, image_path
    FROM `{project_id}.{dataset_id}.products`
    {where_clause}
    """
    
    result = client.query(query).result()
    stats = {
        'products_processed': 0,
        'descriptions_embedded': 0,
        'specs_embedded': 0,
        'reviews_embedded': 0,
        'images_embedded': 0,
        'errors': []
    }
    
    for row in result:
        product_id = row['product_id']
        stats['products_processed'] += 1
        
        try:
            # Process description
            if row['description']:
                manager.generate_text_embedding(
                    content=row['description'],
                    content_type='description',
                    content_id=f"{product_id}_description",
                    product_id=product_id
                )
                stats['descriptions_embedded'] += 1
            
            # Process specifications
            if row['specs']:
                specs_content = json.dumps(row['specs']) if isinstance(row['specs'], dict) else str(row['specs'])
                manager.generate_text_embedding(
                    content=specs_content,
                    content_type='specification',
                    content_id=f"{product_id}_specs",
                    product_id=product_id
                )
                stats['specs_embedded'] += 1
            
            # Process reviews
            if row['reviews']:
                for i, review in enumerate(row['reviews']):
                    if review:
                        manager.generate_text_embedding(
                            content=review,
                            content_type='review',
                            content_id=f"{product_id}_review_{i}",
                            product_id=product_id
                        )
                        stats['reviews_embedded'] += 1
            
            # Process image
            if include_images and row['image_path'] and VISION_AVAILABLE:
                manager.generate_image_embedding(
                    image_path=row['image_path'],
                    content_id=f"{product_id}_image",
                    product_id=product_id
                )
                stats['images_embedded'] += 1
                
        except Exception as e:
            error_msg = f"Error processing product {product_id}: {str(e)}"
            logger.error(error_msg)
            stats['errors'].append(error_msg)
    
    # Add manager stats
    stats['embedding_stats'] = manager.get_embedding_stats()
    
    return stats
