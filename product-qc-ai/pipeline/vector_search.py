"""
Vector Search and Similarity Operations Module

This module provides advanced vector search, similarity analysis, and clustering
operations using the centralized embedding hub. Optimized for product quality
validation and semantic similarity analysis.

Key Features:
- Fast similarity search with multiple distance metrics
- Semantic clustering and product grouping
- Cross-modal similarity (text-image, spec-description)
- Batch operations for efficient processing
- Integration with embedding hub for optimal performance
"""

import logging
import numpy as np
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass
from datetime import datetime
import json
import math

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd

# Clustering imports (with graceful fallback)
try:
    from sklearn.cluster import KMeans, DBSCAN
    from sklearn.metrics.pairwise import cosine_similarity, euclidean_distances
    from sklearn.decomposition import PCA
    from sklearn.manifold import TSNE
    CLUSTERING_AVAILABLE = True
except ImportError:
    CLUSTERING_AVAILABLE = False
    logging.warning("Scikit-learn not available. Advanced clustering features will be disabled.")

logger = logging.getLogger(__name__)

@dataclass
class SimilarityResult:
    """Single similarity search result"""
    content_id: str
    product_id: str
    content_type: str
    similarity_score: float
    distance: float
    original_content: str
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class ClusterResult:
    """Clustering analysis result"""
    cluster_id: int
    cluster_center: List[float]
    cluster_size: int
    cluster_items: List[str]
    cluster_quality: float
    representative_content: str

@dataclass
class VectorSearchConfig:
    """Configuration for vector search operations"""
    similarity_threshold: float = 0.7
    max_results: int = 10
    distance_metric: str = "cosine"  # cosine, euclidean, dot_product
    include_metadata: bool = True
    filter_content_types: Optional[List[str]] = None

class VectorSearchEngine:
    """Advanced vector search and similarity analysis engine"""
    
    def __init__(self, client, project_id: str, dataset_id: str, config: Optional[VectorSearchConfig] = None):
        """
        Initialize the vector search engine
        
        Args:
            client: BigQuery client instance
            project_id: Google Cloud project ID
            dataset_id: BigQuery dataset ID
            config: Optional search configuration
        """
        self.client = client
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.config = config or VectorSearchConfig()
        self.hub_table_id = f"{project_id}.{dataset_id}.embedding_hub"
        
        # Performance tracking
        self.search_count = 0
        self.cache_hits = 0
    
    def find_similar_content(
        self,
        query_embedding: List[float],
        content_types: Optional[List[str]] = None,
        product_ids: Optional[List[str]] = None,
        similarity_threshold: Optional[float] = None,
        max_results: Optional[int] = None,
        exclude_content_ids: Optional[List[str]] = None
    ) -> List[SimilarityResult]:
        """
        Find content similar to the query embedding
        
        Args:
            query_embedding: The embedding vector to search for
            content_types: Optional filter for content types
            product_ids: Optional filter for specific products
            similarity_threshold: Minimum similarity score
            max_results: Maximum number of results
            exclude_content_ids: Content IDs to exclude from results
            
        Returns:
            List of SimilarityResult objects
        """
        threshold = similarity_threshold or self.config.similarity_threshold
        max_res = max_results or self.config.max_results
        
        try:
            # Build query with filters
            where_conditions = ["status = 'ACTIVE'"]
            
            if content_types:
                content_types_str = "', '".join(content_types)
                where_conditions.append(f"content_type IN ('{content_types_str}')")
            
            if product_ids:
                product_ids_str = "', '".join(product_ids)
                where_conditions.append(f"product_id IN ('{product_ids_str}')")
            
            if exclude_content_ids:
                exclude_ids_str = "', '".join(exclude_content_ids)
                where_conditions.append(f"content_id NOT IN ('{exclude_ids_str}')")
            
            where_clause = " AND ".join(where_conditions)
            
            # Use appropriate similarity function based on metric
            if self.config.distance_metric == "cosine":
                similarity_func = f"`{self.project_id}.{self.dataset_id}.cosine_similarity`"
            elif self.config.distance_metric == "euclidean":
                # Convert euclidean distance to similarity (inverse relationship)
                similarity_func = f"1.0 / (1.0 + `{self.project_id}.{self.dataset_id}.euclidean_distance`)"
            else:
                similarity_func = f"`{self.project_id}.{self.dataset_id}.cosine_similarity`"
            
            query = f"""
            WITH query_embedding AS (
                SELECT {query_embedding} as query_emb
            )
            SELECT 
                eh.content_id,
                eh.product_id,
                eh.content_type,
                eh.original_content,
                eh.metadata,
                {similarity_func}(eh.embedding, qe.query_emb) as similarity_score,
                `{self.project_id}.{self.dataset_id}.euclidean_distance`(eh.embedding, qe.query_emb) as distance
            FROM `{self.hub_table_id}` eh
            CROSS JOIN query_embedding qe
            WHERE {where_clause}
              AND {similarity_func}(eh.embedding, qe.query_emb) >= {threshold}
            ORDER BY similarity_score DESC
            LIMIT {max_res}
            """
            
            result = self.client.query(query).result()
            
            similar_items = []
            for row in result:
                metadata = json.loads(row['metadata']) if row['metadata'] else None
                similar_items.append(SimilarityResult(
                    content_id=row['content_id'],
                    product_id=row['product_id'],
                    content_type=row['content_type'],
                    similarity_score=float(row['similarity_score']),
                    distance=float(row['distance']),
                    original_content=row['original_content'],
                    metadata=metadata
                ))
            
            self.search_count += 1
            return similar_items
            
        except Exception as e:
            logger.error(f"Error in similarity search: {str(e)}")
            return []
    
    def find_similar_by_content_id(
        self,
        content_id: str,
        **kwargs
    ) -> List[SimilarityResult]:
        """
        Find content similar to a specific content ID
        
        Args:
            content_id: The content ID to find similar items for
            **kwargs: Additional arguments passed to find_similar_content
            
        Returns:
            List of SimilarityResult objects
        """
        try:
            # Get the embedding for the content ID
            query = f"""
            SELECT embedding
            FROM `{self.hub_table_id}`
            WHERE content_id = @content_id AND status = 'ACTIVE'
            LIMIT 1
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("content_id", "STRING", content_id)
                ]
            )
            
            result = self.client.query(query, job_config=job_config).result()
            rows = list(result)
            
            if not rows:
                logger.warning(f"No embedding found for content_id: {content_id}")
                return []
            
            query_embedding = rows[0]['embedding']
            
            # Exclude the original content from results
            exclude_ids = kwargs.get('exclude_content_ids', [])
            exclude_ids.append(content_id)
            kwargs['exclude_content_ids'] = exclude_ids
            
            return self.find_similar_content(query_embedding, **kwargs)
            
        except Exception as e:
            logger.error(f"Error finding similar content for {content_id}: {str(e)}")
            return []
    
    def cross_modal_similarity(
        self,
        text_content_id: str,
        image_content_ids: List[str],
        similarity_threshold: float = 0.6
    ) -> List[SimilarityResult]:
        """
        Find cross-modal similarities between text and images
        
        Args:
            text_content_id: Text content ID to compare
            image_content_ids: List of image content IDs
            similarity_threshold: Minimum similarity threshold
            
        Returns:
            List of similar image results
        """
        try:
            # Get text embedding
            text_query = f"""
            SELECT embedding
            FROM `{self.hub_table_id}`
            WHERE content_id = @text_content_id AND status = 'ACTIVE'
            LIMIT 1
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("text_content_id", "STRING", text_content_id)
                ]
            )
            
            text_result = self.client.query(text_query, job_config=job_config).result()
            text_rows = list(text_result)
            
            if not text_rows:
                logger.warning(f"No text embedding found for: {text_content_id}")
                return []
            
            text_embedding = text_rows[0]['embedding']
            
            # Compare with image embeddings
            image_ids_str = "', '".join(image_content_ids)
            
            cross_modal_query = f"""
            WITH text_embedding AS (
                SELECT {text_embedding} as text_emb
            )
            SELECT 
                eh.content_id,
                eh.product_id,
                eh.content_type,
                eh.original_content,
                eh.metadata,
                `{self.project_id}.{self.dataset_id}.cosine_similarity`(eh.embedding, te.text_emb) as similarity_score,
                `{self.project_id}.{self.dataset_id}.euclidean_distance`(eh.embedding, te.text_emb) as distance
            FROM `{self.hub_table_id}` eh
            CROSS JOIN text_embedding te
            WHERE eh.content_id IN ('{image_ids_str}')
              AND eh.status = 'ACTIVE'
              AND `{self.project_id}.{self.dataset_id}.cosine_similarity`(eh.embedding, te.text_emb) >= {similarity_threshold}
            ORDER BY similarity_score DESC
            """
            
            result = self.client.query(cross_modal_query).result()
            
            cross_modal_results = []
            for row in result:
                metadata = json.loads(row['metadata']) if row['metadata'] else None
                cross_modal_results.append(SimilarityResult(
                    content_id=row['content_id'],
                    product_id=row['product_id'],
                    content_type=row['content_type'],
                    similarity_score=float(row['similarity_score']),
                    distance=float(row['distance']),
                    original_content=row['original_content'],
                    metadata=metadata
                ))
            
            return cross_modal_results
            
        except Exception as e:
            logger.error(f"Error in cross-modal similarity: {str(e)}")
            return []

# =====================================================
# Legacy function wrappers for backward compatibility
# =====================================================

def text_vector_search(client, project_id, dataset, product_id, top_k=5):
    """
    Legacy function for backward compatibility
    Migrates to use the new VectorSearchEngine approach
    """
    logger.warning("Using legacy text_vector_search function. Consider migrating to VectorSearchEngine.")
    
    search_engine = VectorSearchEngine(client, project_id, dataset)
    
    # Find similar content for the product's description
    content_id = f"{product_id}_description"
    similar_results = search_engine.find_similar_by_content_id(
        content_id,
        max_results=top_k
    )
    
    # Convert to legacy format
    legacy_results = []
    for result in similar_results:
        legacy_results.append({
            'product_id': product_id,
            'similar_product_id': result.product_id,
            'cosine_similarity': result.similarity_score
        })
    
    return legacy_results

# =====================================================
# Utility Functions for Integration
# =====================================================

def find_similar_products(
    client,
    project_id: str,
    dataset_id: str,
    product_id: str,
    content_type: str = "description",
    max_results: int = 5
) -> List[Dict[str, Any]]:
    """
    Convenience function to find products similar to a given product
    
    Args:
        client: BigQuery client
        project_id: Project ID
        dataset_id: Dataset ID
        product_id: Product ID to find similar products for
        content_type: Type of content to compare
        max_results: Maximum number of similar products
        
    Returns:
        List of similar product information
    """
    search_engine = VectorSearchEngine(client, project_id, dataset_id)
    
    content_id = f"{product_id}_{content_type}"
    similar_results = search_engine.find_similar_by_content_id(
        content_id,
        max_results=max_results
    )
    
    return [
        {
            'product_id': result.product_id,
            'content_type': result.content_type,
            'similarity_score': result.similarity_score,
            'original_content': result.original_content[:200] + "..." if len(result.original_content) > 200 else result.original_content
        }
        for result in similar_results
    ]

def analyze_product_consistency(
    client,
    project_id: str,
    dataset_id: str,
    product_id: str
) -> Dict[str, Any]:
    """
    Analyze consistency between different content types for a product
    
    Args:
        client: BigQuery client
        project_id: Project ID
        dataset_id: Dataset ID
        product_id: Product ID to analyze
        
    Returns:
        Consistency analysis results
    """
    search_engine = VectorSearchEngine(client, project_id, dataset_id)
    
    # Find all content for this product
    content_types = ['description', 'specification', 'image']
    
    # Perform cross-comparisons
    consistency_results = {}
    
    for i, content_type_a in enumerate(content_types):
        for j, content_type_b in enumerate(content_types[i+1:], i+1):
            
            content_id_a = f"{product_id}_{content_type_a}"
            content_id_b = f"{product_id}_{content_type_b}"
            
            # Find similarity between the two content types
            similar_results = search_engine.find_similar_by_content_id(
                content_id_a,
                max_results=1,
                exclude_content_ids=[content_id_a]
            )
            
            # Check if the similar result is the expected content
            similarity_score = 0.0
            for result in similar_results:
                if result.content_id == content_id_b:
                    similarity_score = result.similarity_score
                    break
            
            comparison_key = f"{content_type_a}_{content_type_b}"
            consistency_results[comparison_key] = {
                'similarity_score': similarity_score,
                'consistent': similarity_score > 0.7,
                'content_types': [content_type_a, content_type_b]
            }
    
    # Overall consistency assessment
    scores = [result['similarity_score'] for result in consistency_results.values()]
    overall_consistency = np.mean(scores) if scores else 0.0
    
    return {
        'product_id': product_id,
        'overall_consistency_score': overall_consistency,
        'pairwise_comparisons': consistency_results,
        'is_consistent': overall_consistency > 0.7,
        'analysis_timestamp': datetime.now().isoformat()
    }
