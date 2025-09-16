"""
Script to generate and store text embeddings using the centralized embedding hub.
Now uses the enhanced EmbeddingManager with intelligent caching and performance optimization.
"""
import os
import logging
from google.cloud import bigquery
from pipeline.embeddings import EmbeddingManager, batch_embed_products

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Generate text embeddings using the new centralized embedding hub approach"""
    PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "your-gcp-project")
    DATASET = os.environ.get("BQ_DATASET", "product_qc")
    
    # Initialize BigQuery client
    client = bigquery.Client(project=PROJECT_ID)
    
    logger.info("Starting text embeddings generation using EmbeddingManager...")
    
    try:
        # Use the new batch embedding approach
        results = batch_embed_products(
            client=client,
            project_id=PROJECT_ID,
            dataset_id=DATASET,
            include_images=False  # Only text embeddings in this script
        )
        
        # Log results
        logger.info("Text embeddings generation completed!")
        logger.info(f"Products processed: {results['products_processed']}")
        logger.info(f"Descriptions embedded: {results['descriptions_embedded']}")
        logger.info(f"Specifications embedded: {results['specs_embedded']}")
        logger.info(f"Reviews embedded: {results['reviews_embedded']}")
        
        if results['errors']:
            logger.warning(f"Errors encountered: {len(results['errors'])}")
            for error in results['errors'][:5]:  # Show first 5 errors
                logger.error(error)
        
        # Show embedding hub statistics
        if 'embedding_stats' in results:
            stats = results['embedding_stats']
            if 'session_stats' in stats:
                session_stats = stats['session_stats']
                logger.info(f"Cache hit rate: {session_stats['cache_hit_rate']:.2%}")
                logger.info(f"New embeddings generated: {session_stats['generation_count']}")
        
    except Exception as e:
        logger.error(f"Error in text embeddings generation: {str(e)}")
        raise

def run_for_specific_products(product_ids: list):
    """Generate embeddings for specific products only"""
    PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "your-gcp-project")
    DATASET = os.environ.get("BQ_DATASET", "product_qc")
    
    client = bigquery.Client(project=PROJECT_ID)
    
    logger.info(f"Generating embeddings for {len(product_ids)} specific products...")
    
    results = batch_embed_products(
        client=client,
        project_id=PROJECT_ID,
        dataset_id=DATASET,
        product_ids=product_ids,
        include_images=False
    )
    
    logger.info("Specific product embeddings completed!")
    return results

if __name__ == "__main__":
    main()
