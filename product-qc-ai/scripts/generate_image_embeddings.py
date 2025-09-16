"""
Generate image embeddings using the centralized embedding hub.
Now supports both directory-based processing and database-driven image embedding generation.
"""
import os
import logging
from google.cloud import bigquery
from pipeline.embeddings import EmbeddingManager, batch_embed_products, generate_image_embeddings

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Generate image embeddings using the new centralized embedding hub approach"""
    PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "your-gcp-project")
    DATASET = os.environ.get("BQ_DATASET", "product_qc")
    IMAGE_DIR = os.environ.get("IMAGE_DIR", "data/images")
    
    # Initialize BigQuery client
    client = bigquery.Client(project=PROJECT_ID)
    
    logger.info("Starting image embeddings generation using EmbeddingManager...")
    
    try:
        # Option 1: Process images from directory (legacy support)
        if os.path.exists(IMAGE_DIR):
            logger.info(f"Processing images from directory: {IMAGE_DIR}")
            generate_image_embeddings(
                image_dir=IMAGE_DIR,
                project_id=PROJECT_ID,
                dataset=DATASET,
                embeddings_table="embedding_hub",  # Use new hub table
                client=client
            )
        
        # Option 2: Process images from database (recommended)
        logger.info("Processing images referenced in database...")
        results = batch_embed_products(
            client=client,
            project_id=PROJECT_ID,
            dataset_id=DATASET,
            include_images=True  # Only process images
        )
        
        # Log results
        logger.info("Image embeddings generation completed!")
        logger.info(f"Products processed: {results['products_processed']}")
        logger.info(f"Images embedded: {results['images_embedded']}")
        
        if results['errors']:
            logger.warning(f"Errors encountered: {len(results['errors'])}")
            for error in results['errors'][:3]:  # Show first 3 errors
                logger.error(error)
        
        # Show embedding hub statistics
        if 'embedding_stats' in results:
            stats = results['embedding_stats']
            if 'session_stats' in stats:
                session_stats = stats['session_stats']
                logger.info(f"Cache hit rate: {session_stats['cache_hit_rate']:.2%}")
                logger.info(f"New embeddings generated: {session_stats['generation_count']}")
        
    except Exception as e:
        logger.error(f"Error in image embeddings generation: {str(e)}")
        raise

def process_single_image(image_path: str, product_id: str):
    """Process a single image file"""
    PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "your-gcp-project")
    DATASET = os.environ.get("BQ_DATASET", "product_qc")
    
    client = bigquery.Client(project=PROJECT_ID)
    manager = EmbeddingManager(client, PROJECT_ID, DATASET)
    
    logger.info(f"Processing single image: {image_path}")
    
    try:
        embedding = manager.generate_image_embedding(
            image_path=image_path,
            content_id=f"{product_id}_image",
            product_id=product_id
        )
        
        if embedding:
            logger.info(f"Successfully generated embedding for {product_id}")
            return True
        else:
            logger.error(f"Failed to generate embedding for {product_id}")
            return False
            
    except Exception as e:
        logger.error(f"Error processing image {image_path}: {str(e)}")
        return False

if __name__ == "__main__":
    main()
