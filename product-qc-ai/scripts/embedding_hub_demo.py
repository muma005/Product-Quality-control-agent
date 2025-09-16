"""
Embedding Hub Demo Script

This script demonstrates the advanced capabilities of the centralized embedding hub
including intelligent caching, batch processing, similarity search, and performance monitoring.
"""

import os
import json
import logging
from datetime import datetime
from google.cloud import bigquery
from pipeline.embeddings import EmbeddingManager, EmbeddingConfig, batch_embed_products
from pipeline.vector_search import VectorSearchEngine, find_similar_products

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EmbeddingHubDemo:
    """Comprehensive demo of embedding hub capabilities"""
    
    def __init__(self):
        self.project_id = os.environ.get("BQ_PROJECT_ID", "your-gcp-project")
        self.dataset_id = os.environ.get("BQ_DATASET", "product_qc")
        self.client = bigquery.Client(project=self.project_id)
        
        # Initialize managers
        self.embedding_manager = EmbeddingManager(self.client, self.project_id, self.dataset_id)
        self.search_engine = VectorSearchEngine(self.client, self.project_id, self.dataset_id)
        
        logger.info(f"Initialized demo for project: {self.project_id}, dataset: {self.dataset_id}")
    
    def demo_text_embedding_generation(self):
        """Demonstrate text embedding generation with caching"""
        logger.info("\n" + "="*60)
        logger.info("DEMO 1: Text Embedding Generation with Caching")
        logger.info("="*60)
        
        # Sample product descriptions
        sample_products = [
            {
                'content': 'High-quality wireless Bluetooth headphones with noise cancellation',
                'content_id': 'demo_product_001_description',
                'product_id': 'demo_product_001'
            },
            {
                'content': 'Premium wireless headphones featuring advanced noise cancellation technology',
                'content_id': 'demo_product_002_description', 
                'product_id': 'demo_product_002'
            },
            {
                'content': 'Smartphone with 128GB storage and dual camera system',
                'content_id': 'demo_product_003_description',
                'product_id': 'demo_product_003'
            }
        ]
        
        # Generate embeddings (first time - cache miss)
        logger.info("First generation (cache miss expected):")
        start_time = datetime.now()
        
        for product in sample_products:
            embedding = self.embedding_manager.generate_text_embedding(
                content=product['content'],
                content_type='description',
                content_id=product['content_id'],
                product_id=product['product_id']
            )
            
            if embedding:
                logger.info(f"✓ Generated embedding for {product['product_id']} (dimension: {len(embedding)})")
            else:
                logger.error(f"✗ Failed to generate embedding for {product['product_id']}")
        
        first_generation_time = (datetime.now() - start_time).total_seconds()
        
        # Generate same embeddings again (cache hit expected)
        logger.info("\nSecond generation (cache hit expected):")
        start_time = datetime.now()
        
        for product in sample_products:
            embedding = self.embedding_manager.generate_text_embedding(
                content=product['content'],
                content_type='description',
                content_id=product['content_id'],
                product_id=product['product_id']
            )
            
            if embedding:
                logger.info(f"✓ Retrieved cached embedding for {product['product_id']}")
        
        second_generation_time = (datetime.now() - start_time).total_seconds()
        
        # Show performance improvement
        speedup = first_generation_time / max(second_generation_time, 0.001)
        logger.info(f"\nPerformance Results:")
        logger.info(f"First generation: {first_generation_time:.2f}s")
        logger.info(f"Second generation: {second_generation_time:.2f}s")
        logger.info(f"Speedup: {speedup:.1f}x")
        
        # Show cache statistics
        stats = self.embedding_manager.get_embedding_stats()
        if 'session_stats' in stats:
            session_stats = stats['session_stats']
            logger.info(f"Cache hits: {session_stats['cache_hits']}")
            logger.info(f"Cache misses: {session_stats['cache_misses']}")
            logger.info(f"Hit rate: {session_stats['cache_hit_rate']:.2%}")
    
    def demo_batch_processing(self):
        """Demonstrate efficient batch processing"""
        logger.info("\n" + "="*60)
        logger.info("DEMO 2: Batch Processing Efficiency")
        logger.info("="*60)
        
        # Sample batch data
        batch_items = [
            {'content': f'Sample product description {i}', 'content_id': f'batch_demo_{i}', 'product_id': f'batch_product_{i}'}
            for i in range(1, 11)  # 10 items
        ]
        
        logger.info(f"Processing batch of {len(batch_items)} items...")
        
        start_time = datetime.now()
        results = self.embedding_manager.generate_batch_text_embeddings(
            content_items=batch_items,
            content_type='description'
        )
        processing_time = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"Batch processing completed in {processing_time:.2f}s")
        logger.info(f"Successfully processed: {len(results)}/{len(batch_items)} items")
        logger.info(f"Average time per item: {processing_time/len(batch_items):.3f}s")
        
        # Show some results
        for content_id, embedding in list(results.items())[:3]:
            logger.info(f"✓ {content_id}: embedding dimension {len(embedding)}")
    
    def demo_similarity_search(self):
        """Demonstrate advanced similarity search"""
        logger.info("\n" + "="*60)
        logger.info("DEMO 3: Advanced Similarity Search")
        logger.info("="*60)
        
        # First, ensure we have some embeddings to search
        sample_content = [
            ('demo_headphones_1', 'Wireless Bluetooth headphones with premium sound quality'),
            ('demo_headphones_2', 'Noise-cancelling wireless headphones for music lovers'),
            ('demo_phone_1', 'Latest smartphone with advanced camera features'),
            ('demo_laptop_1', 'High-performance laptop for professional use')
        ]
        
        # Generate embeddings for search demo
        query_embeddings = {}
        for content_id, content in sample_content:
            embedding = self.embedding_manager.generate_text_embedding(
                content=content,
                content_type='description',
                content_id=content_id,
                product_id=content_id.split('_')[1] + '_' + content_id.split('_')[2]
            )
            if embedding:
                query_embeddings[content_id] = embedding
        
        logger.info(f"Generated {len(query_embeddings)} embeddings for similarity search")
        
        # Perform similarity search
        if query_embeddings:
            query_content_id = 'demo_headphones_1'
            logger.info(f"\nSearching for content similar to: {query_content_id}")
            
            similar_results = self.search_engine.find_similar_by_content_id(
                content_id=query_content_id,
                max_results=3,
                similarity_threshold=0.1  # Lower threshold for demo
            )
            
            logger.info(f"Found {len(similar_results)} similar items:")
            for result in similar_results:
                logger.info(f"  - {result.content_id}: similarity {result.similarity_score:.3f}")
                logger.info(f"    Content: {result.original_content[:50]}...")
    
    def demo_cross_modal_search(self):
        """Demonstrate cross-modal similarity search"""
        logger.info("\n" + "="*60)
        logger.info("DEMO 4: Cross-Modal Similarity Search")
        logger.info("="*60)
        
        # This demo requires both text and image embeddings
        # For demo purposes, we'll simulate the scenario
        
        logger.info("Cross-modal search allows finding images similar to text descriptions")
        logger.info("Example: Find product images that match a text description")
        
        # Check if we have any image embeddings
        query = f"""
        SELECT COUNT(*) as image_count
        FROM `{self.project_id}.{self.dataset_id}.embedding_hub`
        WHERE content_type = 'image' AND status = 'ACTIVE'
        """
        
        try:
            result = self.client.query(query).result()
            image_count = list(result)[0]['image_count']
            
            if image_count > 0:
                logger.info(f"Found {image_count} image embeddings in the hub")
                logger.info("Cross-modal search is available!")
                
                # Get a few image content IDs
                image_query = f"""
                SELECT content_id, original_content
                FROM `{self.project_id}.{self.dataset_id}.embedding_hub`
                WHERE content_type = 'image' AND status = 'ACTIVE'
                LIMIT 5
                """
                
                image_result = self.client.query(image_query).result()
                image_content_ids = [row['content_id'] for row in image_result]
                
                if image_content_ids:
                    # Use a text content ID for cross-modal search
                    text_content_id = 'demo_headphones_1'
                    
                    cross_modal_results = self.search_engine.cross_modal_similarity(
                        text_content_id=text_content_id,
                        image_content_ids=image_content_ids,
                        similarity_threshold=0.3
                    )
                    
                    logger.info(f"Cross-modal search results: {len(cross_modal_results)} matches")
                    for result in cross_modal_results[:2]:
                        logger.info(f"  - Image: {result.content_id}")
                        logger.info(f"    Similarity: {result.similarity_score:.3f}")
                        logger.info(f"    Path: {result.original_content}")
                else:
                    logger.info("No image content IDs found for cross-modal demo")
            else:
                logger.info("No image embeddings found. Cross-modal search unavailable.")
                logger.info("Run generate_image_embeddings.py first to enable this feature.")
                
        except Exception as e:
            logger.warning(f"Could not check image embeddings: {str(e)}")
    
    def demo_performance_monitoring(self):
        """Demonstrate performance monitoring and statistics"""
        logger.info("\n" + "="*60)
        logger.info("DEMO 5: Performance Monitoring & Statistics")
        logger.info("="*60)
        
        # Get embedding statistics
        embedding_stats = self.embedding_manager.get_embedding_stats()
        
        logger.info("Embedding Hub Statistics:")
        
        if 'database_stats' in embedding_stats:
            db_stats = embedding_stats['database_stats']
            logger.info(f"Database contains {len(db_stats)} content type/model combinations:")
            
            for stat in db_stats[:5]:  # Show top 5
                logger.info(f"  - {stat['content_type']} ({stat['model_name']}): {stat['total_embeddings']} embeddings")
                logger.info(f"    Avg usage: {stat['avg_usage']:.1f}, Avg quality: {stat['avg_quality']:.3f}")
        
        if 'session_stats' in embedding_stats:
            session_stats = embedding_stats['session_stats']
            logger.info("\nSession Statistics:")
            logger.info(f"  - Cache hits: {session_stats['cache_hits']}")
            logger.info(f"  - Cache misses: {session_stats['cache_misses']}")
            logger.info(f"  - Hit rate: {session_stats['cache_hit_rate']:.2%}")
            logger.info(f"  - Generations: {session_stats['generation_count']}")
        
        # Get search performance statistics
        search_stats = self.search_engine.get_search_performance_stats()
        
        logger.info("\nSearch Engine Statistics:")
        logger.info(f"  - Total searches: {search_stats['total_searches']}")
        logger.info(f"  - Cache hit rate: {search_stats['cache_hit_rate']:.2%}")
        logger.info(f"  - Distance metric: {search_stats['config']['distance_metric']}")
        logger.info(f"  - Similarity threshold: {search_stats['config']['similarity_threshold']}")
    
    def demo_hub_overview(self):
        """Show overall embedding hub status"""
        logger.info("\n" + "="*60)
        logger.info("DEMO 6: Embedding Hub Overview")
        logger.info("="*60)
        
        try:
            # Get hub overview
            overview_query = f"""
            SELECT 
                content_type,
                COUNT(*) as total_embeddings,
                COUNT(DISTINCT product_id) as unique_products,
                AVG(usage_count) as avg_usage,
                MAX(created_timestamp) as latest_embedding
            FROM `{self.project_id}.{self.dataset_id}.embedding_hub`
            WHERE status = 'ACTIVE'
            GROUP BY content_type
            ORDER BY total_embeddings DESC
            """
            
            result = self.client.query(overview_query).result()
            
            logger.info("Embedding Hub Overview:")
            total_embeddings = 0
            
            for row in result:
                total_embeddings += row['total_embeddings']
                logger.info(f"  {row['content_type'].title()}:")
                logger.info(f"    - Embeddings: {row['total_embeddings']}")
                logger.info(f"    - Products: {row['unique_products']}")
                logger.info(f"    - Avg usage: {row['avg_usage']:.1f}")
                logger.info(f"    - Latest: {row['latest_embedding']}")
            
            logger.info(f"\nTotal embeddings in hub: {total_embeddings}")
            
            # Hub health check
            if total_embeddings > 0:
                logger.info("✅ Embedding hub is operational and contains data")
            else:
                logger.info("⚠️  Embedding hub is empty - run embedding generation scripts")
                
        except Exception as e:
            logger.error(f"Error getting hub overview: {str(e)}")
    
    def run_full_demo(self):
        """Run the complete embedding hub demonstration"""
        logger.info("🚀 Starting Comprehensive Embedding Hub Demo")
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        
        try:
            self.demo_hub_overview()
            self.demo_text_embedding_generation()
            self.demo_batch_processing()
            self.demo_similarity_search()
            self.demo_cross_modal_search()
            self.demo_performance_monitoring()
            
            logger.info("\n" + "="*60)
            logger.info("🎉 Demo completed successfully!")
            logger.info("="*60)
            logger.info("\nKey Benefits Demonstrated:")
            logger.info("✅ Intelligent caching reduces computation time")
            logger.info("✅ Batch processing improves efficiency")
            logger.info("✅ Advanced similarity search finds relevant content")
            logger.info("✅ Cross-modal search connects text and images")
            logger.info("✅ Performance monitoring ensures optimal operation")
            logger.info("✅ Centralized hub provides unified embedding management")
            
        except Exception as e:
            logger.error(f"Demo failed with error: {str(e)}")
            raise

def main():
    """Main function to run the demo"""
    demo = EmbeddingHubDemo()
    demo.run_full_demo()

if __name__ == "__main__":
    main()