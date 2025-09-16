"""
Advanced vector similarity search using the centralized embedding hub.
Supports multiple search modes, cross-modal similarity, and product consistency analysis.
"""
import os
import json
import logging
from google.cloud import bigquery
from pipeline.vector_search import (
    VectorSearchEngine, 
    find_similar_products, 
    analyze_product_consistency
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Run comprehensive vector search analysis"""
    PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "your-gcp-project")
    DATASET = os.environ.get("BQ_DATASET", "product_qc")
    QUERY_PRODUCT_ID = os.environ.get("QUERY_PRODUCT_ID", "example_product_001")
    SEARCH_MODE = os.environ.get("SEARCH_MODE", "similar_products")  # similar_products, consistency_analysis, cross_modal
    
    client = bigquery.Client(project=PROJECT_ID)
    
    logger.info(f"Starting vector search analysis for product: {QUERY_PRODUCT_ID}")
    
    try:
        if SEARCH_MODE == "similar_products":
            run_similar_products_search(client, PROJECT_ID, DATASET, QUERY_PRODUCT_ID)
        elif SEARCH_MODE == "consistency_analysis":
            run_consistency_analysis(client, PROJECT_ID, DATASET, QUERY_PRODUCT_ID)
        elif SEARCH_MODE == "cross_modal":
            run_cross_modal_search(client, PROJECT_ID, DATASET, QUERY_PRODUCT_ID)
        elif SEARCH_MODE == "advanced":
            run_advanced_search_demo(client, PROJECT_ID, DATASET, QUERY_PRODUCT_ID)
        else:
            logger.error(f"Unknown search mode: {SEARCH_MODE}")
            
    except Exception as e:
        logger.error(f"Error in vector search analysis: {str(e)}")
        raise

def run_similar_products_search(client, project_id: str, dataset: str, product_id: str):
    """Find products similar to the query product"""
    logger.info("Running similar products search...")
    
    # Search by description
    desc_results = find_similar_products(
        client, project_id, dataset, product_id, 
        content_type="description", max_results=5
    )
    
    # Search by specifications
    spec_results = find_similar_products(
        client, project_id, dataset, product_id, 
        content_type="specification", max_results=5
    )
    
    print(f"\n=== Similar Products by Description ===")
    for result in desc_results:
        print(f"Product: {result['product_id']}")
        print(f"Similarity: {result['similarity_score']:.3f}")
        print(f"Content: {result['original_content'][:100]}...")
        print("-" * 50)
    
    print(f"\n=== Similar Products by Specification ===")
    for result in spec_results:
        print(f"Product: {result['product_id']}")
        print(f"Similarity: {result['similarity_score']:.3f}")
        print(f"Content: {result['original_content'][:100]}...")
        print("-" * 50)

def run_consistency_analysis(client, project_id: str, dataset: str, product_id: str):
    """Analyze consistency between different content types for a product"""
    logger.info("Running product consistency analysis...")
    
    consistency_results = analyze_product_consistency(
        client, project_id, dataset, product_id
    )
    
    print(f"\n=== Product Consistency Analysis for {product_id} ===")
    print(f"Overall Consistency Score: {consistency_results['overall_consistency_score']:.3f}")
    print(f"Is Consistent: {consistency_results['is_consistent']}")
    
    print("\nPairwise Comparisons:")
    for comparison, details in consistency_results['pairwise_comparisons'].items():
        print(f"  {comparison}: {details['similarity_score']:.3f} ({'✓' if details['consistent'] else '✗'})")
    
    # Provide recommendations
    if not consistency_results['is_consistent']:
        print("\n🚨 Recommendations:")
        print("- Review content alignment between different modalities")
        print("- Consider updating content to improve consistency")
        print("- Investigate potential quality issues")

def run_cross_modal_search(client, project_id: str, dataset: str, product_id: str):
    """Run cross-modal similarity search between text and images"""
    logger.info("Running cross-modal similarity analysis...")
    
    search_engine = VectorSearchEngine(client, project_id, dataset)
    
    # Find similar images based on description text
    text_content_id = f"{product_id}_description"
    
    # Get all image content IDs (you might want to filter by category)
    query = f"""
    SELECT DISTINCT content_id
    FROM `{project_id}.{dataset}.embedding_hub`
    WHERE content_type = 'image' AND status = 'ACTIVE'
    LIMIT 20
    """
    
    result = client.query(query).result()
    image_content_ids = [row['content_id'] for row in result]
    
    if image_content_ids:
        cross_modal_results = search_engine.cross_modal_similarity(
            text_content_id=text_content_id,
            image_content_ids=image_content_ids,
            similarity_threshold=0.5
        )
        
        print(f"\n=== Cross-Modal Similarity Results ===")
        print(f"Query Text Content: {text_content_id}")
        print(f"Found {len(cross_modal_results)} similar images")
        
        for result in cross_modal_results[:5]:  # Show top 5
            print(f"Image: {result.content_id}")
            print(f"Product: {result.product_id}")
            print(f"Similarity: {result.similarity_score:.3f}")
            print(f"Image Path: {result.original_content}")
            print("-" * 50)
    else:
        print("No image embeddings found for cross-modal analysis")

def run_advanced_search_demo(client, project_id: str, dataset: str, product_id: str):
    """Demonstrate advanced search capabilities"""
    logger.info("Running advanced search demo...")
    
    search_engine = VectorSearchEngine(client, project_id, dataset)
    
    # 1. Find outliers
    print("\n=== Outlier Detection ===")
    outliers = search_engine.find_outliers(
        content_types=['description'],
        outlier_threshold=0.4
    )
    
    print(f"Found {len(outliers)} outlier products:")
    for outlier in outliers[:3]:  # Show top 3 outliers
        print(f"Product: {outlier.product_id}")
        print(f"Outlier Score: {outlier.metadata['outlier_score']:.3f}")
        print(f"Content: {outlier.original_content[:100]}...")
        print("-" * 50)
    
    # 2. Get distribution statistics
    print("\n=== Embedding Distribution Statistics ===")
    stats = search_engine.get_embedding_distribution_stats(['description', 'specification'])
    
    for content_type, type_stats in stats.items():
        if content_type != 'overall':
            print(f"{content_type.title()}:")
            print(f"  Count: {type_stats.get('count', 'N/A')}")
            print(f"  Mean Magnitude: {type_stats.get('mean_magnitude', 0):.3f}")
            print(f"  Sparsity: {type_stats.get('sparsity', 0):.3f}")
    
    # 3. Performance statistics
    print("\n=== Search Performance Statistics ===")
    perf_stats = search_engine.get_search_performance_stats()
    print(f"Total Searches: {perf_stats['total_searches']}")
    print(f"Cache Hit Rate: {perf_stats['cache_hit_rate']:.2%}")

if __name__ == "__main__":
    main()
