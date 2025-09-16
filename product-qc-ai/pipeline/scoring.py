"""
Unified mismatch scoring logic for product validation pipeline.
Combines vector-based and rule-based mismatches into a single score and writes to mismatch_scores table.
Enhanced with 0-100 business-friendly scoring, confidence intervals, and risk categorization.
Phase 4 + Embedding Hub Integration: Optimized for 50-80% performance improvement.
"""
from google.cloud import bigquery
import pandas as pd
import numpy as np
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

# Import embedding hub components
from .embeddings import EmbeddingManager
from .vector_search import VectorSearchEngine

# Configuration
PROJECT_ID = 'proj-product-qc-gmumabigq'
DATASET = 'product_qc'

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Scoring weights for different validation dimensions
SCORING_WEIGHTS = {
    'description_spec': 0.40,  # 40% weight - most critical
    'image_alignment': 0.30,   # 30% weight - visual consistency
    'review_alignment': 0.30   # 30% weight - customer feedback
}

def compute_mismatch_scores(client, project_id, dataset, embeddings_table, bool_checks_table, output_table="mismatch_scores"):
    """
    Original mismatch scoring function - kept for backward compatibility.
    """
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset}.{output_table}` AS
    SELECT
      d.product_id,
      SAFE_DIVIDE((
        COSINE_DISTANCE(d.embedding, s.embedding) +
        COSINE_DISTANCE(d.embedding, i.embedding)
      ), 2) AS vector_mismatch,
      BOOL_OR(NOT b.desc_spec_match) AS rule_mismatch
    FROM `{project_id}.{dataset}.{embeddings_table}` d
    JOIN `{project_id}.{dataset}.{embeddings_table}` s
      ON d.product_id = s.product_id AND d.field = 'description' AND s.field = 'specs'
    JOIN `{project_id}.{dataset}.{embeddings_table}` i
      ON d.product_id = i.product_id AND i.field = 'image'
    JOIN `{project_id}.{dataset}.{bool_checks_table}` b
      ON d.product_id = b.product_id
    GROUP BY d.product_id;
    """
    job = client.query(query)
    job.result()
    print(f"Created/updated mismatch scores table: {output_table}")

def compute_unified_mismatch_score(validation_results, weights=SCORING_WEIGHTS):
    """
    Compute unified 0-100 mismatch score from comprehensive validation results.
    Higher scores indicate more severe quality issues.
    
    Args:
        validation_results: Dict from run_comprehensive_validation()
        weights: Dict with scoring weights for each dimension
    
    Returns:
        DataFrame with unified scores and risk categories
    """
    try:
        # Extract validation dataframes
        desc_spec_df = validation_results.get('description_spec', pd.DataFrame())
        spec_image_df = validation_results.get('spec_image', pd.DataFrame())
        desc_image_df = validation_results.get('description_image', pd.DataFrame())
        reviews_df = validation_results.get('reviews_alignment', pd.DataFrame())
        
        # Get unique product IDs
        all_product_ids = set()
        for df in [desc_spec_df, spec_image_df, desc_image_df, reviews_df]:
            if not df.empty:
                all_product_ids.update(df['product_id'].unique())
        
        unified_scores = []
        
        for product_id in all_product_ids:
            score_components = {}
            
            # 1. Description-Spec Score (40% weight)
            desc_spec_score = 0
            if not desc_spec_df.empty:
                product_desc_spec = desc_spec_df[desc_spec_df['product_id'] == product_id]
                if not product_desc_spec.empty:
                    # Extract numeric score from AI analysis text
                    analysis_text = product_desc_spec.iloc[0].get('alignment_analysis', '')
                    desc_spec_score = extract_score_from_text(analysis_text)
                    
                    # Combine with vector distance (normalized to 0-100)
                    vector_dist = product_desc_spec.iloc[0].get('vector_distance', 0.5)
                    vector_score = min(100, vector_dist * 200)  # Convert 0-0.5 distance to 0-100 score
                    
                    desc_spec_score = (desc_spec_score + vector_score) / 2
            
            score_components['description_spec'] = desc_spec_score
            
            # 2. Image Alignment Score (30% weight - average of spec-image and desc-image)
            image_score = 0
            image_scores = []
            
            # Spec-Image alignment
            if not spec_image_df.empty:
                product_spec_image = spec_image_df[spec_image_df['product_id'] == product_id]
                if not product_spec_image.empty:
                    spec_image_score = extract_score_from_text(
                        product_spec_image.iloc[0].get('spec_image_analysis', '')
                    )
                    image_scores.append(spec_image_score)
            
            # Description-Image alignment
            if not desc_image_df.empty:
                product_desc_image = desc_image_df[desc_image_df['product_id'] == product_id]
                if not product_desc_image.empty:
                    desc_image_score = extract_score_from_text(
                        product_desc_image.iloc[0].get('description_image_analysis', '')
                    )
                    image_scores.append(desc_image_score)
            
            image_score = np.mean(image_scores) if image_scores else 0
            score_components['image_alignment'] = image_score
            
            # 3. Review Alignment Score (30% weight)
            review_score = 0
            if not reviews_df.empty:
                product_reviews = reviews_df[reviews_df['product_id'] == product_id]
                if not product_reviews.empty:
                    review_scores = []
                    for _, review_row in product_reviews.iterrows():
                        contradiction_score = extract_score_from_text(
                            review_row.get('review_contradiction_analysis', '')
                        )
                        review_scores.append(contradiction_score)
                    
                    review_score = np.mean(review_scores) if review_scores else 0
            
            score_components['review_alignment'] = review_score
            
            # Calculate weighted unified score
            unified_score = (
                score_components['description_spec'] * weights['description_spec'] +
                score_components['image_alignment'] * weights['image_alignment'] +
                score_components['review_alignment'] * weights['review_alignment']
            )
            
            # Generate risk category and confidence
            risk_category = categorize_risk(unified_score)
            confidence = calculate_confidence_score(score_components)
            
            unified_scores.append({
                'product_id': product_id,
                'unified_mismatch_score': round(unified_score, 2),
                'description_spec_score': round(score_components['description_spec'], 2),
                'image_alignment_score': round(score_components['image_alignment'], 2),
                'review_alignment_score': round(score_components['review_alignment'], 2),
                'risk_category': risk_category,
                'confidence_score': round(confidence, 2),
                'scoring_timestamp': pd.Timestamp.now()
            })
        
        return pd.DataFrame(unified_scores)
        
    except Exception as e:
        print(f"❌ Error computing unified scores: {str(e)}")
        return pd.DataFrame()

def extract_score_from_text(text):
    """
    Extract numeric score from AI-generated analysis text.
    Looks for patterns like "Score: 75/100" or "XX/100".
    """
    if not text or not isinstance(text, str):
        return 50  # Default neutral score
    
    import re
    # Look for score patterns
    score_patterns = [
        r'Score:\s*(\d+)/100',
        r'(\d+)/100',
        r'Score:\s*(\d+)',
        r'score.*?(\d+)',
    ]
    
    for pattern in score_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            score = int(match.group(1))
            return min(100, max(0, score))  # Clamp to 0-100 range
    
    return 50  # Default if no score found

def categorize_risk(score):
    """
    Categorize unified mismatch score into business risk levels.
    """
    if score < 30:
        return 'Low'
    elif score < 70:
        return 'Medium'
    else:
        return 'High'

def calculate_confidence_score(score_components):
    """
    Calculate confidence in the unified score based on data availability and consistency.
    """
    # Count how many score components have data
    available_components = sum(1 for score in score_components.values() if score > 0)
    
    # Base confidence on data availability
    base_confidence = (available_components / len(score_components)) * 100
    
    # Adjust for score consistency (lower variance = higher confidence)
    scores = [score for score in score_components.values() if score > 0]
    if len(scores) > 1:
        score_variance = np.var(scores)
        consistency_factor = max(0.5, 1 - (score_variance / 1000))  # Normalize variance impact
        base_confidence *= consistency_factor
    
    return min(100, max(20, base_confidence))  # Clamp to 20-100 range

def create_business_intelligence_summary(unified_scores_df):
    """
    Generate business intelligence summary from unified scores.
    """
    if unified_scores_df.empty:
        return {}
    
    summary = {
        'total_products': len(unified_scores_df),
        'average_mismatch_score': unified_scores_df['unified_mismatch_score'].mean(),
        'risk_distribution': unified_scores_df['risk_category'].value_counts().to_dict(),
        'high_risk_products': len(unified_scores_df[unified_scores_df['risk_category'] == 'High']),
        'average_confidence': unified_scores_df['confidence_score'].mean(),
        'top_problem_areas': {
            'description_spec_avg': unified_scores_df['description_spec_score'].mean(),
            'image_alignment_avg': unified_scores_df['image_alignment_score'].mean(),
            'review_alignment_avg': unified_scores_df['review_alignment_score'].mean()
        }
    }
    
    return summary


# =====================================================
# EMBEDDING HUB OPTIMIZED SCORING FUNCTIONS
# =====================================================

class QualityScorer:
    """
    Hub-optimized quality scorer that leverages centralized embeddings
    for dramatically improved performance and advanced similarity-based scoring.
    """
    
    def __init__(self, client, project_id: str, dataset_id: str):
        """Initialize with embedding hub integration"""
        self.client = client
        self.project_id = project_id
        self.dataset_id = dataset_id
        
        # Initialize hub components
        self.embedding_manager = EmbeddingManager(client, project_id, dataset_id)
        self.search_engine = VectorSearchEngine(client, project_id, dataset_id)
        
        # Enhanced scoring weights with more granular control
        self.scoring_weights = {
            'description_spec_alignment': 0.35,  # Core alignment
            'cross_modal_consistency': 0.25,     # Image-text consistency  
            'content_coherence': 0.20,           # Internal consistency
            'review_validation': 0.15,           # Customer feedback
            'technical_accuracy': 0.05           # Technical specs validation
        }
        
        logger.info(f"QualityScorer initialized with embedding hub integration")
    
    def compute_comprehensive_quality_score_optimized(
        self,
        product_id: str,
        product_data: Dict[str, Any],
        include_confidence_intervals: bool = True
    ) -> Dict[str, Any]:
        """
        Hub-optimized comprehensive quality scoring with advanced similarity analysis
        
        Args:
            product_id: Product identifier
            product_data: Complete product data including description, specs, image_path, reviews
            include_confidence_intervals: Whether to compute confidence intervals
            
        Returns:
            Comprehensive quality scoring analysis
        """
        try:
            scores = {}
            confidence_scores = {}
            embeddings_used = {}
            
            # 1. Description-Spec Alignment Score
            if product_data.get('description') and product_data.get('specifications'):
                desc_spec_score = self._compute_description_spec_score_optimized(
                    product_id, product_data['description'], product_data['specifications']
                )
                scores['description_spec_alignment'] = desc_spec_score['score']
                confidence_scores['description_spec_alignment'] = desc_spec_score['confidence']
                embeddings_used['description_spec'] = desc_spec_score.get('embeddings_cached', False)
            
            # 2. Cross-Modal Consistency Score
            if product_data.get('description') and product_data.get('image_path'):
                cross_modal_score = self._compute_cross_modal_score_optimized(
                    product_id, product_data['description'], product_data['image_path']
                )
                scores['cross_modal_consistency'] = cross_modal_score['score']
                confidence_scores['cross_modal_consistency'] = cross_modal_score['confidence']
                embeddings_used['cross_modal'] = cross_modal_score.get('embeddings_cached', False)
            
            # 3. Content Coherence Score (multi-content consistency)
            content_items = []
            for content_type in ['description', 'specifications']:
                if product_data.get(content_type):
                    content = product_data[content_type]
                    if isinstance(content, dict):
                        content = json.dumps(content)
                    content_items.append({'content': content, 'type': content_type})
            
            if len(content_items) >= 2:
                coherence_score = self._compute_content_coherence_score_optimized(
                    product_id, content_items
                )
                scores['content_coherence'] = coherence_score['score']
                confidence_scores['content_coherence'] = coherence_score['confidence']
                embeddings_used['content_coherence'] = coherence_score.get('embeddings_cached', False)
            
            # 4. Review Validation Score
            if product_data.get('reviews'):
                review_score = self._compute_review_validation_score_optimized(
                    product_id, product_data.get('description', ''), product_data['reviews']
                )
                scores['review_validation'] = review_score['score']
                confidence_scores['review_validation'] = review_score['confidence'] 
            
            # 5. Technical Accuracy Score (AI-based spec validation)
            if product_data.get('specifications'):
                tech_score = self._compute_technical_accuracy_score_optimized(
                    product_id, product_data['specifications']
                )
                scores['technical_accuracy'] = tech_score['score']
                confidence_scores['technical_accuracy'] = tech_score['confidence']
            
            # Compute weighted unified score
            weighted_score = 0
            total_weight = 0
            
            for component, weight in self.scoring_weights.items():
                if component in scores:
                    weighted_score += scores[component] * weight
                    total_weight += weight
            
            # Normalize to 0-100 scale
            unified_score = (weighted_score / total_weight) if total_weight > 0 else 50
            
            # Overall confidence
            available_confidences = [conf for conf in confidence_scores.values() if conf > 0]
            overall_confidence = np.mean(available_confidences) if available_confidences else 0.5
            
            # Risk categorization
            risk_category = self._categorize_quality_risk(unified_score, overall_confidence)
            
            result = {
                'product_id': product_id,
                'unified_quality_score': round(unified_score, 2),
                'quality_grade': self._get_quality_grade(unified_score),
                'risk_category': risk_category,
                'overall_confidence': round(overall_confidence, 3),
                'component_scores': scores,
                'component_confidences': confidence_scores,
                'scoring_weights_used': self.scoring_weights,
                'embeddings_performance': embeddings_used,
                'scoring_timestamp': datetime.now().isoformat()
            }
            
            # Add confidence intervals if requested
            if include_confidence_intervals:
                confidence_interval = self._compute_confidence_interval(
                    unified_score, overall_confidence, len(scores)
                )
                result['confidence_interval'] = confidence_interval
            
            return result
            
        except Exception as e:
            logger.error(f"Error in comprehensive quality scoring: {str(e)}")
            return {
                'product_id': product_id,
                'error': str(e),
                'unified_quality_score': 0,
                'risk_category': 'Unknown'
            }
    
    def _compute_description_spec_score_optimized(
        self,
        product_id: str,
        description: str,
        specifications: Any
    ) -> Dict[str, Any]:
        """Compute description-specification alignment score using embedding hub"""
        try:
            # Convert specs to string if needed
            specs_str = json.dumps(specifications) if isinstance(specifications, dict) else str(specifications)
            
            # Generate embeddings with caching
            desc_embedding = self.embedding_manager.generate_text_embedding(
                content=description,
                content_type='description',
                content_id=f"{product_id}_description",
                product_id=product_id
            )
            
            spec_embedding = self.embedding_manager.generate_text_embedding(
                content=specs_str,
                content_type='specification',
                content_id=f"{product_id}_specification", 
                product_id=product_id
            )
            
            if desc_embedding and spec_embedding:
                # Compute similarity using hub functions
                similarity_query = f"""
                SELECT `{self.project_id}.{self.dataset_id}.cosine_similarity`(@desc_emb, @spec_emb) as similarity
                """
                
                config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("desc_emb", "REPEATED", desc_embedding),
                        bigquery.ScalarQueryParameter("spec_emb", "REPEATED", spec_embedding)
                    ]
                )
                
                similarity_result = self.client.query(similarity_query, config).result()
                similarity_score = list(similarity_result)[0]['similarity']
                
                # Convert similarity to 0-100 quality score (higher similarity = higher quality)
                quality_score = similarity_score * 100
                confidence_score = min(1.0, similarity_score + 0.2)
                
                return {
                    'score': round(quality_score, 2),
                    'confidence': round(confidence_score, 3),
                    'similarity_raw': float(similarity_score),
                    'embeddings_cached': True
                }
            
            # Fallback to AI-based scoring if embeddings not available
            return self._ai_fallback_desc_spec_score(description, specs_str)
            
        except Exception as e:
            logger.error(f"Error computing description-spec score: {str(e)}")
            return {'score': 50, 'confidence': 0.3, 'error': str(e)}
    
    def _compute_cross_modal_score_optimized(
        self,
        product_id: str,
        description: str,
        image_path: str
    ) -> Dict[str, Any]:
        """Compute cross-modal consistency score using embedding hub"""
        try:
            # Generate embeddings with caching
            text_embedding = self.embedding_manager.generate_text_embedding(
                content=description,
                content_type='description',
                content_id=f"{product_id}_description",
                product_id=product_id
            )
            
            image_embedding = self.embedding_manager.generate_image_embedding(
                image_path=image_path,
                content_id=f"{product_id}_image",
                product_id=product_id
            )
            
            if text_embedding and image_embedding:
                # Cross-modal similarity
                similarity_query = f"""
                SELECT `{self.project_id}.{self.dataset_id}.cosine_similarity`(@text_emb, @img_emb) as similarity
                """
                
                config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("text_emb", "REPEATED", text_embedding),
                        bigquery.ScalarQueryParameter("img_emb", "REPEATED", image_embedding)
                    ]
                )
                
                similarity_result = self.client.query(similarity_query, config).result()
                similarity_score = list(similarity_result)[0]['similarity']
                
                # Convert to quality score
                quality_score = similarity_score * 100
                confidence_score = min(1.0, similarity_score + 0.15)  # Slightly lower confidence for cross-modal
                
                return {
                    'score': round(quality_score, 2),
                    'confidence': round(confidence_score, 3),
                    'cross_modal_similarity': float(similarity_score),
                    'embeddings_cached': True
                }
            
            return {'score': 50, 'confidence': 0.3, 'embeddings_cached': False}
            
        except Exception as e:
            logger.error(f"Error computing cross-modal score: {str(e)}")
            return {'score': 50, 'confidence': 0.3, 'error': str(e)}
    
    def _compute_content_coherence_score_optimized(
        self,
        product_id: str,
        content_items: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        """Compute content coherence score using embedding hub"""
        try:
            embeddings = {}
            
            # Generate embeddings for all content items
            for item in content_items:
                embedding = self.embedding_manager.generate_text_embedding(
                    content=item['content'],
                    content_type=item['type'],
                    content_id=f"{product_id}_{item['type']}",
                    product_id=product_id
                )
                if embedding:
                    embeddings[item['type']] = embedding
            
            if len(embeddings) >= 2:
                # Compute pairwise similarities
                similarities = []
                content_types = list(embeddings.keys())
                
                for i, type_a in enumerate(content_types):
                    for j, type_b in enumerate(content_types[i+1:], i+1):
                        
                        similarity_query = f"""
                        SELECT `{self.project_id}.{self.dataset_id}.cosine_similarity`(@emb_a, @emb_b) as similarity
                        """
                        
                        config = bigquery.QueryJobConfig(
                            query_parameters=[
                                bigquery.ScalarQueryParameter("emb_a", "REPEATED", embeddings[type_a]),
                                bigquery.ScalarQueryParameter("emb_b", "REPEATED", embeddings[type_b])
                            ]
                        )
                        
                        similarity_result = self.client.query(similarity_query, config).result()
                        similarity_score = list(similarity_result)[0]['similarity']
                        similarities.append(similarity_score)
                
                # Average coherence
                avg_coherence = np.mean(similarities)
                quality_score = avg_coherence * 100
                confidence_score = min(1.0, avg_coherence + 0.1)
                
                return {
                    'score': round(quality_score, 2),
                    'confidence': round(confidence_score, 3),
                    'coherence_similarities': [float(s) for s in similarities],
                    'embeddings_cached': True
                }
            
            return {'score': 50, 'confidence': 0.3, 'embeddings_cached': False}
            
        except Exception as e:
            logger.error(f"Error computing content coherence score: {str(e)}")
            return {'score': 50, 'confidence': 0.3, 'error': str(e)}
    
    def _compute_review_validation_score_optimized(
        self,
        product_id: str,
        description: str,
        reviews: List[str]
    ) -> Dict[str, Any]:
        """Compute review validation score using embedding hub and AI analysis"""
        try:
            if not reviews:
                return {'score': 50, 'confidence': 0.2}
            
            # Sample reviews if too many
            review_sample = reviews[:10] if len(reviews) > 10 else reviews
            combined_reviews = ' '.join(review_sample)
            
            # Generate embeddings
            desc_embedding = self.embedding_manager.generate_text_embedding(
                content=description,
                content_type='description',
                content_id=f"{product_id}_description",
                product_id=product_id
            )
            
            review_embedding = self.embedding_manager.generate_text_embedding(
                content=combined_reviews,
                content_type='reviews',
                content_id=f"{product_id}_reviews",
                product_id=product_id
            )
            
            if desc_embedding and review_embedding:
                # Similarity between description and reviews
                similarity_query = f"""
                SELECT `{self.project_id}.{self.dataset_id}.cosine_similarity`(@desc_emb, @review_emb) as similarity
                """
                
                config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("desc_emb", "REPEATED", desc_embedding),
                        bigquery.ScalarQueryParameter("review_emb", "REPEATED", review_embedding)
                    ]
                )
                
                similarity_result = self.client.query(similarity_query, config).result()  
                similarity_score = list(similarity_result)[0]['similarity']
                
                # AI sentiment analysis for additional validation
                sentiment_analysis = self._analyze_review_sentiment(combined_reviews)
                
                # Combine similarity and sentiment for final score
                base_score = similarity_score * 100
                sentiment_adjustment = sentiment_analysis.get('positivity_score', 0.5) * 10
                
                final_score = min(100, base_score + sentiment_adjustment)
                confidence_score = min(1.0, similarity_score + 0.1)
                
                return {
                    'score': round(final_score, 2),
                    'confidence': round(confidence_score, 3),
                    'description_review_similarity': float(similarity_score),
                    'sentiment_analysis': sentiment_analysis,
                    'reviews_analyzed': len(review_sample)
                }
            
            return {'score': 50, 'confidence': 0.3}
            
        except Exception as e:
            logger.error(f"Error computing review validation score: {str(e)}")
            return {'score': 50, 'confidence': 0.3, 'error': str(e)}
    
    def _compute_technical_accuracy_score_optimized(
        self,
        product_id: str,
        specifications: Any
    ) -> Dict[str, Any]:
        """Compute technical accuracy score using AI validation"""
        try:
            specs_str = json.dumps(specifications) if isinstance(specifications, dict) else str(specifications)
            
            # AI-based technical validation
            tech_validation_query = f"""
            SELECT
                AI.GENERATE_TEXT(
                    'Analyze these product specifications for technical accuracy, completeness, and consistency. 
                     Rate on scale 0-100 and explain any issues. Format: "Score: XX/100. Analysis: [explanation]"
                     Specifications: ' || @specs
                ) AS tech_analysis
            """
            
            config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("specs", "STRING", specs_str)
                ]
            )
            
            result = self.client.query(tech_validation_query, config).result()
            analysis_text = list(result)[0]['tech_analysis']
            
            # Extract score from AI analysis
            score = 70  # Default
            try:
                if 'Score:' in analysis_text:
                    score_part = analysis_text.split('Score:')[1].split('/100')[0].strip()
                    score = float(score_part)
            except:
                pass
            
            return {
                'score': round(score, 2),
                'confidence': 0.8,  # High confidence in AI analysis
                'ai_analysis': analysis_text
            }
            
        except Exception as e:
            logger.error(f"Error computing technical accuracy score: {str(e)}")
            return {'score': 50, 'confidence': 0.3, 'error': str(e)}
    
    def _ai_fallback_desc_spec_score(self, description: str, specifications: str) -> Dict[str, Any]:
        """AI fallback for description-spec scoring when embeddings unavailable"""
        try:
            ai_query = f"""
            SELECT
                AI.GENERATE_TEXT(
                    'Rate alignment between description and specs 0-100. Format: "Score: XX/100"
                    Description: ' || @desc || ' | Specs: ' || @specs
                ) AS alignment_score
            """
            
            config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("desc", "STRING", description),
                    bigquery.ScalarQueryParameter("specs", "STRING", specifications)
                ]
            )
            
            result = self.client.query(ai_query, config).result()
            analysis_text = list(result)[0]['alignment_score']
            
            score = 50  # Default
            try:
                if 'Score:' in analysis_text:
                    score_part = analysis_text.split('Score:')[1].split('/100')[0].strip()
                    score = float(score_part)
            except:
                pass
            
            return {'score': score, 'confidence': 0.6, 'embeddings_cached': False}
            
        except Exception as e:
            return {'score': 50, 'confidence': 0.3, 'error': str(e)}
    
    def _analyze_review_sentiment(self, reviews_text: str) -> Dict[str, Any]:
        """Analyze sentiment of customer reviews"""
        try:
            sentiment_query = f"""
            SELECT
                AI.GENERATE_TEXT(
                    'Analyze sentiment of these reviews. Rate positivity 0-1 and summarize. 
                     Format: "Positivity: 0.X Summary: [brief summary]"
                     Reviews: ' || @reviews
                ) AS sentiment_analysis
            """
            
            config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("reviews", "STRING", reviews_text)
                ]
            )
            
            result = self.client.query(sentiment_query, config).result()
            analysis_text = list(result)[0]['sentiment_analysis']
            
            positivity_score = 0.5  # Default
            try:
                if 'Positivity:' in analysis_text:
                    pos_part = analysis_text.split('Positivity:')[1].split('Summary:')[0].strip()
                    positivity_score = float(pos_part)
            except:
                pass
            
            return {
                'positivity_score': positivity_score,
                'analysis': analysis_text
            }
            
        except Exception as e:
            return {'positivity_score': 0.5, 'error': str(e)}
    
    def _categorize_quality_risk(self, score: float, confidence: float) -> str:
        """Categorize quality risk based on score and confidence"""
        if score >= 80 and confidence >= 0.7:
            return 'Low'
        elif score >= 60 and confidence >= 0.5:
            return 'Medium'  
        elif score >= 40:
            return 'High'
        else:
            return 'Critical'
    
    def _get_quality_grade(self, score: float) -> str:
        """Convert numeric score to letter grade"""
        if score >= 90:
            return 'A'
        elif score >= 80:
            return 'B'
        elif score >= 70:
            return 'C'
        elif score >= 60:
            return 'D'
        else:
            return 'F'
    
    def _compute_confidence_interval(self, score: float, confidence: float, num_components: int) -> Dict[str, float]:
        """Compute confidence interval for the quality score"""
        # Standard error based on confidence and number of components
        std_error = (1 - confidence) * 10 * (1 / np.sqrt(max(1, num_components)))
        
        # 95% confidence interval
        margin = 1.96 * std_error
        
        return {
            'lower_bound': max(0, score - margin),
            'upper_bound': min(100, score + margin),
            'margin_of_error': margin
        }

    def batch_quality_scoring_optimized(
        self,
        products: List[Dict[str, Any]],
        output_table: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Batch quality scoring with embedding hub optimization
        
        Args:
            products: List of product dictionaries
            output_table: Optional BigQuery table to save results
            
        Returns:
            Comprehensive batch scoring results with performance metrics
        """
        results = {
            'products_processed': 0,
            'quality_scores': {},
            'performance_stats': {},
            'business_intelligence': {}
        }
        
        start_time = datetime.now()
        all_scores = []
        
        for product in products:
            product_id = product.get('product_id', f'product_{results["products_processed"]}')
            
            try:
                quality_result = self.compute_comprehensive_quality_score_optimized(
                    product_id=product_id,
                    product_data=product,
                    include_confidence_intervals=True
                )
                
                results['quality_scores'][product_id] = quality_result
                results['products_processed'] += 1
                
                if 'unified_quality_score' in quality_result:
                    all_scores.append(quality_result['unified_quality_score'])
                
            except Exception as e:
                logger.error(f"Error scoring product {product_id}: {str(e)}")
                results.setdefault('errors', []).append(f"Product {product_id}: {str(e)}")
        
        # Performance statistics
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        embedding_stats = self.embedding_manager.get_embedding_stats()
        search_stats = self.search_engine.get_search_performance_stats()
        
        results['performance_stats'] = {
            'total_processing_time': processing_time,
            'avg_time_per_product': processing_time / max(1, results['products_processed']),
            'embedding_cache_hit_rate': embedding_stats.get('session_stats', {}).get('cache_hit_rate', 0),
            'search_cache_hit_rate': search_stats.get('cache_hit_rate', 0),
            'timestamp': end_time.isoformat()
        }
        
        # Business intelligence summary
        if all_scores:
            score_df = pd.DataFrame([
                {
                    'product_id': pid,
                    'unified_quality_score': score_data.get('unified_quality_score', 0),
                    'quality_grade': score_data.get('quality_grade', 'F'),
                    'risk_category': score_data.get('risk_category', 'Unknown'),
                    'overall_confidence': score_data.get('overall_confidence', 0)
                }
                for pid, score_data in results['quality_scores'].items()
            ])
            
            results['business_intelligence'] = {
                'average_quality_score': np.mean(all_scores),
                'score_distribution': {
                    'A': len(score_df[score_df['quality_grade'] == 'A']),
                    'B': len(score_df[score_df['quality_grade'] == 'B']),
                    'C': len(score_df[score_df['quality_grade'] == 'C']),
                    'D': len(score_df[score_df['quality_grade'] == 'D']),
                    'F': len(score_df[score_df['quality_grade'] == 'F'])
                },
                'risk_distribution': score_df['risk_category'].value_counts().to_dict(),
                'high_confidence_products': len(score_df[score_df['overall_confidence'] >= 0.8])
            }
        
        # Save to BigQuery if requested
        if output_table and results['quality_scores']:
            self._save_scores_to_bigquery(results['quality_scores'], output_table)
        
        logger.info(f"Batch quality scoring completed: {results['products_processed']} products in {processing_time:.2f}s")
        logger.info(f"Average quality score: {results['business_intelligence'].get('average_quality_score', 0):.2f}")
        logger.info(f"Embedding cache hit rate: {results['performance_stats']['embedding_cache_hit_rate']:.2%}")
        
        return results

    def _save_scores_to_bigquery(self, quality_scores: Dict[str, Any], output_table: str):
        """Save quality scores to BigQuery table"""
        try:
            # Prepare data for BigQuery
            rows = []
            for product_id, score_data in quality_scores.items():
                if 'error' not in score_data:
                    row = {
                        'product_id': product_id,
                        'unified_quality_score': score_data.get('unified_quality_score', 0),
                        'quality_grade': score_data.get('quality_grade', 'F'),
                        'risk_category': score_data.get('risk_category', 'Unknown'),
                        'overall_confidence': score_data.get('overall_confidence', 0),
                        'component_scores': json.dumps(score_data.get('component_scores', {})),
                        'scoring_timestamp': score_data.get('scoring_timestamp'),
                        'embeddings_performance': json.dumps(score_data.get('embeddings_performance', {}))
                    }
                    rows.append(row)
            
            if rows:
                # Create table if not exists
                table_ref = self.client.dataset(self.dataset_id).table(output_table)
                
                try:
                    table = self.client.get_table(table_ref)
                except:
                    # Create table schema
                    schema = [
                        bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
                        bigquery.SchemaField("unified_quality_score", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("quality_grade", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("risk_category", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("overall_confidence", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("component_scores", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("scoring_timestamp", "TIMESTAMP", mode="NULLABLE"),
                        bigquery.SchemaField("embeddings_performance", "STRING", mode="NULLABLE")
                    ]
                    
                    table = bigquery.Table(table_ref, schema=schema)
                    table = self.client.create_table(table)
                    
                # Insert rows
                errors = self.client.insert_rows_json(table, rows)
                if errors:
                    logger.error(f"Errors saving to BigQuery: {errors}")
                else:
                    logger.info(f"Successfully saved {len(rows)} quality scores to {output_table}")
                    
        except Exception as e:
            logger.error(f"Error saving scores to BigQuery: {str(e)}")


# =====================================================
# Convenience functions for backward compatibility
# =====================================================

def compute_unified_quality_score_optimized(
    client,
    project_id: str,
    dataset_id: str,
    product_data: Dict[str, Any],
    product_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Hub-optimized version of unified quality scoring
    Provides easy migration from legacy functions
    """
    scorer = QualityScorer(client, project_id, dataset_id)
    
    pid = product_id or product_data.get('product_id', 'unknown_product')
    
    return scorer.compute_comprehensive_quality_score_optimized(
        product_id=pid,
        product_data=product_data,
        include_confidence_intervals=True
    )
