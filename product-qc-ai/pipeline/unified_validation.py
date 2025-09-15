"""
Unified Product Quality Validation Orchestrator

This module serves as the master orchestrator for comprehensive product quality assessment,
coordinating all enhanced validation, scoring, consistency, and recommendation modules
to provide a single entry point for complete business intelligence.

Key Features:
- Unified 0-100 scoring across all dimensions
- Confidence intervals and risk categorization
- Cross-modal validation (spec↔image, description↔image, text↔text)
- Business intelligence with executive reporting
- Priority-based action plans and correction recommendations
"""

import logging
from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime

from .validation import (
    run_comprehensive_validation,
    validate_description_spec_alignment_enhanced,
    validate_spec_image_alignment,
    validate_image_text_alignment_enhanced,
    validate_review_alignment_enhanced
)
from .scoring import (
    compute_unified_mismatch_score,
    calculate_confidence_interval,
    categorize_risk_level,
    generate_business_intelligence_summary
)
from .consistency import (
    check_spec_image_consistency,
    check_description_image_consistency,
    generate_multimodal_consistency_report
)
from .recommendations import (
    generate_enhanced_corrected_descriptions,
    generate_enhanced_image_text_alerts,
    generate_business_action_plan,
    assess_correction_priority,
    generate_executive_summary
)

logger = logging.getLogger(__name__)

@dataclass
class UnifiedValidationResult:
    """Comprehensive validation result with business intelligence"""
    product_id: str
    validation_timestamp: datetime
    
    # Core Scores (0-100 scale)
    overall_quality_score: float
    description_spec_score: float
    image_alignment_score: float
    review_consistency_score: float
    
    # Confidence & Risk Assessment
    confidence_interval: Tuple[float, float]
    risk_level: str  # LOW, MEDIUM, HIGH, CRITICAL
    confidence_score: float
    
    # Validation Details
    validation_results: Dict[str, Any]
    consistency_report: Dict[str, Any]
    
    # Business Intelligence
    corrected_descriptions: List[Dict[str, Any]]
    image_text_alerts: List[Dict[str, Any]]
    action_plan: Dict[str, Any]
    priority_assessment: Dict[str, Any]
    executive_summary: Dict[str, Any]
    
    # Technical Metadata
    processing_time_seconds: float
    ai_model_versions: Dict[str, str]
    data_quality_flags: List[str]


class UnifiedValidationOrchestrator:
    """Master orchestrator for comprehensive product quality validation"""
    
    def __init__(self, client, project_id: str, dataset_id: str):
        """
        Initialize the unified validation orchestrator
        
        Args:
            client: BigQuery client instance
            project_id: Google Cloud project ID
            dataset_id: BigQuery dataset ID
        """
        self.client = client
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.ai_model_versions = {
            "embedding_model": "textembedding-gecko@003",
            "text_generation": "gemini-1.5-pro-002",
            "image_analysis": "gemini-1.5-pro-vision-002"
        }
    
    def run_unified_validation(
        self,
        product_id: str,
        description: str,
        specifications: str,
        image_path: Optional[str] = None,
        reviews: Optional[List[str]] = None,
        include_recommendations: bool = True
    ) -> UnifiedValidationResult:
        """
        Execute comprehensive unified validation for a product
        
        This is the main entry point that orchestrates all validation modules
        to provide complete business intelligence on product quality.
        
        Args:
            product_id: Unique product identifier
            description: Product description text
            specifications: Product specifications text
            image_path: Optional path to product image
            reviews: Optional list of customer reviews
            include_recommendations: Whether to generate correction recommendations
            
        Returns:
            UnifiedValidationResult with comprehensive quality assessment
        """
        start_time = datetime.now()
        logger.info(f"Starting unified validation for product {product_id}")
        
        try:
            # Phase 1: Comprehensive Validation
            logger.info("Phase 1: Running comprehensive validation...")
            validation_results = run_comprehensive_validation(
                self.client, self.project_id, self.dataset_id,
                description, specifications, image_path, reviews
            )
            
            # Phase 2: Cross-Modal Consistency Analysis
            logger.info("Phase 2: Analyzing cross-modal consistency...")
            consistency_report = self._analyze_cross_modal_consistency(
                description, specifications, image_path, reviews
            )
            
            # Phase 3: Unified Scoring and Risk Assessment
            logger.info("Phase 3: Computing unified scores and risk assessment...")
            scoring_results = self._compute_unified_scoring(validation_results, consistency_report)
            
            # Phase 4: Business Intelligence and Recommendations
            recommendations = {}
            if include_recommendations:
                logger.info("Phase 4: Generating business intelligence and recommendations...")
                recommendations = self._generate_business_intelligence(
                    product_id, description, specifications, image_path,
                    validation_results, consistency_report, scoring_results
                )
            
            # Phase 5: Executive Summary and Action Planning
            logger.info("Phase 5: Creating executive summary and action plans...")
            executive_insights = self._create_executive_insights(
                product_id, scoring_results, recommendations, validation_results
            )
            
            # Compile comprehensive result
            processing_time = (datetime.now() - start_time).total_seconds()
            
            result = UnifiedValidationResult(
                product_id=product_id,
                validation_timestamp=start_time,
                overall_quality_score=scoring_results['overall_score'],
                description_spec_score=scoring_results['description_spec_score'],
                image_alignment_score=scoring_results['image_alignment_score'],
                review_consistency_score=scoring_results['review_consistency_score'],
                confidence_interval=scoring_results['confidence_interval'],
                risk_level=scoring_results['risk_level'],
                confidence_score=scoring_results['confidence_score'],
                validation_results=validation_results,
                consistency_report=consistency_report,
                corrected_descriptions=recommendations.get('corrected_descriptions', []),
                image_text_alerts=recommendations.get('image_text_alerts', []),
                action_plan=recommendations.get('action_plan', {}),
                priority_assessment=recommendations.get('priority_assessment', {}),
                executive_summary=executive_insights,
                processing_time_seconds=processing_time,
                ai_model_versions=self.ai_model_versions,
                data_quality_flags=self._assess_data_quality_flags(
                    description, specifications, image_path, reviews
                )
            )
            
            logger.info(f"Unified validation completed for product {product_id} "
                       f"in {processing_time:.2f} seconds")
            return result
            
        except Exception as e:
            logger.error(f"Error in unified validation for product {product_id}: {str(e)}")
            raise
    
    def _analyze_cross_modal_consistency(
        self,
        description: str,
        specifications: str,
        image_path: Optional[str],
        reviews: Optional[List[str]]
    ) -> Dict[str, Any]:
        """Analyze consistency across all product modalities"""
        consistency_results = {}
        
        try:
            # Spec-Image Consistency
            if image_path:
                consistency_results['spec_image'] = check_spec_image_consistency(
                    self.client, self.project_id, self.dataset_id,
                    specifications, image_path
                )
                
                # Description-Image Consistency
                consistency_results['description_image'] = check_description_image_consistency(
                    self.client, self.project_id, self.dataset_id,
                    description, image_path
                )
            
            # Generate comprehensive multimodal report
            consistency_results['multimodal_report'] = generate_multimodal_consistency_report(
                self.client, self.project_id, self.dataset_id,
                description, specifications, image_path, reviews
            )
            
        except Exception as e:
            logger.error(f"Error in cross-modal consistency analysis: {str(e)}")
            consistency_results['error'] = str(e)
        
        return consistency_results
    
    def _compute_unified_scoring(
        self,
        validation_results: Dict[str, Any],
        consistency_report: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Compute unified 0-100 scores with confidence intervals and risk assessment"""
        try:
            # Extract validation scores
            desc_spec_score = validation_results.get('description_spec_alignment', {}).get('alignment_score', 50)
            image_score = validation_results.get('image_alignment', {}).get('alignment_score', 50)
            review_score = validation_results.get('review_alignment', {}).get('consistency_score', 50)
            
            # Compute unified mismatch score
            overall_score = compute_unified_mismatch_score(
                self.client, self.project_id, self.dataset_id,
                desc_spec_score, image_score, review_score,
                validation_results, consistency_report
            )
            
            # Calculate confidence interval
            confidence_interval = calculate_confidence_interval(
                overall_score, validation_results, consistency_report
            )
            
            # Assess risk level
            risk_level = categorize_risk_level(overall_score, confidence_interval[0])
            
            # Calculate confidence score
            confidence_score = min(100.0, max(0.0, 
                100 - abs(confidence_interval[1] - confidence_interval[0])
            ))
            
            # Generate business intelligence summary
            bi_summary = generate_business_intelligence_summary(
                overall_score, confidence_interval, risk_level,
                validation_results, consistency_report
            )
            
            return {
                'overall_score': overall_score,
                'description_spec_score': desc_spec_score,
                'image_alignment_score': image_score,
                'review_consistency_score': review_score,
                'confidence_interval': confidence_interval,
                'risk_level': risk_level,
                'confidence_score': confidence_score,
                'business_intelligence': bi_summary
            }
            
        except Exception as e:
            logger.error(f"Error in unified scoring: {str(e)}")
            return {
                'overall_score': 50.0,
                'description_spec_score': 50.0,
                'image_alignment_score': 50.0,
                'review_consistency_score': 50.0,
                'confidence_interval': (40.0, 60.0),
                'risk_level': 'MEDIUM',
                'confidence_score': 50.0,
                'business_intelligence': {'error': str(e)}
            }
    
    def _generate_business_intelligence(
        self,
        product_id: str,
        description: str,
        specifications: str,
        image_path: Optional[str],
        validation_results: Dict[str, Any],
        consistency_report: Dict[str, Any],
        scoring_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate comprehensive business intelligence and recommendations"""
        recommendations = {}
        
        try:
            # Generate corrected descriptions
            recommendations['corrected_descriptions'] = generate_enhanced_corrected_descriptions(
                self.client, self.project_id, self.dataset_id,
                description, specifications, validation_results
            )
            
            # Generate image-text alerts if image exists
            if image_path:
                recommendations['image_text_alerts'] = generate_enhanced_image_text_alerts(
                    self.client, self.project_id, self.dataset_id,
                    description, image_path, validation_results
                )
            else:
                recommendations['image_text_alerts'] = []
            
            # Generate business action plan
            recommendations['action_plan'] = generate_business_action_plan(
                self.client, self.project_id, self.dataset_id,
                product_id, scoring_results['overall_score'],
                validation_results, consistency_report
            )
            
            # Assess correction priority
            recommendations['priority_assessment'] = assess_correction_priority(
                self.client, self.project_id, self.dataset_id,
                scoring_results['overall_score'], scoring_results['risk_level'],
                validation_results, recommendations['corrected_descriptions']
            )
            
        except Exception as e:
            logger.error(f"Error generating business intelligence: {str(e)}")
            recommendations['error'] = str(e)
        
        return recommendations
    
    def _create_executive_insights(
        self,
        product_id: str,
        scoring_results: Dict[str, Any],
        recommendations: Dict[str, Any],
        validation_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create executive summary with high-level insights and strategic recommendations"""
        try:
            executive_summary = generate_executive_summary(
                self.client, self.project_id, self.dataset_id,
                product_id, scoring_results['overall_score'],
                scoring_results['risk_level'], recommendations,
                validation_results
            )
            
            # Add strategic insights
            executive_summary['strategic_insights'] = self._generate_strategic_insights(
                scoring_results, recommendations, validation_results
            )
            
            # Add KPI dashboard data
            executive_summary['kpi_dashboard'] = self._generate_kpi_dashboard(
                scoring_results, validation_results
            )
            
            return executive_summary
            
        except Exception as e:
            logger.error(f"Error creating executive insights: {str(e)}")
            return {
                'error': str(e),
                'fallback_summary': f"Product {product_id} quality score: {scoring_results.get('overall_score', 'N/A')}"
            }
    
    def _generate_strategic_insights(
        self,
        scoring_results: Dict[str, Any],
        recommendations: Dict[str, Any],
        validation_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate strategic business insights"""
        overall_score = scoring_results.get('overall_score', 50)
        risk_level = scoring_results.get('risk_level', 'MEDIUM')
        
        insights = {
            'quality_trend': 'stable',  # Would be computed from historical data
            'competitive_position': 'average',  # Would be computed vs. category benchmarks
            'improvement_potential': max(0, 100 - overall_score),
            'business_impact': self._assess_business_impact(overall_score, risk_level),
            'resource_requirements': self._estimate_resource_requirements(recommendations),
            'timeline_estimate': self._estimate_correction_timeline(recommendations)
        }
        
        return insights
    
    def _generate_kpi_dashboard(
        self,
        scoring_results: Dict[str, Any],
        validation_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate KPI dashboard data for executive reporting"""
        return {
            'quality_score': scoring_results.get('overall_score', 50),
            'confidence_level': scoring_results.get('confidence_score', 50),
            'risk_level': scoring_results.get('risk_level', 'MEDIUM'),
            'validation_coverage': len([k for k, v in validation_results.items() if v and not isinstance(v, str)]),
            'critical_issues': len([k for k, v in validation_results.items() 
                                  if isinstance(v, dict) and v.get('severity') == 'HIGH']),
            'compliance_status': 'COMPLIANT' if scoring_results.get('overall_score', 50) >= 70 else 'NON_COMPLIANT'
        }
    
    def _assess_business_impact(self, overall_score: float, risk_level: str) -> str:
        """Assess business impact based on quality score and risk level"""
        if overall_score >= 85 and risk_level == 'LOW':
            return 'POSITIVE'
        elif overall_score >= 70 and risk_level in ['LOW', 'MEDIUM']:
            return 'NEUTRAL'
        elif overall_score >= 50:
            return 'MODERATE_NEGATIVE'
        else:
            return 'HIGH_NEGATIVE'
    
    def _estimate_resource_requirements(self, recommendations: Dict[str, Any]) -> str:
        """Estimate resource requirements for implementing recommendations"""
        corrected_desc = len(recommendations.get('corrected_descriptions', []))
        image_alerts = len(recommendations.get('image_text_alerts', []))
        
        total_changes = corrected_desc + image_alerts
        
        if total_changes <= 2:
            return 'LOW'
        elif total_changes <= 5:
            return 'MEDIUM'
        else:
            return 'HIGH'
    
    def _estimate_correction_timeline(self, recommendations: Dict[str, Any]) -> str:
        """Estimate timeline for implementing corrections"""
        priority = recommendations.get('priority_assessment', {}).get('priority_level', 'MEDIUM')
        
        if priority == 'CRITICAL':
            return '1-2 days'
        elif priority == 'HIGH':
            return '3-5 days'
        elif priority == 'MEDIUM':
            return '1-2 weeks'
        else:
            return '2-4 weeks'
    
    def _assess_data_quality_flags(
        self,
        description: str,
        specifications: str,
        image_path: Optional[str],
        reviews: Optional[List[str]]
    ) -> List[str]:
        """Assess data quality flags for the input data"""
        flags = []
        
        if not description or len(description.strip()) < 10:
            flags.append('INSUFFICIENT_DESCRIPTION')
        
        if not specifications or len(specifications.strip()) < 10:
            flags.append('INSUFFICIENT_SPECIFICATIONS')
        
        if image_path is None:
            flags.append('MISSING_PRODUCT_IMAGE')
        
        if not reviews or len(reviews) == 0:
            flags.append('NO_CUSTOMER_REVIEWS')
        elif len(reviews) < 3:
            flags.append('LIMITED_REVIEW_DATA')
        
        return flags

    def run_batch_validation(
        self,
        products: List[Dict[str, Any]],
        include_recommendations: bool = True
    ) -> List[UnifiedValidationResult]:
        """
        Run unified validation on a batch of products
        
        Args:
            products: List of product dictionaries with keys: 
                     product_id, description, specifications, image_path, reviews
            include_recommendations: Whether to generate recommendations
            
        Returns:
            List of UnifiedValidationResult objects
        """
        results = []
        
        for i, product in enumerate(products):
            logger.info(f"Processing product {i+1}/{len(products)}: {product.get('product_id', 'Unknown')}")
            
            try:
                result = self.run_unified_validation(
                    product_id=product.get('product_id', f'product_{i}'),
                    description=product.get('description', ''),
                    specifications=product.get('specifications', ''),
                    image_path=product.get('image_path'),
                    reviews=product.get('reviews', []),
                    include_recommendations=include_recommendations
                )
                results.append(result)
                
            except Exception as e:
                logger.error(f"Error processing product {product.get('product_id', f'product_{i}')}: {str(e)}")
                # Create error result
                error_result = UnifiedValidationResult(
                    product_id=product.get('product_id', f'product_{i}'),
                    validation_timestamp=datetime.now(),
                    overall_quality_score=0.0,
                    description_spec_score=0.0,
                    image_alignment_score=0.0,
                    review_consistency_score=0.0,
                    confidence_interval=(0.0, 0.0),
                    risk_level='CRITICAL',
                    confidence_score=0.0,
                    validation_results={'error': str(e)},
                    consistency_report={'error': str(e)},
                    corrected_descriptions=[],
                    image_text_alerts=[],
                    action_plan={'error': str(e)},
                    priority_assessment={'error': str(e)},
                    executive_summary={'error': str(e)},
                    processing_time_seconds=0.0,
                    ai_model_versions=self.ai_model_versions,
                    data_quality_flags=['PROCESSING_ERROR']
                )
                results.append(error_result)
        
        return results

    def generate_portfolio_insights(
        self,
        validation_results: List[UnifiedValidationResult]
    ) -> Dict[str, Any]:
        """
        Generate portfolio-level insights from multiple product validations
        
        Args:
            validation_results: List of validation results from batch processing
            
        Returns:
            Portfolio-level insights and analytics
        """
        if not validation_results:
            return {'error': 'No validation results provided'}
        
        # Calculate portfolio metrics
        scores = [r.overall_quality_score for r in validation_results if r.overall_quality_score > 0]
        avg_score = sum(scores) / len(scores) if scores else 0
        
        risk_distribution = {}
        for result in validation_results:
            risk_level = result.risk_level
            risk_distribution[risk_level] = risk_distribution.get(risk_level, 0) + 1
        
        # Identify top issues
        all_issues = []
        for result in validation_results:
            if result.action_plan and 'issues' in result.action_plan:
                all_issues.extend(result.action_plan['issues'])
        
        issue_frequency = {}
        for issue in all_issues:
            issue_type = issue.get('type', 'unknown')
            issue_frequency[issue_type] = issue_frequency.get(issue_type, 0) + 1
        
        top_issues = sorted(issue_frequency.items(), key=lambda x: x[1], reverse=True)[:5]
        
        return {
            'portfolio_summary': {
                'total_products': len(validation_results),
                'average_quality_score': avg_score,
                'score_distribution': {
                    'excellent': len([s for s in scores if s >= 85]),
                    'good': len([s for s in scores if 70 <= s < 85]),
                    'fair': len([s for s in scores if 50 <= s < 70]),
                    'poor': len([s for s in scores if s < 50])
                },
                'risk_distribution': risk_distribution
            },
            'top_issues': [{'issue_type': issue[0], 'frequency': issue[1]} for issue in top_issues],
            'recommendations': {
                'immediate_attention': [r.product_id for r in validation_results 
                                      if r.risk_level == 'CRITICAL'],
                'improvement_candidates': [r.product_id for r in validation_results 
                                         if 50 <= r.overall_quality_score < 70],
                'best_practices': [r.product_id for r in validation_results 
                                 if r.overall_quality_score >= 85]
            }
        }