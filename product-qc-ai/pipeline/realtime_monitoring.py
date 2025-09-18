"""
Real-time Quality Monitoring System
==================================

Comprehensive real-time monitoring dashboard with:
- Live quality metrics and KPI tracking
- Real-time alert notifications and threshold monitoring
- Auto-refreshing charts and trend visualization
- System health monitoring and performance tracking
- Live data streaming and processing status
- Interactive filtering and multi-view analytics

This module provides the backend infrastructure for Phase 4, Step 1:
Real-time Quality Monitoring Dashboard implementation.
"""

import time
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import logging

try:
    from google.cloud import bigquery
    from google.api_core.exceptions import GoogleCloudError
    BIGQUERY_AVAILABLE = True
except ImportError:
    print("Warning: Google Cloud BigQuery not available")
    bigquery = None
    BIGQUERY_AVAILABLE = False

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class AlertConfig:
    """Configuration for real-time alerts"""
    metric_name: str
    threshold_value: float
    condition: str  # 'above', 'below', 'equals'
    severity: str   # 'critical', 'warning', 'info'
    enabled: bool = True
    cooldown_minutes: int = 5

@dataclass
class LiveMetric:
    """Live metric data point"""
    timestamp: datetime
    metric_name: str
    value: float
    category: Optional[str] = None
    metadata: Optional[Dict] = None

@dataclass
class AlertNotification:
    """Alert notification"""
    alert_id: str
    timestamp: datetime
    metric_name: str
    current_value: float
    threshold_value: float
    severity: str
    message: str
    acknowledged: bool = False

class RealTimeDataStreamer:
    """Handles real-time data streaming and processing"""
    
    def __init__(self, client: Any, project_id: str, dataset_id: str):
        self.client = client
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.streaming_active = False
        self.stream_thread = None
        self.data_buffer = deque(maxlen=1000)  # Keep last 1000 data points
        self.callbacks = []
        
    def add_callback(self, callback):
        """Add callback for new data"""
        self.callbacks.append(callback)
        
    def start_streaming(self, interval_seconds: int = 30):
        """Start real-time data streaming"""
        if self.streaming_active:
            logger.warning("Streaming already active")
            return
            
        self.streaming_active = True
        self.stream_thread = threading.Thread(
            target=self._stream_loop,
            args=(interval_seconds,),
            daemon=True
        )
        self.stream_thread.start()
        logger.info(f"Started real-time streaming with {interval_seconds}s interval")
        
    def stop_streaming(self):
        """Stop real-time data streaming"""
        self.streaming_active = False
        if self.stream_thread:
            self.stream_thread.join(timeout=5)
        logger.info("Stopped real-time streaming")
        
    def _stream_loop(self, interval_seconds: int):
        """Main streaming loop"""
        while self.streaming_active:
            try:
                # Fetch latest metrics
                current_metrics = self._fetch_current_metrics()
                
                # Add to buffer
                for metric in current_metrics:
                    self.data_buffer.append(metric)
                
                # Notify callbacks
                for callback in self.callbacks:
                    try:
                        callback(current_metrics)
                    except Exception as e:
                        logger.error(f"Callback error: {e}")
                
                time.sleep(interval_seconds)
                
            except Exception as e:
                logger.error(f"Streaming error: {e}")
                time.sleep(interval_seconds)
                
    def _fetch_current_metrics(self) -> List[LiveMetric]:
        """Fetch current quality metrics from BigQuery"""
        try:
            query = f"""
            WITH recent_quality AS (
                SELECT 
                    product_id,
                    category,
                    quality_score,
                    processing_timestamp,
                    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY processing_timestamp DESC) as rn
                FROM `{self.project_id}.{self.dataset_id}.quality_scores`
                WHERE processing_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
            ),
            live_metrics AS (
                SELECT
                    CURRENT_TIMESTAMP() as timestamp,
                    'overall_quality_score' as metric_name,
                    AVG(quality_score) as value,
                    'overall' as category
                FROM recent_quality WHERE rn = 1
                UNION ALL
                SELECT
                    CURRENT_TIMESTAMP() as timestamp,
                    'processing_rate' as metric_name,
                    COUNT(*) as value,
                    'overall' as category
                FROM recent_quality
                UNION ALL
                SELECT
                    CURRENT_TIMESTAMP() as timestamp,
                    'category_quality_score' as metric_name,
                    AVG(quality_score) as value,
                    category
                FROM recent_quality WHERE rn = 1 AND category IS NOT NULL
                GROUP BY category
            )
            SELECT * FROM live_metrics
            WHERE value IS NOT NULL
            """
            
            results = self.client.query(query).result()
            metrics = []
            
            for row in results:
                metric = LiveMetric(
                    timestamp=row.timestamp,
                    metric_name=row.metric_name,
                    value=float(row.value),
                    category=row.category,
                    metadata={}
                )
                metrics.append(metric)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error fetching current metrics: {e}")
            return []
    
    def get_recent_data(self, metric_name: str = None, minutes: int = 30) -> List[LiveMetric]:
        """Get recent data from buffer"""
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        
        filtered_data = [
            metric for metric in self.data_buffer
            if metric.timestamp >= cutoff_time and (
                metric_name is None or metric.metric_name == metric_name
            )
        ]
        
        return sorted(filtered_data, key=lambda x: x.timestamp)

class AlertManager:
    """Manages real-time alerts and notifications"""
    
    def __init__(self):
        self.alert_configs: Dict[str, AlertConfig] = {}
        self.active_alerts: Dict[str, AlertNotification] = {}
        self.alert_history: List[AlertNotification] = []
        self.last_alert_times: Dict[str, datetime] = {}
        
    def add_alert_config(self, config: AlertConfig):
        """Add alert configuration"""
        self.alert_configs[config.metric_name] = config
        logger.info(f"Added alert config for {config.metric_name}")
        
    def check_alerts(self, metrics: List[LiveMetric]) -> List[AlertNotification]:
        """Check metrics against alert configurations"""
        new_alerts = []
        
        for metric in metrics:
            if metric.metric_name in self.alert_configs:
                config = self.alert_configs[metric.metric_name]
                
                if not config.enabled:
                    continue
                
                # Check cooldown
                last_alert_key = f"{metric.metric_name}_{metric.category or 'overall'}"
                if last_alert_key in self.last_alert_times:
                    time_since_last = datetime.now() - self.last_alert_times[last_alert_key]
                    if time_since_last.total_seconds() < config.cooldown_minutes * 60:
                        continue
                
                # Check threshold
                alert_triggered = False
                if config.condition == 'above' and metric.value > config.threshold_value:
                    alert_triggered = True
                elif config.condition == 'below' and metric.value < config.threshold_value:
                    alert_triggered = True
                elif config.condition == 'equals' and abs(metric.value - config.threshold_value) < 0.001:
                    alert_triggered = True
                
                if alert_triggered:
                    alert = self._create_alert(metric, config)
                    new_alerts.append(alert)
                    self.active_alerts[alert.alert_id] = alert
                    self.alert_history.append(alert)
                    self.last_alert_times[last_alert_key] = datetime.now()
        
        return new_alerts
    
    def _create_alert(self, metric: LiveMetric, config: AlertConfig) -> AlertNotification:
        """Create alert notification"""
        alert_id = f"{metric.metric_name}_{int(time.time())}"
        
        message = (
            f"{config.severity.upper()}: {metric.metric_name} "
            f"({metric.value:.2f}) is {config.condition} threshold ({config.threshold_value:.2f})"
        )
        
        return AlertNotification(
            alert_id=alert_id,
            timestamp=metric.timestamp,
            metric_name=metric.metric_name,
            current_value=metric.value,
            threshold_value=config.threshold_value,
            severity=config.severity,
            message=message
        )
    
    def acknowledge_alert(self, alert_id: str):
        """Acknowledge an alert"""
        if alert_id in self.active_alerts:
            self.active_alerts[alert_id].acknowledged = True
            logger.info(f"Acknowledged alert {alert_id}")
    
    def get_active_alerts(self) -> List[AlertNotification]:
        """Get all active alerts"""
        return [alert for alert in self.active_alerts.values() if not alert.acknowledged]
    
    def get_alert_history(self, hours: int = 24) -> List[AlertNotification]:
        """Get alert history"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        return [
            alert for alert in self.alert_history
            if alert.timestamp >= cutoff_time
        ]

class SystemHealthMonitor:
    """Monitors system health and performance"""
    
    def __init__(self, client: Any, project_id: str, dataset_id: str):
        self.client = client
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.health_metrics = {}
        self.performance_history = deque(maxlen=100)
        
    def check_system_health(self) -> Dict[str, Any]:
        """Check overall system health"""
        health_status = {
            'timestamp': datetime.now(),
            'overall_status': 'healthy',
            'components': {},
            'performance_metrics': {},
            'issues': []
        }
        
        try:
            # Check BigQuery connectivity
            bq_status = self._check_bigquery_health()
            health_status['components']['bigquery'] = bq_status
            
            # Check data freshness
            data_freshness = self._check_data_freshness()
            health_status['components']['data_freshness'] = data_freshness
            
            # Check processing performance
            performance = self._check_processing_performance()
            health_status['performance_metrics'] = performance
            
            # Determine overall status
            component_statuses = [comp['status'] for comp in health_status['components'].values()]
            if 'critical' in component_statuses:
                health_status['overall_status'] = 'critical'
            elif 'warning' in component_statuses:
                health_status['overall_status'] = 'warning'
            
            # Collect issues
            for comp_name, comp_data in health_status['components'].items():
                if comp_data['status'] != 'healthy':
                    health_status['issues'].append(f"{comp_name}: {comp_data.get('message', 'Issue detected')}")
            
        except Exception as e:
            health_status['overall_status'] = 'critical'
            health_status['issues'].append(f"Health check error: {str(e)}")
            logger.error(f"System health check error: {e}")
        
        # Store in history
        self.performance_history.append(health_status)
        
        return health_status
    
    def _check_bigquery_health(self) -> Dict[str, Any]:
        """Check BigQuery connectivity and performance"""
        try:
            start_time = time.time()
            
            # Simple connectivity test
            query = f"SELECT 1 as test_connection"
            self.client.query(query).result()
            
            response_time = time.time() - start_time
            
            status = 'healthy'
            if response_time > 5:
                status = 'warning'
            elif response_time > 10:
                status = 'critical'
            
            return {
                'status': status,
                'response_time_seconds': response_time,
                'message': f"BigQuery response time: {response_time:.2f}s"
            }
            
        except Exception as e:
            return {
                'status': 'critical',
                'response_time_seconds': None,
                'message': f"BigQuery connection failed: {str(e)}"
            }
    
    def _check_data_freshness(self) -> Dict[str, Any]:
        """Check data freshness"""
        try:
            query = f"""
            SELECT 
                MAX(processing_timestamp) as latest_processing,
                COUNT(*) as recent_records
            FROM `{self.project_id}.{self.dataset_id}.quality_scores`
            WHERE processing_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
            """
            
            result = list(self.client.query(query).result())[0]
            
            if result.latest_processing:
                minutes_since_last = (datetime.now(result.latest_processing.tzinfo) - result.latest_processing).total_seconds() / 60
                recent_count = result.recent_records or 0
                
                status = 'healthy'
                if minutes_since_last > 30:
                    status = 'warning'
                elif minutes_since_last > 60:
                    status = 'critical'
                
                return {
                    'status': status,
                    'minutes_since_last_processing': minutes_since_last,
                    'recent_records_count': recent_count,
                    'message': f"Last processing: {minutes_since_last:.1f} minutes ago"
                }
            else:
                return {
                    'status': 'critical',
                    'minutes_since_last_processing': None,
                    'recent_records_count': 0,
                    'message': "No recent processing activity detected"
                }
                
        except Exception as e:
            return {
                'status': 'critical',
                'message': f"Data freshness check failed: {str(e)}"
            }
    
    def _check_processing_performance(self) -> Dict[str, Any]:
        """Check processing performance metrics"""
        try:
            query = f"""
            WITH hourly_stats AS (
                SELECT 
                    EXTRACT(HOUR FROM processing_timestamp) as hour,
                    COUNT(*) as records_processed,
                    AVG(quality_score) as avg_quality,
                    STDDEV(quality_score) as quality_stddev
                FROM `{self.project_id}.{self.dataset_id}.quality_scores`
                WHERE processing_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
                GROUP BY EXTRACT(HOUR FROM processing_timestamp)
            )
            SELECT 
                AVG(records_processed) as avg_hourly_processing,
                MAX(records_processed) as peak_hourly_processing,
                MIN(records_processed) as min_hourly_processing,
                AVG(avg_quality) as overall_avg_quality,
                AVG(quality_stddev) as avg_quality_variation
            FROM hourly_stats
            """
            
            result = list(self.client.query(query).result())[0]
            
            return {
                'avg_hourly_processing': float(result.avg_hourly_processing or 0),
                'peak_hourly_processing': float(result.peak_hourly_processing or 0),
                'min_hourly_processing': float(result.min_hourly_processing or 0),
                'overall_avg_quality': float(result.overall_avg_quality or 0),
                'avg_quality_variation': float(result.avg_quality_variation or 0)
            }
            
        except Exception as e:
            logger.error(f"Performance check error: {e}")
            return {}

    def get_performance_history(self, hours: int = 24) -> List[Dict]:
        """Get performance history"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        return [
            entry for entry in self.performance_history
            if entry['timestamp'] >= cutoff_time
        ]

class RealTimeMonitoringManager:
    """Main manager for real-time monitoring system"""
    
    def __init__(self, client: Any, project_id: str, dataset_id: str):
        self.client = client
        self.project_id = project_id
        self.dataset_id = dataset_id
        
        # Initialize components
        self.data_streamer = RealTimeDataStreamer(client, project_id, dataset_id)
        self.alert_manager = AlertManager()
        self.health_monitor = SystemHealthMonitor(client, project_id, dataset_id)
        
        # Setup default alert configurations
        self._setup_default_alerts()
        
        # Setup data streaming callback
        self.data_streamer.add_callback(self._process_new_data)
        
    def _setup_default_alerts(self):
        """Setup default alert configurations"""
        default_alerts = [
            AlertConfig(
                metric_name='overall_quality_score',
                threshold_value=70.0,
                condition='below',
                severity='warning',
                cooldown_minutes=10
            ),
            AlertConfig(
                metric_name='overall_quality_score',
                threshold_value=50.0,
                condition='below',
                severity='critical',
                cooldown_minutes=5
            ),
            AlertConfig(
                metric_name='processing_rate',
                threshold_value=10.0,
                condition='below',
                severity='warning',
                cooldown_minutes=15
            )
        ]
        
        for alert_config in default_alerts:
            self.alert_manager.add_alert_config(alert_config)
    
    def _process_new_data(self, metrics: List[LiveMetric]):
        """Process new streaming data"""
        # Check for alerts
        new_alerts = self.alert_manager.check_alerts(metrics)
        
        if new_alerts:
            logger.info(f"Generated {len(new_alerts)} new alerts")
            
    def start_monitoring(self, stream_interval: int = 30):
        """Start real-time monitoring"""
        self.data_streamer.start_streaming(stream_interval)
        logger.info("Real-time monitoring started")
        
    def stop_monitoring(self):
        """Stop real-time monitoring"""
        self.data_streamer.stop_streaming()
        logger.info("Real-time monitoring stopped")
        
    def get_live_dashboard_data(self) -> Dict[str, Any]:
        """Get comprehensive live dashboard data"""
        try:
            # Get recent metrics
            recent_metrics = self.data_streamer.get_recent_data(minutes=30)
            
            # Get system health
            system_health = self.health_monitor.check_system_health()
            
            # Get active alerts
            active_alerts = self.alert_manager.get_active_alerts()
            
            # Process metrics for dashboard
            dashboard_metrics = self._process_metrics_for_dashboard(recent_metrics)
            
            return {
                'timestamp': datetime.now(),
                'live_metrics': dashboard_metrics,
                'system_health': system_health,
                'active_alerts': [asdict(alert) for alert in active_alerts],
                'alert_count': len(active_alerts),
                'monitoring_status': 'active' if self.data_streamer.streaming_active else 'inactive'
            }
            
        except Exception as e:
            logger.error(f"Error getting live dashboard data: {e}")
            return {
                'timestamp': datetime.now(),
                'error': str(e),
                'monitoring_status': 'error'
            }
    
    def _process_metrics_for_dashboard(self, metrics: List[LiveMetric]) -> Dict[str, Any]:
        """Process metrics for dashboard display"""
        processed = {
            'current_values': {},
            'trends': defaultdict(list),
            'categories': defaultdict(dict)
        }
        
        # Group metrics by name
        metrics_by_name = defaultdict(list)
        for metric in metrics:
            metrics_by_name[metric.metric_name].append(metric)
        
        # Process each metric type
        for metric_name, metric_list in metrics_by_name.items():
            # Sort by timestamp
            metric_list.sort(key=lambda x: x.timestamp)
            
            # Current value (latest)
            if metric_list:
                latest = metric_list[-1]
                processed['current_values'][metric_name] = {
                    'value': latest.value,
                    'timestamp': latest.timestamp,
                    'category': latest.category
                }
            
            # Trend data
            for metric in metric_list:
                processed['trends'][metric_name].append({
                    'timestamp': metric.timestamp,
                    'value': metric.value,
                    'category': metric.category
                })
            
            # Category breakdown
            category_values = defaultdict(list)
            for metric in metric_list:
                if metric.category:
                    category_values[metric.category].append(metric.value)
            
            for category, values in category_values.items():
                if values:
                    processed['categories'][metric_name][category] = {
                        'current_value': values[-1],
                        'avg_value': sum(values) / len(values),
                        'count': len(values)
                    }
        
        return processed
    
    def add_custom_alert(self, metric_name: str, threshold: float, 
                        condition: str, severity: str = 'warning') -> bool:
        """Add custom alert configuration"""
        try:
            config = AlertConfig(
                metric_name=metric_name,
                threshold_value=threshold,
                condition=condition,
                severity=severity
            )
            self.alert_manager.add_alert_config(config)
            return True
        except Exception as e:
            logger.error(f"Error adding custom alert: {e}")
            return False
    
    def get_historical_performance(self, hours: int = 24) -> Dict[str, Any]:
        """Get historical performance data"""
        return {
            'system_health_history': self.health_monitor.get_performance_history(hours),
            'alert_history': [asdict(alert) for alert in self.alert_manager.get_alert_history(hours)]
        }

# Convenience functions for easy integration
def create_monitoring_manager(client: Any = None, 
                            project_id: str = "proj-product-qc-gmumabigq",
                            dataset_id: str = "product_qc") -> RealTimeMonitoringManager:
    """Create monitoring manager with default settings"""
    if client is None and BIGQUERY_AVAILABLE:
        client = bigquery.Client(project=project_id)
    
    return RealTimeMonitoringManager(client, project_id, dataset_id)

def get_live_monitoring_data(client: Any = None,
                           project_id: str = "proj-product-qc-gmumabigq",
                           dataset_id: str = "product_qc") -> Dict[str, Any]:
    """Get live monitoring data (convenience function)"""
    manager = create_monitoring_manager(client, project_id, dataset_id)
    return manager.get_live_dashboard_data()