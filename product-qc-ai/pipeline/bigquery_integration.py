"""
BigQuery Integration Module

This module provides a centralized interface for all BigQuery operations
in the Product Quality Control AI System. It handles data querying,
writing, and management operations with proper error handling and
connection management.
"""

import os
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union
import logging

# BigQuery imports with error handling
try:
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound, GoogleCloudError
    BIGQUERY_AVAILABLE = True
except ImportError:
    print("Warning: Google Cloud BigQuery not available")
    bigquery = None
    NotFound = Exception
    GoogleCloudError = Exception
    BIGQUERY_AVAILABLE = False

class BigQueryIntegration:
    """
    Centralized BigQuery integration class for the Quality Control System.
    
    This class provides a unified interface for all BigQuery operations including:
    - Data querying and retrieval
    - Data writing and table management
    - Connection health monitoring
    - Cost and performance optimization
    """
    
    def __init__(self, project_id: Optional[str] = None, 
                 dataset_id: str = "product_qc_dataset"):
        """
        Initialize BigQuery integration.
        
        Args:
            project_id: Google Cloud project ID. If None, uses default from environment
            dataset_id: Default dataset ID for operations
        """
        self.project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT", "proj-product-qc-gmumabigq")
        self.dataset_id = dataset_id
        self.client = None
        self.logger = logging.getLogger(__name__)
        
        # Initialize connection
        self._initialize_client()
    
    def _initialize_client(self) -> None:
        """Initialize BigQuery client with error handling."""
        if not BIGQUERY_AVAILABLE:
            self.logger.warning("BigQuery client not available - missing dependencies")
            return
        
        try:
            self.client = bigquery.Client(project=self.project_id)
            self.logger.info(f"BigQuery client initialized for project: {self.project_id}")
        except Exception as e:
            self.logger.error(f"Failed to initialize BigQuery client: {str(e)}")
            self.client = None
    
    def is_available(self) -> bool:
        """Check if BigQuery integration is available and properly configured."""
        return BIGQUERY_AVAILABLE and self.client is not None
    
    def check_connection(self) -> Dict[str, Any]:
        """
        Check BigQuery connection health and performance.
        
        Returns:
            Dict with connection status, response time, and details
        """
        if not self.is_available():
            return {
                'status': 'unavailable',
                'healthy': False,
                'message': 'BigQuery client not available',
                'response_time': None
            }
        
        try:
            start_time = datetime.now()
            
            # Simple query to test connection
            query = "SELECT 1 as test_connection"
            query_job = self.client.query(query)
            list(query_job.result())  # Force execution
            
            response_time = (datetime.now() - start_time).total_seconds()
            
            return {
                'status': 'healthy',
                'healthy': True,
                'response_time': response_time,
                'message': f'Connection successful - {response_time:.2f}s response time'
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'healthy': False,
                'message': f'Connection failed: {str(e)}',
                'response_time': None
            }
    
    def execute_query(self, query: str, parameters: Optional[List] = None) -> pd.DataFrame:
        """
        Execute a BigQuery SQL query and return results as DataFrame.
        
        Args:
            query: SQL query string
            parameters: Optional query parameters
            
        Returns:
            pandas DataFrame with query results
            
        Raises:
            Exception: If query execution fails
        """
        if not self.is_available():
            raise Exception("BigQuery client not available")
        
        try:
            self.logger.info(f"Executing query: {query[:100]}...")
            
            job_config = bigquery.QueryJobConfig()
            if parameters:
                job_config.query_parameters = parameters
            
            query_job = self.client.query(query, job_config=job_config)
            df = query_job.to_dataframe()
            
            self.logger.info(f"Query executed successfully, returned {len(df)} rows")
            return df
            
        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def write_dataframe(self, df: pd.DataFrame, table_name: str, 
                       write_disposition: str = "WRITE_TRUNCATE",
                       create_table: bool = True) -> bool:
        """
        Write DataFrame to BigQuery table.
        
        Args:
            df: pandas DataFrame to write
            table_name: Target table name
            write_disposition: How to handle existing data (WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY)
            create_table: Whether to create table if it doesn't exist
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_available():
            self.logger.error("BigQuery client not available")
            return False
        
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED if create_table else bigquery.CreateDisposition.CREATE_NEVER
            )
            
            job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for completion
            
            self.logger.info(f"Successfully wrote {len(df)} rows to {table_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to write DataFrame to {table_name}: {str(e)}")
            return False
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a BigQuery table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table information or None if table doesn't exist
        """
        if not self.is_available():
            return None
        
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            table = self.client.get_table(table_id)
            
            return {
                'table_id': table.table_id,
                'project': table.project,
                'dataset_id': table.dataset_id,
                'created': table.created,
                'modified': table.modified,
                'num_rows': table.num_rows,
                'num_bytes': table.num_bytes,
                'schema': [{'name': field.name, 'type': field.field_type, 'mode': field.mode} 
                          for field in table.schema]
            }
            
        except NotFound:
            self.logger.warning(f"Table {table_name} not found")
            return None
        except Exception as e:
            self.logger.error(f"Failed to get table info for {table_name}: {str(e)}")
            return None
    
    def list_tables(self) -> List[str]:
        """
        List all tables in the dataset.
        
        Returns:
            List of table names
        """
        if not self.is_available():
            return []
        
        try:
            dataset_id = f"{self.project_id}.{self.dataset_id}"
            tables = self.client.list_tables(dataset_id)
            return [table.table_id for table in tables]
            
        except Exception as e:
            self.logger.error(f"Failed to list tables: {str(e)}")
            return []
    
    def delete_table(self, table_name: str) -> bool:
        """
        Delete a BigQuery table.
        
        Args:
            table_name: Name of the table to delete
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_available():
            return False
        
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            self.client.delete_table(table_id, not_found_ok=True)
            self.logger.info(f"Successfully deleted table {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to delete table {table_name}: {str(e)}")
            return False
    
    # Quality Control Specific Methods
    
    def get_quality_metrics(self, start_date: Optional[datetime] = None, 
                           end_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Get quality control metrics from BigQuery.
        
        Args:
            start_date: Start date for metrics (defaults to 24 hours ago)
            end_date: End date for metrics (defaults to now)
            
        Returns:
            DataFrame with quality metrics
        """
        if start_date is None:
            start_date = datetime.now() - timedelta(days=1)
        if end_date is None:
            end_date = datetime.now()
        
        query = f"""
        SELECT 
            TIMESTAMP_TRUNC(timestamp, HOUR) as hour,
            COUNT(*) as total_products,
            COUNTIF(quality_score >= 0.9) as high_quality_products,
            COUNTIF(quality_score < 0.7) as low_quality_products,
            AVG(quality_score) as avg_quality_score,
            STDDEV(quality_score) as quality_score_stddev,
            COUNT(DISTINCT product_category) as categories_processed
        FROM `{self.project_id}.{self.dataset_id}.quality_results`
        WHERE timestamp BETWEEN @start_date AND @end_date
        GROUP BY hour
        ORDER BY hour DESC
        """
        
        parameters = [
            bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
            bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date)
        ]
        
        try:
            return self.execute_query(query, parameters)
        except Exception as e:
            self.logger.error(f"Failed to get quality metrics: {str(e)}")
            return pd.DataFrame()
    
    def get_defect_analysis(self, time_window_hours: int = 24) -> pd.DataFrame:
        """
        Get defect analysis data.
        
        Args:
            time_window_hours: Time window for analysis in hours
            
        Returns:
            DataFrame with defect analysis
        """
        query = f"""
        SELECT 
            product_category,
            defect_type,
            COUNT(*) as defect_count,
            AVG(severity_score) as avg_severity,
            ARRAY_AGG(DISTINCT product_id LIMIT 5) as sample_product_ids
        FROM `{self.project_id}.{self.dataset_id}.defect_results`
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
        GROUP BY product_category, defect_type
        ORDER BY defect_count DESC
        """
        
        parameters = [
            bigquery.ScalarQueryParameter("hours", "INT64", time_window_hours)
        ]
        
        try:
            return self.execute_query(query, parameters)
        except Exception as e:
            self.logger.error(f"Failed to get defect analysis: {str(e)}")
            return pd.DataFrame()
    
    def get_production_stats(self, date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Get production statistics for a specific date.
        
        Args:
            date: Date for statistics (defaults to today)
            
        Returns:
            Dictionary with production statistics
        """
        if date is None:
            date = datetime.now().date()
        
        query = f"""
        SELECT 
            COUNT(*) as total_processed,
            COUNT(DISTINCT product_category) as categories,
            AVG(processing_time_ms) as avg_processing_time,
            SUM(CASE WHEN quality_score >= 0.9 THEN 1 ELSE 0 END) as passed_quality,
            SUM(CASE WHEN quality_score < 0.7 THEN 1 ELSE 0 END) as failed_quality
        FROM `{self.project_id}.{self.dataset_id}.processing_log`
        WHERE DATE(timestamp) = @target_date
        """
        
        parameters = [
            bigquery.ScalarQueryParameter("target_date", "DATE", date)
        ]
        
        try:
            df = self.execute_query(query, parameters)
            if not df.empty:
                return df.iloc[0].to_dict()
            return {}
        except Exception as e:
            self.logger.error(f"Failed to get production stats: {str(e)}")
            return {}
    
    def get_alerts_data(self, severity: Optional[str] = None, 
                       limit: int = 100) -> pd.DataFrame:
        """
        Get recent alerts from BigQuery.
        
        Args:
            severity: Filter by severity level (critical, warning, info)
            limit: Maximum number of alerts to return
            
        Returns:
            DataFrame with alerts data
        """
        where_clause = ""
        parameters = []
        
        if severity:
            where_clause = "WHERE severity = @severity"
            parameters.append(
                bigquery.ScalarQueryParameter("severity", "STRING", severity)
            )
        
        query = f"""
        SELECT 
            alert_id,
            timestamp,
            severity,
            alert_type,
            message,
            product_id,
            resolved_at
        FROM `{self.project_id}.{self.dataset_id}.alerts`
        {where_clause}
        ORDER BY timestamp DESC
        LIMIT @limit
        """
        
        parameters.append(
            bigquery.ScalarQueryParameter("limit", "INT64", limit)
        )
        
        try:
            return self.execute_query(query, parameters)
        except Exception as e:
            self.logger.error(f"Failed to get alerts data: {str(e)}")
            return pd.DataFrame()
    
    def write_quality_result(self, product_id: str, quality_score: float, 
                           category: str, timestamp: Optional[datetime] = None,
                           metadata: Optional[Dict] = None) -> bool:
        """
        Write a quality control result to BigQuery.
        
        Args:
            product_id: Product identifier
            quality_score: Quality score (0.0 to 1.0)
            category: Product category
            timestamp: Timestamp of the result
            metadata: Additional metadata
            
        Returns:
            True if successful
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        data = {
            'product_id': [product_id],
            'quality_score': [quality_score],
            'product_category': [category],
            'timestamp': [timestamp],
            'metadata': [json.dumps(metadata) if metadata else None]
        }
        
        df = pd.DataFrame(data)
        return self.write_dataframe(df, 'quality_results', write_disposition="WRITE_APPEND")
    
    def create_alert(self, severity: str, alert_type: str, message: str,
                    product_id: Optional[str] = None, 
                    metadata: Optional[Dict] = None) -> bool:
        """
        Create an alert in BigQuery.
        
        Args:
            severity: Alert severity (critical, warning, info)
            alert_type: Type of alert
            message: Alert message
            product_id: Associated product ID
            metadata: Additional metadata
            
        Returns:
            True if successful
        """
        import uuid
        
        data = {
            'alert_id': [str(uuid.uuid4())],
            'timestamp': [datetime.now()],
            'severity': [severity],
            'alert_type': [alert_type],
            'message': [message],
            'product_id': [product_id],
            'metadata': [json.dumps(metadata) if metadata else None],
            'resolved_at': [None]
        }
        
        df = pd.DataFrame(data)
        return self.write_dataframe(df, 'alerts', write_disposition="WRITE_APPEND")
    
    def get_system_health_metrics(self) -> Dict[str, Any]:
        """
        Get system health metrics from BigQuery.
        
        Returns:
            Dictionary with system health information
        """
        try:
            # Get processing volume in last hour
            volume_query = f"""
            SELECT COUNT(*) as hourly_volume
            FROM `{self.project_id}.{self.dataset_id}.processing_log`
            WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
            """
            
            # Get error rate in last hour
            error_query = f"""
            SELECT 
                COUNT(*) as total_processed,
                COUNTIF(status = 'error') as error_count
            FROM `{self.project_id}.{self.dataset_id}.processing_log`
            WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
            """
            
            volume_df = self.execute_query(volume_query)
            error_df = self.execute_query(error_query)
            
            hourly_volume = volume_df.iloc[0]['hourly_volume'] if not volume_df.empty else 0
            error_data = error_df.iloc[0] if not error_df.empty else {'total_processed': 0, 'error_count': 0}
            
            error_rate = (error_data['error_count'] / max(error_data['total_processed'], 1)) * 100
            
            return {
                'hourly_processing_volume': hourly_volume,
                'error_rate_percent': error_rate,
                'total_processed_last_hour': error_data['total_processed'],
                'errors_last_hour': error_data['error_count']
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get system health metrics: {str(e)}")
            return {
                'hourly_processing_volume': 0,
                'error_rate_percent': 0,
                'total_processed_last_hour': 0,
                'errors_last_hour': 0
            }
    
    def cleanup_old_data(self, table_name: str, days_to_keep: int = 30) -> bool:
        """
        Clean up old data from a table.
        
        Args:
            table_name: Name of the table to clean
            days_to_keep: Number of days of data to keep
            
        Returns:
            True if successful
        """
        query = f"""
        DELETE FROM `{self.project_id}.{self.dataset_id}.{table_name}`
        WHERE timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
        """
        
        parameters = [
            bigquery.ScalarQueryParameter("days", "INT64", days_to_keep)
        ]
        
        try:
            self.execute_query(query, parameters)
            self.logger.info(f"Successfully cleaned old data from {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to clean old data from {table_name}: {str(e)}")
            return False

# Convenience functions for backward compatibility
def get_bigquery_client(project_id: Optional[str] = None) -> Optional[Any]:
    """Get a BigQuery client instance."""
    if not BIGQUERY_AVAILABLE:
        return None
    
    try:
        return bigquery.Client(project=project_id or "proj-product-qc-gmumabigq")
    except Exception:
        return None

def execute_bigquery_query(query: str, project_id: Optional[str] = None) -> pd.DataFrame:
    """Execute a BigQuery query and return results as DataFrame."""
    integration = BigQueryIntegration(project_id)
    return integration.execute_query(query)