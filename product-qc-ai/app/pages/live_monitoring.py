"""Live Monitoring page for real-time quality monitoring."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import time
from typing import Dict, Any, List, Optional

# Import pipeline components
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from pipeline.realtime_monitoring import RealTimeMonitoringManager
from pipeline.bigquery_integration import BigQueryIntegration
from utils.ui_helpers import (
    create_metric_card, create_status_indicator, format_large_number,
    check_auto_refresh, display_alert, create_gauge_chart, create_time_series_chart
)
from components.common_components import (
    render_header_section, render_kpi_metrics, render_status_overview,
    render_alerts_panel, render_performance_summary, render_quick_actions_panel
)
from config.app_config import AUTO_REFRESH_CONFIG

class LiveMonitoringPage:
    """Live Monitoring page class."""
    
    def __init__(self):
        """Initialize Live Monitoring page."""
        self.monitoring_manager = None
        self.bq_integration = None
        self._initialize_components()
    
    def _initialize_components(self):
        """Initialize required components."""
        try:
            self.monitoring_manager = RealTimeMonitoringManager()
            self.bq_integration = BigQueryIntegration()
        except Exception as e:
            st.error(f"Failed to initialize monitoring components: {str(e)}")
    
    def render(self):
        """Render the Live Monitoring page."""
        render_header_section(
            title="Live Monitoring",
            description="Real-time quality monitoring and system alerts",
            icon="🔴"
        )
        
        # Auto-refresh setup
        if check_auto_refresh("live_monitoring"):
            st.experimental_rerun()
        
        # Check if components are available
        if not self.monitoring_manager:
            st.error("❌ Real-time monitoring is not available. Please check system configuration.")
            return
        
        # Main layout
        self._render_control_panel()
        self._render_real_time_metrics()
        self._render_alerts_section()
        self._render_live_charts()
        self._render_system_health()
        self._render_data_streams()
    
    def _render_control_panel(self):
        """Render monitoring control panel."""
        st.subheader("🎛️ Control Panel")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            auto_refresh = st.checkbox(
                "Auto Refresh", 
                value=st.session_state.get("auto_refresh_enabled", True),
                key="live_auto_refresh"
            )
            st.session_state.auto_refresh_enabled = auto_refresh
        
        with col2:
            refresh_interval = st.selectbox(
                "Refresh Interval",
                options=[1, 5, 10, 30, 60],
                index=1,  # Default to 5 seconds
                format_func=lambda x: f"{x}s",
                key="refresh_interval"
            )
        
        with col3:
            if st.button("🔄 Manual Refresh", key="manual_refresh"):
                st.experimental_rerun()
        
        with col4:
            monitoring_status = self._get_monitoring_status()
            status_indicator = create_status_indicator(
                monitoring_status, 
                "Monitoring"
            )
            st.markdown(f"**Status:** {status_indicator}")
        
        st.markdown("---")
    
    def _render_real_time_metrics(self):
        """Render real-time key metrics."""
        st.subheader("📊 Real-Time Metrics")
        
        try:
            # Get current metrics from monitoring manager
            current_metrics = self._get_current_metrics()
            
            # Create metrics grid
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                create_metric_card(
                    title="Products Processed",
                    value=format_large_number(current_metrics.get("products_processed", 0)),
                    delta=current_metrics.get("products_processed_delta"),
                    delta_color="normal"
                )
            
            with col2:
                defect_rate = current_metrics.get("defect_rate", 0)
                create_metric_card(
                    title="Defect Rate",
                    value=f"{defect_rate:.2f}%",
                    delta=current_metrics.get("defect_rate_delta"),
                    delta_color="inverse" if defect_rate > 5 else "normal"
                )
            
            with col3:
                create_metric_card(
                    title="Active Alerts",
                    value=current_metrics.get("active_alerts", 0),
                    delta=current_metrics.get("active_alerts_delta"),
                    delta_color="inverse" if current_metrics.get("active_alerts", 0) > 0 else "normal"
                )
            
            with col4:
                processing_speed = current_metrics.get("processing_speed", 0)
                create_metric_card(
                    title="Processing Speed",
                    value=f"{format_large_number(processing_speed)}/min",
                    delta=current_metrics.get("processing_speed_delta"),
                    delta_color="normal"
                )
            
            with col5:
                system_health = current_metrics.get("system_health", 0)
                create_metric_card(
                    title="System Health",
                    value=f"{system_health:.1f}%",
                    delta=current_metrics.get("system_health_delta"),
                    delta_color="inverse" if system_health < 90 else "normal"
                )
        
        except Exception as e:
            st.error(f"❌ Failed to load real-time metrics: {str(e)}")
        
        st.markdown("---")
    
    def _render_alerts_section(self):
        """Render alerts section."""
        try:
            alerts = self._get_recent_alerts()
            render_alerts_panel(alerts, max_alerts=5)
            
            # Alert summary
            if alerts:
                alert_counts = self._count_alerts_by_severity(alerts)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Critical", alert_counts.get("critical", 0))
                with col2:
                    st.metric("Warning", alert_counts.get("warning", 0))
                with col3:
                    st.metric("Info", alert_counts.get("info", 0))
        
        except Exception as e:
            st.error(f"❌ Failed to load alerts: {str(e)}")
        
        st.markdown("---")
    
    def _render_live_charts(self):
        """Render live monitoring charts."""
        st.subheader("📈 Live Data Streams")
        
        try:
            # Create tabs for different chart types
            tab1, tab2, tab3, tab4 = st.tabs([
                "🔍 Quality Metrics", 
                "⚡ Performance", 
                "📊 Production Stats",
                "🌡️ Sensor Data"
            ])
            
            with tab1:
                self._render_quality_metrics_chart()
            
            with tab2:
                self._render_performance_chart()
            
            with tab3:
                self._render_production_stats_chart()
            
            with tab4:
                self._render_sensor_data_chart()
        
        except Exception as e:
            st.error(f"❌ Failed to render live charts: {str(e)}")
    
    def _render_quality_metrics_chart(self):
        """Render quality metrics live chart."""
        # Get quality metrics data
        quality_data = self._get_quality_metrics_data()
        
        if quality_data.empty:
            st.info("📭 No quality metrics data available")
            return
        
        # Create multi-line chart
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=['Defect Rate', 'Quality Score', 'Pass Rate', 'Inspection Speed'],
            vertical_spacing=0.1,
            horizontal_spacing=0.1
        )
        
        # Defect Rate
        fig.add_trace(
            go.Scatter(
                x=quality_data.index,
                y=quality_data.get('defect_rate', []),
                mode='lines+markers',
                name='Defect Rate',
                line=dict(color='red', width=2)
            ),
            row=1, col=1
        )
        
        # Quality Score
        fig.add_trace(
            go.Scatter(
                x=quality_data.index,
                y=quality_data.get('quality_score', []),
                mode='lines+markers',
                name='Quality Score',
                line=dict(color='green', width=2)
            ),
            row=1, col=2
        )
        
        # Pass Rate
        fig.add_trace(
            go.Scatter(
                x=quality_data.index,
                y=quality_data.get('pass_rate', []),
                mode='lines+markers',
                name='Pass Rate',
                line=dict(color='blue', width=2)
            ),
            row=2, col=1
        )
        
        # Inspection Speed
        fig.add_trace(
            go.Scatter(
                x=quality_data.index,
                y=quality_data.get('inspection_speed', []),
                mode='lines+markers',
                name='Inspection Speed',
                line=dict(color='orange', width=2)
            ),
            row=2, col=2
        )
        
        fig.update_layout(
            height=600,
            showlegend=False,
            title_text="Quality Metrics - Live Stream"
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_performance_chart(self):
        """Render system performance live chart."""
        performance_data = self._get_performance_data()
        
        if performance_data.empty:
            st.info("📭 No performance data available")
            return
        
        # Create gauge charts for current performance
        col1, col2, col3 = st.columns(3)
        
        with col1:
            cpu_gauge = create_gauge_chart(
                value=performance_data.get('cpu_usage', [0])[-1] if not performance_data.empty else 0,
                title="CPU Usage (%)",
                max_val=100,
                threshold_good=70,
                threshold_warning=85
            )
            st.plotly_chart(cpu_gauge, use_container_width=True)
        
        with col2:
            memory_gauge = create_gauge_chart(
                value=performance_data.get('memory_usage', [0])[-1] if not performance_data.empty else 0,
                title="Memory Usage (%)",
                max_val=100,
                threshold_good=70,
                threshold_warning=85
            )
            st.plotly_chart(memory_gauge, use_container_width=True)
        
        with col3:
            response_time = performance_data.get('response_time', [0])[-1] if not performance_data.empty else 0
            response_gauge = create_gauge_chart(
                value=response_time,
                title="Response Time (ms)",
                max_val=2000,
                threshold_good=500,
                threshold_warning=1000
            )
            st.plotly_chart(response_gauge, use_container_width=True)
        
        # Time series chart
        if not performance_data.empty:
            fig = create_time_series_chart(
                df=performance_data,
                x_col='timestamp',
                y_col='throughput',
                title="System Throughput Over Time"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    def _render_production_stats_chart(self):
        """Render production statistics chart."""
        production_data = self._get_production_data()
        
        if production_data.empty:
            st.info("📭 No production data available")
            return
        
        # Production volume chart
        fig = px.area(
            production_data,
            x='timestamp',
            y='production_volume',
            title='Production Volume - Live Stream',
            color_discrete_sequence=['#1f77b4']
        )
        
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Production efficiency
        col1, col2 = st.columns(2)
        
        with col1:
            efficiency_data = production_data.get('efficiency', [])
            if efficiency_data:
                avg_efficiency = sum(efficiency_data) / len(efficiency_data)
                efficiency_gauge = create_gauge_chart(
                    value=avg_efficiency,
                    title="Production Efficiency (%)",
                    max_val=100
                )
                st.plotly_chart(efficiency_gauge, use_container_width=True)
        
        with col2:
            if 'downtime' in production_data.columns:
                downtime_chart = px.bar(
                    production_data.tail(10),
                    x='timestamp',
                    y='downtime',
                    title='Recent Downtime (minutes)'
                )
                downtime_chart.update_layout(height=300)
                st.plotly_chart(downtime_chart, use_container_width=True)
    
    def _render_sensor_data_chart(self):
        """Render sensor data chart."""
        sensor_data = self._get_sensor_data()
        
        if sensor_data.empty:
            st.info("📭 No sensor data available")
            return
        
        # Multi-sensor dashboard
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=['Temperature', 'Pressure', 'Vibration', 'Humidity']
        )
        
        sensors = ['temperature', 'pressure', 'vibration', 'humidity']
        colors = ['red', 'blue', 'green', 'orange']
        positions = [(1,1), (1,2), (2,1), (2,2)]
        
        for i, (sensor, color, pos) in enumerate(zip(sensors, colors, positions)):
            if sensor in sensor_data.columns:
                fig.add_trace(
                    go.Scatter(
                        x=sensor_data.index,
                        y=sensor_data[sensor],
                        mode='lines',
                        name=sensor.title(),
                        line=dict(color=color, width=2)
                    ),
                    row=pos[0], col=pos[1]
                )
        
        fig.update_layout(
            height=500,
            showlegend=False,
            title_text="Sensor Data - Live Stream"
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_system_health(self):
        """Render system health section."""
        st.subheader("💓 System Health")
        
        try:
            health_data = self._get_system_health_data()
            render_performance_summary(health_data)
            
            # Component status
            component_status = self._get_component_status()
            render_status_overview(component_status, "Component Status")
        
        except Exception as e:
            st.error(f"❌ Failed to load system health: {str(e)}")
    
    def _render_data_streams(self):
        """Render data streams information."""
        st.subheader("🌊 Data Streams")
        
        try:
            stream_info = self._get_stream_info()
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                create_metric_card(
                    title="Active Streams",
                    value=stream_info.get("active_streams", 0),
                    help_text="Number of active data streams"
                )
            
            with col2:
                create_metric_card(
                    title="Data Points/sec",
                    value=format_large_number(stream_info.get("data_rate", 0)),
                    help_text="Real-time data ingestion rate"
                )
            
            with col3:
                create_metric_card(
                    title="Buffer Usage",
                    value=f"{stream_info.get('buffer_usage', 0):.1f}%",
                    delta_color="inverse" if stream_info.get('buffer_usage', 0) > 80 else "normal"
                )
            
            with col4:
                create_metric_card(
                    title="Lag (ms)",
                    value=stream_info.get("lag_ms", 0),
                    delta_color="inverse" if stream_info.get('lag_ms', 0) > 1000 else "normal"
                )
        
        except Exception as e:
            st.error(f"❌ Failed to load stream information: {str(e)}")
    
    # Helper methods for data retrieval
    def _get_monitoring_status(self) -> str:
        """Get current monitoring status."""
        try:
            if self.monitoring_manager and hasattr(self.monitoring_manager, 'is_monitoring_active'):
                return "healthy" if self.monitoring_manager.is_monitoring_active() else "offline"
            return "unknown"
        except:
            return "offline"
    
    def _get_current_metrics(self) -> Dict[str, Any]:
        """Get current real-time metrics."""
        # This would normally fetch from the monitoring manager
        # For now, return sample data
        return {
            "products_processed": 15420,
            "products_processed_delta": "+5.2%",
            "defect_rate": 2.1,
            "defect_rate_delta": "-0.3%",
            "active_alerts": 3,
            "active_alerts_delta": "+1",
            "processing_speed": 450,
            "processing_speed_delta": "+2.1%",
            "system_health": 94.5,
            "system_health_delta": "+1.2%"
        }
    
    def _get_recent_alerts(self) -> List[Dict[str, Any]]:
        """Get recent alerts."""
        # Sample alerts data
        return [
            {
                "title": "High Defect Rate Detected",
                "message": "Defect rate exceeded threshold in Line A",
                "severity": "warning",
                "timestamp": "2024-01-15 10:30:45"
            },
            {
                "title": "System Performance Optimal",
                "message": "All systems operating within normal parameters",
                "severity": "info",
                "timestamp": "2024-01-15 10:25:12"
            }
        ]
    
    def _count_alerts_by_severity(self, alerts: List[Dict[str, Any]]) -> Dict[str, int]:
        """Count alerts by severity."""
        counts = {"critical": 0, "warning": 0, "info": 0}
        for alert in alerts:
            severity = alert.get("severity", "info").lower()
            if severity in counts:
                counts[severity] += 1
        return counts
    
    def _get_quality_metrics_data(self) -> pd.DataFrame:
        """Get quality metrics data for charts."""
        # Generate sample time series data
        timestamps = pd.date_range(
            start=datetime.now() - timedelta(hours=1),
            end=datetime.now(),
            freq='1min'
        )
        
        import numpy as np
        
        return pd.DataFrame({
            'defect_rate': np.random.normal(2.0, 0.5, len(timestamps)),
            'quality_score': np.random.normal(95, 2, len(timestamps)),
            'pass_rate': np.random.normal(97.5, 1, len(timestamps)),
            'inspection_speed': np.random.normal(450, 20, len(timestamps))
        }, index=timestamps)
    
    def _get_performance_data(self) -> pd.DataFrame:
        """Get performance data for charts."""
        timestamps = pd.date_range(
            start=datetime.now() - timedelta(hours=1),
            end=datetime.now(),
            freq='1min'
        )
        
        import numpy as np
        
        return pd.DataFrame({
            'timestamp': timestamps,
            'cpu_usage': np.random.normal(65, 10, len(timestamps)),
            'memory_usage': np.random.normal(70, 8, len(timestamps)),
            'response_time': np.random.normal(250, 50, len(timestamps)),
            'throughput': np.random.normal(1200, 100, len(timestamps))
        })
    
    def _get_production_data(self) -> pd.DataFrame:
        """Get production data for charts."""
        timestamps = pd.date_range(
            start=datetime.now() - timedelta(hours=2),
            end=datetime.now(),
            freq='5min'
        )
        
        import numpy as np
        
        return pd.DataFrame({
            'timestamp': timestamps,
            'production_volume': np.random.poisson(100, len(timestamps)),
            'efficiency': np.random.normal(85, 5, len(timestamps)),
            'downtime': np.random.exponential(2, len(timestamps))
        })
    
    def _get_sensor_data(self) -> pd.DataFrame:
        """Get sensor data for charts."""
        timestamps = pd.date_range(
            start=datetime.now() - timedelta(hours=1),
            end=datetime.now(),
            freq='30s'
        )
        
        import numpy as np
        
        return pd.DataFrame({
            'temperature': np.random.normal(22, 2, len(timestamps)),
            'pressure': np.random.normal(1013, 10, len(timestamps)),
            'vibration': np.random.normal(0.5, 0.1, len(timestamps)),
            'humidity': np.random.normal(45, 5, len(timestamps))
        }, index=timestamps)
    
    def _get_system_health_data(self) -> Dict[str, Any]:
        """Get system health data."""
        return {
            "cpu_usage": 65.2,
            "memory_usage": 72.1,
            "avg_response_time": 245,
            "throughput": 1250
        }
    
    def _get_component_status(self) -> Dict[str, str]:
        """Get component status."""
        return {
            "Database": "healthy",
            "API Gateway": "healthy",
            "ML Pipeline": "healthy",
            "Data Streams": "warning",
            "Alert System": "healthy"
        }
    
    def _get_stream_info(self) -> Dict[str, Any]:
        """Get data stream information."""
        return {
            "active_streams": 8,
            "data_rate": 2450,
            "buffer_usage": 67.3,
            "lag_ms": 125
        }

def render_live_monitoring_page():
    """Render the Live Monitoring page."""
    page = LiveMonitoringPage()
    page.render()