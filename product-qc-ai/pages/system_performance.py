"""
System Performance Page - Comprehensive system monitoring and diagnostics
Part of the Product Quality Control Agent modular Streamlit application.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import psutil
import time
import json
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path

# Add the parent directory to the path to import our modules
sys.path.append(str(Path(__file__).parent.parent))

try:
    from utils.ui_helpers import render_metric_card, render_chart_container, render_status_indicator
    from components.common_components import render_page_header, render_sidebar_filters, render_export_section
    from config.app_config import APP_CONFIG
except ImportError:
    # Fallback implementations
    def render_metric_card(title, value, delta=None, delta_color="normal"):
        st.metric(label=title, value=value, delta=delta)
    
    def render_chart_container(title, chart, height=400):
        st.subheader(title)
        st.plotly_chart(chart, use_container_width=True, height=height)
    
    def render_status_indicator(status, label="Status"):
        color = "🟢" if status == "healthy" else "🟡" if status == "warning" else "🔴"
        st.write(f"{color} {label}: {status}")
    
    def render_page_header(title, subtitle=None):
        st.title(title)
        if subtitle:
            st.markdown(subtitle)
    
    def render_sidebar_filters():
        return {}
    
    def render_export_section(data, filename_prefix="export"):
        if st.button("Export Data"):
            st.download_button(
                "Download CSV", 
                data.to_csv(index=False), 
                f"{filename_prefix}.csv"
            )
    
    APP_CONFIG = {
        "performance_thresholds": {
            "cpu_warning": 70,
            "cpu_critical": 85,
            "memory_warning": 80,
            "memory_critical": 90,
            "disk_warning": 85,
            "disk_critical": 95
        }
    }


class SystemPerformancePage:
    """Comprehensive system performance monitoring and diagnostics page."""
    
    def __init__(self):
        self.setup_page_config()
        
    def setup_page_config(self):
        """Configure page settings and initialize state."""
        if 'performance_data' not in st.session_state:
            st.session_state.performance_data = []
        if 'system_alerts' not in st.session_state:
            st.session_state.system_alerts = []
        if 'monitoring_enabled' not in st.session_state:
            st.session_state.monitoring_enabled = True
    
    def get_system_metrics(self):
        """Collect current system performance metrics."""
        try:
            # CPU metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_count = psutil.cpu_count()
            cpu_freq = psutil.cpu_freq()
            
            # Memory metrics
            memory = psutil.virtual_memory()
            swap = psutil.swap_memory()
            
            # Disk metrics
            disk = psutil.disk_usage('/')
            disk_io = psutil.disk_io_counters()
            
            # Network metrics
            network = psutil.net_io_counters()
            
            # Process metrics
            processes = len(psutil.pids())
            
            return {
                'timestamp': datetime.now(),
                'cpu': {
                    'percent': cpu_percent,
                    'count': cpu_count,
                    'frequency': cpu_freq.current if cpu_freq else 0
                },
                'memory': {
                    'total': memory.total,
                    'available': memory.available,
                    'percent': memory.percent,
                    'used': memory.used,
                    'swap_percent': swap.percent
                },
                'disk': {
                    'total': disk.total,
                    'used': disk.used,
                    'free': disk.free,
                    'percent': disk.percent,
                    'read_bytes': disk_io.read_bytes if disk_io else 0,
                    'write_bytes': disk_io.write_bytes if disk_io else 0
                },
                'network': {
                    'bytes_sent': network.bytes_sent,
                    'bytes_recv': network.bytes_recv,
                    'packets_sent': network.packets_sent,
                    'packets_recv': network.packets_recv
                },
                'processes': processes
            }
        except Exception as e:
            st.error(f"Error collecting system metrics: {str(e)}")
            return None
    
    def get_system_health_status(self, metrics):
        """Determine overall system health status."""
        if not metrics:
            return "unknown", "Unable to determine system status"
        
        cpu_status = self.get_threshold_status(
            metrics['cpu']['percent'], 
            APP_CONFIG['performance_thresholds']['cpu_warning'],
            APP_CONFIG['performance_thresholds']['cpu_critical']
        )
        
        memory_status = self.get_threshold_status(
            metrics['memory']['percent'],
            APP_CONFIG['performance_thresholds']['memory_warning'], 
            APP_CONFIG['performance_thresholds']['memory_critical']
        )
        
        disk_status = self.get_threshold_status(
            metrics['disk']['percent'],
            APP_CONFIG['performance_thresholds']['disk_warning'],
            APP_CONFIG['performance_thresholds']['disk_critical']
        )
        
        # Determine overall status
        if any(status == "critical" for status in [cpu_status, memory_status, disk_status]):
            return "critical", "System requires immediate attention"
        elif any(status == "warning" for status in [cpu_status, memory_status, disk_status]):
            return "warning", "System performance is degraded"
        else:
            return "healthy", "System is operating normally"
    
    def get_threshold_status(self, value, warning_threshold, critical_threshold):
        """Get status based on thresholds."""
        if value >= critical_threshold:
            return "critical"
        elif value >= warning_threshold:
            return "warning"
        else:
            return "healthy"
    
    def format_bytes(self, bytes_value):
        """Format bytes to human readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.2f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.2f} PB"
    
    def render_system_overview(self):
        """Render system health overview section."""
        st.header("🖥️ System Health Overview")
        
        metrics = self.get_system_metrics()
        if not metrics:
            st.error("Unable to collect system metrics")
            return
        
        # Get system health status
        health_status, health_message = self.get_system_health_status(metrics)
        
        # Create overview metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            status_color = {"healthy": "🟢", "warning": "🟡", "critical": "🔴"}.get(health_status, "⚪")
            render_metric_card(
                "System Status",
                f"{status_color} {health_status.title()}",
                None
            )
        
        with col2:
            render_metric_card(
                "CPU Usage",
                f"{metrics['cpu']['percent']:.1f}%",
                None
            )
        
        with col3:
            render_metric_card(
                "Memory Usage", 
                f"{metrics['memory']['percent']:.1f}%",
                None
            )
        
        with col4:
            render_metric_card(
                "Disk Usage",
                f"{metrics['disk']['percent']:.1f}%", 
                None
            )
        
        # Health status message
        if health_status == "critical":
            st.error(health_message)
        elif health_status == "warning":
            st.warning(health_message)
        else:
            st.success(health_message)
        
        # Store metrics for historical tracking
        if st.session_state.monitoring_enabled:
            st.session_state.performance_data.append(metrics)
            # Keep only last 100 readings
            if len(st.session_state.performance_data) > 100:
                st.session_state.performance_data.pop(0)
    
    def render_performance_metrics(self):
        """Render detailed performance metrics section."""
        st.header("📊 Performance Metrics")
        
        metrics = self.get_system_metrics()
        if not metrics:
            return
        
        # CPU Metrics
        with st.expander("🔧 CPU Performance", expanded=True):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                render_metric_card("CPU Usage", f"{metrics['cpu']['percent']:.1f}%")
            with col2:
                render_metric_card("CPU Cores", f"{metrics['cpu']['count']}")
            with col3:
                render_metric_card("CPU Frequency", f"{metrics['cpu']['frequency']:.0f} MHz")
            
            # CPU usage gauge
            fig_cpu = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=metrics['cpu']['percent'],
                domain={'x': [0, 1], 'y': [0, 1]},
                title={'text': "CPU Usage %"},
                delta={'reference': 50},
                gauge={
                    'axis': {'range': [None, 100]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, 50], 'color': "lightgray"},
                        {'range': [50, 80], 'color': "yellow"},
                        {'range': [80, 100], 'color': "red"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 90
                    }
                }
            ))
            fig_cpu.update_layout(height=300)
            st.plotly_chart(fig_cpu, use_container_width=True)
        
        # Memory Metrics
        with st.expander("💾 Memory Performance", expanded=True):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                render_metric_card(
                    "Memory Usage", 
                    f"{metrics['memory']['percent']:.1f}%"
                )
            with col2:
                render_metric_card(
                    "Used Memory", 
                    self.format_bytes(metrics['memory']['used'])
                )
            with col3:
                render_metric_card(
                    "Available Memory",
                    self.format_bytes(metrics['memory']['available'])
                )
            
            # Memory usage bar chart
            memory_data = pd.DataFrame({
                'Type': ['Used', 'Available'],
                'Size (GB)': [
                    metrics['memory']['used'] / (1024**3),
                    metrics['memory']['available'] / (1024**3)
                ]
            })
            
            fig_memory = px.bar(
                memory_data, 
                x='Type', 
                y='Size (GB)',
                title='Memory Usage Distribution',
                color='Type',
                color_discrete_map={'Used': '#ff6b6b', 'Available': '#4ecdc4'}
            )
            fig_memory.update_layout(height=300)
            st.plotly_chart(fig_memory, use_container_width=True)
        
        # Disk Metrics
        with st.expander("💽 Disk Performance", expanded=True):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                render_metric_card(
                    "Disk Usage",
                    f"{metrics['disk']['percent']:.1f}%"
                )
            with col2:
                render_metric_card(
                    "Used Space",
                    self.format_bytes(metrics['disk']['used'])
                )
            with col3:
                render_metric_card(
                    "Free Space", 
                    self.format_bytes(metrics['disk']['free'])
                )
            
            # Disk usage pie chart
            disk_data = pd.DataFrame({
                'Type': ['Used', 'Free'],
                'Size': [metrics['disk']['used'], metrics['disk']['free']]
            })
            
            fig_disk = px.pie(
                disk_data,
                values='Size',
                names='Type', 
                title='Disk Space Distribution',
                color_discrete_map={'Used': '#ff9f43', 'Free': '#10ac84'}
            )
            fig_disk.update_layout(height=300)
            st.plotly_chart(fig_disk, use_container_width=True)
    
    def render_historical_trends(self):
        """Render historical performance trends."""
        st.header("📈 Performance Trends")
        
        if not st.session_state.performance_data:
            st.info("No historical data available. Performance data will appear as the system runs.")
            return
        
        # Convert performance data to DataFrame
        df_performance = pd.DataFrame([
            {
                'timestamp': metric['timestamp'],
                'cpu_percent': metric['cpu']['percent'],
                'memory_percent': metric['memory']['percent'],
                'disk_percent': metric['disk']['percent']
            }
            for metric in st.session_state.performance_data
        ])
        
        # Time series chart
        fig_trends = make_subplots(
            rows=3, cols=1,
            subplot_titles=['CPU Usage %', 'Memory Usage %', 'Disk Usage %'],
            vertical_spacing=0.08
        )
        
        # CPU trend
        fig_trends.add_trace(
            go.Scatter(
                x=df_performance['timestamp'],
                y=df_performance['cpu_percent'],
                mode='lines+markers',
                name='CPU %',
                line=dict(color='#3498db')
            ),
            row=1, col=1
        )
        
        # Memory trend
        fig_trends.add_trace(
            go.Scatter(
                x=df_performance['timestamp'],
                y=df_performance['memory_percent'],
                mode='lines+markers',
                name='Memory %',
                line=dict(color='#e74c3c')
            ),
            row=2, col=1
        )
        
        # Disk trend
        fig_trends.add_trace(
            go.Scatter(
                x=df_performance['timestamp'],
                y=df_performance['disk_percent'],
                mode='lines+markers',
                name='Disk %',
                line=dict(color='#f39c12')
            ),
            row=3, col=1
        )
        
        fig_trends.update_layout(
            height=600,
            title_text="System Performance Trends",
            showlegend=False
        )
        
        # Add threshold lines
        thresholds = APP_CONFIG['performance_thresholds']
        for i, (metric, warning, critical) in enumerate([
            ('cpu', thresholds['cpu_warning'], thresholds['cpu_critical']),
            ('memory', thresholds['memory_warning'], thresholds['memory_critical']), 
            ('disk', thresholds['disk_warning'], thresholds['disk_critical'])
        ]):
            fig_trends.add_hline(
                y=warning, line_dash="dash", line_color="orange",
                row=i+1, col=1, annotation_text="Warning"
            )
            fig_trends.add_hline(
                y=critical, line_dash="dash", line_color="red", 
                row=i+1, col=1, annotation_text="Critical"
            )
        
        st.plotly_chart(fig_trends, use_container_width=True)
        
        # Performance statistics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.subheader("CPU Statistics")
            st.write(f"Average: {df_performance['cpu_percent'].mean():.1f}%")
            st.write(f"Peak: {df_performance['cpu_percent'].max():.1f}%")
            st.write(f"Min: {df_performance['cpu_percent'].min():.1f}%")
        
        with col2:
            st.subheader("Memory Statistics")
            st.write(f"Average: {df_performance['memory_percent'].mean():.1f}%")
            st.write(f"Peak: {df_performance['memory_percent'].max():.1f}%")
            st.write(f"Min: {df_performance['memory_percent'].min():.1f}%")
        
        with col3:
            st.subheader("Disk Statistics")
            st.write(f"Average: {df_performance['disk_percent'].mean():.1f}%")
            st.write(f"Peak: {df_performance['disk_percent'].max():.1f}%")
            st.write(f"Min: {df_performance['disk_percent'].min():.1f}%")
    
    def render_system_diagnostics(self):
        """Render system diagnostics and troubleshooting section."""
        st.header("🔧 System Diagnostics")
        
        # System Information
        with st.expander("ℹ️ System Information", expanded=True):
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Platform Info")
                st.write(f"**OS:** {os.name}")
                st.write(f"**Platform:** {sys.platform}")
                st.write(f"**Python Version:** {sys.version}")
                st.write(f"**PID:** {os.getpid()}")
            
            with col2:
                st.subheader("Resource Limits")
                try:
                    import resource
                    st.write(f"**Max Memory:** {self.format_bytes(resource.getrlimit(resource.RLIMIT_AS)[1])}")
                    st.write(f"**Max File Size:** {self.format_bytes(resource.getrlimit(resource.RLIMIT_FSIZE)[1])}")
                except:
                    st.write("Resource limits not available")
        
        # Process Information
        with st.expander("⚙️ Process Information"):
            try:
                current_process = psutil.Process()
                process_info = {
                    'PID': current_process.pid,
                    'Parent PID': current_process.ppid(),
                    'Status': current_process.status(),
                    'Created': datetime.fromtimestamp(current_process.create_time()).strftime('%Y-%m-%d %H:%M:%S'),
                    'Memory Info': self.format_bytes(current_process.memory_info().rss),
                    'CPU Times': f"User: {current_process.cpu_times().user:.2f}s, System: {current_process.cpu_times().system:.2f}s",
                    'Threads': current_process.num_threads(),
                    'Open Files': len(current_process.open_files()) if current_process.open_files() else 0
                }
                
                for key, value in process_info.items():
                    st.write(f"**{key}:** {value}")
                    
            except Exception as e:
                st.error(f"Error getting process information: {str(e)}")
        
        # System Health Checks
        with st.expander("🏥 Health Checks"):
            if st.button("Run System Health Check"):
                with st.spinner("Running health checks..."):
                    results = self.run_health_checks()
                    
                    for check_name, result in results.items():
                        if result['status'] == 'pass':
                            st.success(f"✅ {check_name}: {result['message']}")
                        elif result['status'] == 'warning':
                            st.warning(f"⚠️ {check_name}: {result['message']}")
                        else:
                            st.error(f"❌ {check_name}: {result['message']}")
        
        # Performance Optimization Suggestions
        with st.expander("💡 Performance Suggestions"):
            suggestions = self.get_performance_suggestions()
            for suggestion in suggestions:
                st.info(f"💡 {suggestion}")
    
    def run_health_checks(self):
        """Run comprehensive system health checks."""
        results = {}
        
        # Check disk space
        disk = psutil.disk_usage('/')
        disk_percent = (disk.used / disk.total) * 100
        if disk_percent > 95:
            results['Disk Space'] = {'status': 'fail', 'message': 'Disk space critically low'}
        elif disk_percent > 85:
            results['Disk Space'] = {'status': 'warning', 'message': 'Disk space getting low'}
        else:
            results['Disk Space'] = {'status': 'pass', 'message': 'Sufficient disk space available'}
        
        # Check memory usage
        memory = psutil.virtual_memory()
        if memory.percent > 90:
            results['Memory'] = {'status': 'fail', 'message': 'Memory usage critically high'}
        elif memory.percent > 80:
            results['Memory'] = {'status': 'warning', 'message': 'Memory usage is high'}
        else:
            results['Memory'] = {'status': 'pass', 'message': 'Memory usage is normal'}
        
        # Check CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        if cpu_percent > 85:
            results['CPU'] = {'status': 'fail', 'message': 'CPU usage critically high'}
        elif cpu_percent > 70:
            results['CPU'] = {'status': 'warning', 'message': 'CPU usage is high'}
        else:
            results['CPU'] = {'status': 'pass', 'message': 'CPU usage is normal'}
        
        # Check system load (Unix-like systems)
        try:
            load_avg = os.getloadavg()
            cpu_count = psutil.cpu_count()
            load_ratio = load_avg[0] / cpu_count
            if load_ratio > 2.0:
                results['System Load'] = {'status': 'fail', 'message': f'System load very high: {load_avg[0]:.2f}'}
            elif load_ratio > 1.0:
                results['System Load'] = {'status': 'warning', 'message': f'System load high: {load_avg[0]:.2f}'}
            else:
                results['System Load'] = {'status': 'pass', 'message': f'System load normal: {load_avg[0]:.2f}'}
        except:
            results['System Load'] = {'status': 'pass', 'message': 'Load average not available on this platform'}
        
        return results
    
    def get_performance_suggestions(self):
        """Get performance optimization suggestions."""
        suggestions = []
        metrics = self.get_system_metrics()
        
        if not metrics:
            return ["Unable to analyze system - metrics collection failed"]
        
        # CPU suggestions
        if metrics['cpu']['percent'] > 80:
            suggestions.append("High CPU usage detected - consider closing unnecessary applications")
            suggestions.append("Check for background processes consuming CPU resources")
        
        # Memory suggestions  
        if metrics['memory']['percent'] > 80:
            suggestions.append("High memory usage detected - consider closing unused applications")
            suggestions.append("Restart the application to free up memory leaks")
        
        # Disk suggestions
        if metrics['disk']['percent'] > 85:
            suggestions.append("Low disk space - consider cleaning temporary files")
            suggestions.append("Archive or delete old log files and data")
        
        # Process suggestions
        if metrics['processes'] > 200:
            suggestions.append(f"Many processes running ({metrics['processes']}) - consider system cleanup")
        
        # General suggestions
        suggestions.extend([
            "Regularly restart the system to apply updates and clear caches",
            "Monitor system performance during peak usage times",
            "Set up automated alerts for critical resource thresholds",
            "Keep system and applications updated for optimal performance"
        ])
        
        return suggestions
    
    def render_alert_management(self):
        """Render alert management and configuration section."""
        st.header("⚠️ Alert Management")
        
        # Alert Configuration
        with st.expander("🔔 Alert Configuration", expanded=True):
            st.subheader("Threshold Settings")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**CPU Thresholds**")
                cpu_warning = st.slider("CPU Warning (%)", 0, 100, 70)
                cpu_critical = st.slider("CPU Critical (%)", 0, 100, 85)
                
                st.write("**Memory Thresholds**") 
                memory_warning = st.slider("Memory Warning (%)", 0, 100, 80)
                memory_critical = st.slider("Memory Critical (%)", 0, 100, 90)
            
            with col2:
                st.write("**Disk Thresholds**")
                disk_warning = st.slider("Disk Warning (%)", 0, 100, 85)
                disk_critical = st.slider("Disk Critical (%)", 0, 100, 95)
                
                st.write("**Alert Settings**")
                alert_frequency = st.selectbox("Alert Frequency", ["Immediate", "Every 5 minutes", "Every 15 minutes", "Hourly"])
                email_alerts = st.checkbox("Enable Email Alerts")
        
        # Current Alerts
        with st.expander("📋 Active Alerts"):
            metrics = self.get_system_metrics()
            if metrics:
                current_alerts = []
                
                # Check for alerts based on current metrics
                if metrics['cpu']['percent'] >= cpu_critical:
                    current_alerts.append({"type": "Critical", "component": "CPU", "value": f"{metrics['cpu']['percent']:.1f}%", "threshold": f"{cpu_critical}%"})
                elif metrics['cpu']['percent'] >= cpu_warning:
                    current_alerts.append({"type": "Warning", "component": "CPU", "value": f"{metrics['cpu']['percent']:.1f}%", "threshold": f"{cpu_warning}%"})
                
                if metrics['memory']['percent'] >= memory_critical:
                    current_alerts.append({"type": "Critical", "component": "Memory", "value": f"{metrics['memory']['percent']:.1f}%", "threshold": f"{memory_critical}%"})
                elif metrics['memory']['percent'] >= memory_warning:
                    current_alerts.append({"type": "Warning", "component": "Memory", "value": f"{metrics['memory']['percent']:.1f}%", "threshold": f"{memory_warning}%"})
                
                if metrics['disk']['percent'] >= disk_critical:
                    current_alerts.append({"type": "Critical", "component": "Disk", "value": f"{metrics['disk']['percent']:.1f}%", "threshold": f"{disk_critical}%"})
                elif metrics['disk']['percent'] >= disk_warning:
                    current_alerts.append({"type": "Warning", "component": "Disk", "value": f"{metrics['disk']['percent']:.1f}%", "threshold": f"{disk_warning}%"})
                
                if current_alerts:
                    alert_df = pd.DataFrame(current_alerts)
                    st.dataframe(alert_df, use_container_width=True)
                else:
                    st.success("No active alerts - system is operating within normal parameters")
            else:
                st.error("Unable to check for alerts - metrics collection failed")
        
        # Alert History
        with st.expander("📊 Alert History"):
            if st.session_state.system_alerts:
                history_df = pd.DataFrame(st.session_state.system_alerts)
                st.dataframe(history_df, use_container_width=True)
                
                # Alert statistics
                if len(st.session_state.system_alerts) > 0:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Alerts", len(st.session_state.system_alerts))
                    with col2:
                        critical_count = len([a for a in st.session_state.system_alerts if a.get('type') == 'Critical'])
                        st.metric("Critical Alerts", critical_count)
                    with col3:
                        warning_count = len([a for a in st.session_state.system_alerts if a.get('type') == 'Warning']) 
                        st.metric("Warning Alerts", warning_count)
            else:
                st.info("No alert history available")
    
    def render_resource_monitoring(self):
        """Render resource monitoring section."""
        st.header("🔄 Resource Monitoring")
        
        # Database Connections (Mock)
        with st.expander("🗄️ Database Connections", expanded=True):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                render_metric_card("Active Connections", "12", "+2")
            with col2:
                render_metric_card("Connection Pool", "85%", None)
            with col3:
                render_metric_card("Query Response", "45ms", "-5ms")
            
            # Mock connection data
            conn_data = pd.DataFrame({
                'Database': ['PostgreSQL', 'Redis', 'BigQuery', 'MongoDB'],
                'Status': ['Connected', 'Connected', 'Connected', 'Warning'],
                'Response_Time': [23, 12, 45, 156],
                'Active_Connections': [8, 4, 2, 3]
            })
            
            st.dataframe(conn_data, use_container_width=True)
        
        # API Usage
        with st.expander("🌐 API Usage"):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                render_metric_card("Requests/min", "234", "+12%")
            with col2:
                render_metric_card("Error Rate", "0.2%", "-0.1%")
            with col3:
                render_metric_card("Avg Latency", "120ms", "+5ms")
            
            # Mock API metrics
            api_data = {
                'time': pd.date_range(start='2024-01-01', periods=24, freq='H'),
                'requests': np.random.poisson(200, 24),
                'errors': np.random.poisson(2, 24),
                'latency': np.random.normal(120, 20, 24)
            }
            
            fig_api = make_subplots(
                rows=2, cols=2,
                subplot_titles=['Requests per Hour', 'Error Count', 'API Latency', 'Success Rate'],
                specs=[[{"secondary_y": False}, {"secondary_y": False}],
                       [{"secondary_y": False}, {"secondary_y": False}]]
            )
            
            # Requests
            fig_api.add_trace(
                go.Scatter(x=api_data['time'], y=api_data['requests'], name='Requests'),
                row=1, col=1
            )
            
            # Errors
            fig_api.add_trace(
                go.Bar(x=api_data['time'], y=api_data['errors'], name='Errors'),
                row=1, col=2
            )
            
            # Latency
            fig_api.add_trace(
                go.Scatter(x=api_data['time'], y=api_data['latency'], name='Latency'),
                row=2, col=1
            )
            
            # Success rate
            success_rate = ((api_data['requests'] - api_data['errors']) / api_data['requests'] * 100)
            fig_api.add_trace(
                go.Scatter(x=api_data['time'], y=success_rate, name='Success Rate %'),
                row=2, col=2
            )
            
            fig_api.update_layout(height=500, showlegend=False)
            st.plotly_chart(fig_api, use_container_width=True)
        
        # Service Status
        with st.expander("🚀 Service Status"):
            services = [
                {"name": "Web Server", "status": "Running", "uptime": "15d 4h", "cpu": "12%", "memory": "245MB"},
                {"name": "Database", "status": "Running", "uptime": "15d 4h", "cpu": "8%", "memory": "1.2GB"},
                {"name": "Cache Server", "status": "Running", "uptime": "7d 12h", "cpu": "3%", "memory": "156MB"},
                {"name": "Background Jobs", "status": "Warning", "uptime": "2d 8h", "cpu": "25%", "memory": "512MB"},
                {"name": "File Storage", "status": "Running", "uptime": "30d 2h", "cpu": "2%", "memory": "89MB"}
            ]
            
            service_df = pd.DataFrame(services)
            
            # Color code status
            def color_status(val):
                if val == "Running":
                    return "background-color: #d4edda; color: #155724"
                elif val == "Warning":
                    return "background-color: #fff3cd; color: #856404"
                else:
                    return "background-color: #f8d7da; color: #721c24"
            
            styled_df = service_df.style.applymap(color_status, subset=['status'])
            st.dataframe(styled_df, use_container_width=True)
    
    def render_export_options(self):
        """Render data export options."""
        st.header("📤 Export Performance Data")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Export Current Metrics"):
                metrics = self.get_system_metrics()
                if metrics:
                    metrics_df = pd.DataFrame([{
                        'timestamp': metrics['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                        'cpu_percent': metrics['cpu']['percent'],
                        'cpu_cores': metrics['cpu']['count'],
                        'memory_percent': metrics['memory']['percent'],
                        'memory_total_gb': metrics['memory']['total'] / (1024**3),
                        'disk_percent': metrics['disk']['percent'],
                        'disk_total_gb': metrics['disk']['total'] / (1024**3),
                        'processes': metrics['processes']
                    }])
                    
                    st.download_button(
                        "Download Current Metrics",
                        metrics_df.to_csv(index=False),
                        "current_metrics.csv",
                        "text/csv"
                    )
        
        with col2:
            if st.button("Export Historical Data"):
                if st.session_state.performance_data:
                    historical_df = pd.DataFrame([
                        {
                            'timestamp': metric['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                            'cpu_percent': metric['cpu']['percent'],
                            'memory_percent': metric['memory']['percent'],
                            'disk_percent': metric['disk']['percent']
                        }
                        for metric in st.session_state.performance_data
                    ])
                    
                    st.download_button(
                        "Download Historical Data",
                        historical_df.to_csv(index=False),
                        "performance_history.csv",
                        "text/csv"
                    )
                else:
                    st.info("No historical data available for export")
    
    def render(self):
        """Main render method for the System Performance page."""
        # Page header
        render_page_header(
            "System Performance Dashboard",
            "Monitor system health, performance metrics, and resource utilization"
        )
        
        # Sidebar controls
        with st.sidebar:
            st.header("⚙️ Controls")
            
            # Monitoring toggle
            st.session_state.monitoring_enabled = st.toggle(
                "Enable Monitoring", 
                value=st.session_state.monitoring_enabled
            )
            
            # Refresh controls
            if st.button("🔄 Refresh Metrics"):
                st.rerun()
            
            # Auto-refresh
            auto_refresh = st.selectbox(
                "Auto Refresh",
                ["Disabled", "Every 5 seconds", "Every 30 seconds", "Every minute"]
            )
            
            if auto_refresh != "Disabled":
                refresh_time = {
                    "Every 5 seconds": 5,
                    "Every 30 seconds": 30, 
                    "Every minute": 60
                }[auto_refresh]
                
                if st.session_state.monitoring_enabled:
                    time.sleep(refresh_time)
                    st.rerun()
            
            # Clear data
            if st.button("🗑️ Clear History"):
                st.session_state.performance_data = []
                st.session_state.system_alerts = []
                st.success("History cleared!")
        
        # Main content tabs
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "📊 Overview", 
            "📈 Metrics", 
            "📉 Trends",
            "🔧 Diagnostics", 
            "⚠️ Alerts",
            "🔄 Resources"
        ])
        
        with tab1:
            self.render_system_overview()
        
        with tab2:
            self.render_performance_metrics()
        
        with tab3:
            self.render_historical_trends()
        
        with tab4:
            self.render_system_diagnostics()
        
        with tab5:
            self.render_alert_management()
        
        with tab6:
            self.render_resource_monitoring()
        
        # Export section at bottom
        st.divider()
        self.render_export_options()


def render_system_performance_page():
    """Entry point for the System Performance page."""
    page = SystemPerformancePage()
    page.render()


if __name__ == "__main__":
    render_system_performance_page()