"""Common components used across multiple pages."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

from utils.ui_helpers import (
    create_metric_card, create_status_indicator, format_large_number, 
    format_percentage, format_currency, COLOR_SCHEMES
)

def render_header_section(title: str, description: str, icon: str = ""):
    """Render page header with title, description and optional icon."""
    if icon:
        st.title(f"{icon} {title}")
    else:
        st.title(title)
    
    if description:
        st.markdown(f"*{description}*")
    
    st.markdown("---")

def render_kpi_metrics(metrics: Dict[str, Dict[str, Any]], columns: int = 4):
    """Render KPI metrics in a grid layout."""
    cols = st.columns(columns)
    
    for i, (key, metric_data) in enumerate(metrics.items()):
        col_idx = i % columns
        with cols[col_idx]:
            create_metric_card(
                title=metric_data.get("title", key),
                value=metric_data.get("value", 0),
                delta=metric_data.get("delta"),
                delta_color=metric_data.get("delta_color", "normal"),
                help_text=metric_data.get("help")
            )

def render_status_overview(status_data: Dict[str, str], title: str = "System Status"):
    """Render system status overview."""
    st.subheader(title)
    
    cols = st.columns(len(status_data))
    for i, (component, status) in enumerate(status_data.items()):
        with cols[i]:
            status_indicator = create_status_indicator(status, component)
            st.markdown(f"**{status_indicator}**")

def render_alerts_panel(alerts: List[Dict[str, Any]], max_alerts: int = 5):
    """Render alerts panel with recent alerts."""
    st.subheader("🚨 Recent Alerts")
    
    if not alerts:
        st.info("No recent alerts")
        return
    
    # Sort alerts by timestamp (newest first)
    sorted_alerts = sorted(alerts, key=lambda x: x.get('timestamp', ''), reverse=True)
    
    for alert in sorted_alerts[:max_alerts]:
        severity = alert.get('severity', 'info').lower()
        
        if severity == 'critical':
            alert_color = "🔴"
            container = st.error
        elif severity == 'warning':
            alert_color = "🟡"
            container = st.warning
        elif severity == 'info':
            alert_color = "🔵"
            container = st.info
        else:
            alert_color = "⚫"
            container = st.info
        
        with container:
            st.markdown(f"{alert_color} **{alert.get('title', 'Alert')}**")
            if alert.get('message'):
                st.markdown(alert['message'])
            if alert.get('timestamp'):
                st.caption(f"Time: {alert['timestamp']}")

def render_data_quality_summary(quality_metrics: Dict[str, float]):
    """Render data quality summary with gauges."""
    st.subheader("📊 Data Quality Overview")
    
    cols = st.columns(len(quality_metrics))
    
    for i, (metric, value) in enumerate(quality_metrics.items()):
        with cols[i]:
            # Create mini gauge chart
            fig = go.Figure(go.Indicator(
                mode = "gauge+number",
                value = value,
                domain = {'x': [0, 1], 'y': [0, 1]},
                title = {'text': metric.replace('_', ' ').title()},
                gauge = {
                    'axis': {'range': [None, 100]},
                    'bar': {'color': COLOR_SCHEMES["primary"]},
                    'steps': [
                        {'range': [0, 60], 'color': "lightgray"},
                        {'range': [60, 80], 'color': "gray"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 90
                    }
                }
            ))
            fig.update_layout(height=200, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig, use_container_width=True)

def render_trend_indicators(trend_data: Dict[str, Dict[str, Any]]):
    """Render trend indicators with sparklines."""
    st.subheader("📈 Trend Indicators")
    
    cols = st.columns(len(trend_data))
    
    for i, (metric, data) in enumerate(trend_data.items()):
        with cols[i]:
            current_value = data.get('current', 0)
            trend_values = data.get('trend', [])
            change = data.get('change', 0)
            
            # Create sparkline
            if trend_values:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    y=trend_values,
                    mode='lines',
                    line=dict(color=COLOR_SCHEMES["primary"], width=2),
                    showlegend=False
                ))
                fig.update_layout(
                    height=100,
                    margin=dict(l=0, r=0, t=0, b=0),
                    xaxis=dict(showgrid=False, showticklabels=False),
                    yaxis=dict(showgrid=False, showticklabels=False),
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Display metric
            create_metric_card(
                title=metric.replace('_', ' ').title(),
                value=format_large_number(current_value),
                delta=f"{change:+.1f}%" if change != 0 else None,
                delta_color="inverse" if change < 0 else "normal"
            )

def render_performance_summary(performance_data: Dict[str, Any]):
    """Render system performance summary."""
    st.subheader("⚡ Performance Summary")
    
    # Create performance metrics grid
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        cpu_usage = performance_data.get('cpu_usage', 0)
        create_metric_card(
            title="CPU Usage",
            value=f"{cpu_usage:.1f}%",
            delta_color="inverse" if cpu_usage > 80 else "normal"
        )
    
    with col2:
        memory_usage = performance_data.get('memory_usage', 0)
        create_metric_card(
            title="Memory Usage",
            value=f"{memory_usage:.1f}%",
            delta_color="inverse" if memory_usage > 85 else "normal"
        )
    
    with col3:
        response_time = performance_data.get('avg_response_time', 0)
        create_metric_card(
            title="Avg Response Time",
            value=f"{response_time:.0f}ms",
            delta_color="inverse" if response_time > 1000 else "normal"
        )
    
    with col4:
        throughput = performance_data.get('throughput', 0)
        create_metric_card(
            title="Throughput",
            value=f"{format_large_number(throughput)}/s",
            delta_color="normal"
        )

def render_quick_actions_panel():
    """Render quick actions panel."""
    st.subheader("⚡ Quick Actions")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("🔄 Refresh Data", key="refresh_data"):
            st.experimental_rerun()
    
    with col2:
        if st.button("📊 Generate Report", key="generate_report"):
            st.info("Report generation initiated...")
    
    with col3:
        if st.button("⚠️ View All Alerts", key="view_alerts"):
            st.info("Navigating to alerts dashboard...")
    
    with col4:
        if st.button("⚙️ System Settings", key="system_settings"):
            st.info("Opening system settings...")

def render_time_range_selector(key_suffix: str = "") -> tuple:
    """Render time range selector and return selected range."""
    st.subheader("📅 Time Range")
    
    col1, col2 = st.columns(2)
    
    with col1:
        time_range = st.selectbox(
            "Select Time Range",
            options=["Last Hour", "Last 24 Hours", "Last 7 Days", "Last 30 Days", "Custom"],
            key=f"time_range_{key_suffix}"
        )
    
    if time_range == "Custom":
        with col2:
            start_date = st.date_input("Start Date", key=f"start_date_{key_suffix}")
            end_date = st.date_input("End Date", key=f"end_date_{key_suffix}")
        return start_date, end_date
    else:
        # Calculate date range based on selection
        end_date = datetime.now()
        if time_range == "Last Hour":
            start_date = end_date - timedelta(hours=1)
        elif time_range == "Last 24 Hours":
            start_date = end_date - timedelta(days=1)
        elif time_range == "Last 7 Days":
            start_date = end_date - timedelta(days=7)
        elif time_range == "Last 30 Days":
            start_date = end_date - timedelta(days=30)
        else:
            start_date = end_date - timedelta(days=1)
        
        return start_date, end_date

def render_export_options(data: Any, filename_prefix: str = "export"):
    """Render export options for data."""
    st.subheader("📤 Export Options")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if isinstance(data, pd.DataFrame):
            csv = data.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"{filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    with col2:
        if isinstance(data, pd.DataFrame):
            excel_buffer = io.BytesIO()
            data.to_excel(excel_buffer, index=False, engine='openpyxl')
            excel_buffer.seek(0)
            
            st.download_button(
                label="Download Excel",
                data=excel_buffer,
                file_name=f"{filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    
    with col3:
        if hasattr(data, 'to_json'):
            json_str = data.to_json(orient='records', indent=2)
        else:
            import json
            json_str = json.dumps(data, indent=2, default=str)
        
        st.download_button(
            label="Download JSON",
            data=json_str,
            file_name=f"{filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json"
        )

def render_loading_state(message: str = "Loading data..."):
    """Render loading state with spinner."""
    with st.spinner(message):
        # Create placeholder for loading animation
        placeholder = st.empty()
        
        # Simple loading animation
        import time
        for i in range(3):
            placeholder.text(f"{message}{'.' * (i + 1)}")
            time.sleep(0.5)
        
        placeholder.empty()

def render_error_state(error_message: str, show_details: bool = False, details: str = ""):
    """Render error state with message and optional details."""
    st.error(f"❌ {error_message}")
    
    if show_details and details:
        with st.expander("Error Details"):
            st.code(details)
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Retry", key="retry_error"):
            st.experimental_rerun()
    
    with col2:
        if st.button("📞 Contact Support", key="contact_support"):
            st.info("Please contact support with the error details above.")

def render_empty_state(message: str = "No data available", 
                      suggestion: str = "Try adjusting your filters or time range."):
    """Render empty state when no data is available."""
    st.info(f"📭 {message}")
    if suggestion:
        st.markdown(f"💡 *{suggestion}*")

# Import required modules for export functionality
try:
    import io
    import openpyxl
except ImportError:
    pass