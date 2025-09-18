"""Common utility functions for the Streamlit application."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta
import time
import json

from config.app_config import COLOR_SCHEMES, CHART_CONFIG, AUTO_REFRESH_CONFIG

def create_metric_card(title: str, value: Union[str, int, float], 
                      delta: Optional[Union[str, int, float]] = None,
                      delta_color: str = "normal",
                      help_text: Optional[str] = None) -> None:
    """Create a styled metric card."""
    st.metric(
        label=title,
        value=value,
        delta=delta,
        delta_color=delta_color,
        help=help_text
    )

def create_status_indicator(status: str, label: str = "") -> str:
    """Create a colored status indicator."""
    status_colors = {
        "healthy": "🟢",
        "warning": "🟡", 
        "critical": "🔴",
        "unknown": "⚫",
        "online": "🟢",
        "offline": "🔴",
        "active": "🟢",
        "inactive": "⚫"
    }
    
    indicator = status_colors.get(status.lower(), "⚫")
    return f"{indicator} {label}" if label else indicator

def format_large_number(num: Union[int, float], precision: int = 1) -> str:
    """Format large numbers with appropriate suffixes."""
    if num >= 1_000_000_000:
        return f"{num/1_000_000_000:.{precision}f}B"
    elif num >= 1_000_000:
        return f"{num/1_000_000:.{precision}f}M"
    elif num >= 1_000:
        return f"{num/1_000:.{precision}f}K"
    else:
        return f"{num:.{precision}f}" if isinstance(num, float) else str(num)

def format_percentage(value: float, precision: int = 1) -> str:
    """Format percentage values."""
    return f"{value:.{precision}f}%"

def format_currency(amount: float, currency: str = "USD", precision: int = 0) -> str:
    """Format currency values."""
    if currency == "USD":
        return f"${format_large_number(amount, precision)}"
    else:
        return f"{amount:,.{precision}f} {currency}"

def create_gauge_chart(value: float, title: str, min_val: float = 0, 
                      max_val: float = 100, threshold_good: float = 80,
                      threshold_warning: float = 60) -> go.Figure:
    """Create a gauge chart for KPI visualization."""
    
    # Determine color based on thresholds
    if value >= threshold_good:
        color = COLOR_SCHEMES["success"]
    elif value >= threshold_warning:
        color = COLOR_SCHEMES["warning"]
    else:
        color = COLOR_SCHEMES["danger"]
    
    fig = go.Figure(go.Indicator(
        mode = "gauge+number+delta",
        value = value,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': title},
        gauge = {
            'axis': {'range': [None, max_val]},
            'bar': {'color': color},
            'steps': [
                {'range': [min_val, threshold_warning], 'color': "lightgray"},
                {'range': [threshold_warning, threshold_good], 'color': "gray"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': max_val * 0.9
            }
        }
    ))
    
    fig.update_layout(height=300)
    return fig

def create_time_series_chart(df: pd.DataFrame, x_col: str, y_col: str, 
                           title: str, color: Optional[str] = None) -> go.Figure:
    """Create a time series chart."""
    fig = px.line(
        df, 
        x=x_col, 
        y=y_col, 
        title=title,
        color_discrete_sequence=[color or COLOR_SCHEMES["primary"]]
    )
    
    fig.update_layout(
        height=CHART_CONFIG["default_height"],
        showlegend=False,
        xaxis_title="Time",
        yaxis_title=y_col.replace('_', ' ').title()
    )
    
    return fig

def create_bar_chart(df: pd.DataFrame, x_col: str, y_col: str, 
                    title: str, orientation: str = "v") -> go.Figure:
    """Create a bar chart."""
    if orientation == "h":
        fig = px.bar(df, x=y_col, y=x_col, title=title, orientation="h")
    else:
        fig = px.bar(df, x=x_col, y=y_col, title=title)
    
    fig.update_layout(height=CHART_CONFIG["default_height"])
    return fig

def create_pie_chart(df: pd.DataFrame, values_col: str, names_col: str, 
                    title: str) -> go.Figure:
    """Create a pie chart."""
    fig = px.pie(
        df, 
        values=values_col, 
        names=names_col, 
        title=title,
        color_discrete_sequence=CHART_CONFIG["color_palette"]
    )
    
    fig.update_layout(height=CHART_CONFIG["default_height"])
    return fig

def create_scatter_plot(df: pd.DataFrame, x_col: str, y_col: str, 
                       title: str, size_col: Optional[str] = None,
                       color_col: Optional[str] = None) -> go.Figure:
    """Create a scatter plot."""
    fig = px.scatter(
        df, 
        x=x_col, 
        y=y_col, 
        title=title,
        size=size_col,
        color=color_col,
        color_discrete_sequence=CHART_CONFIG["color_palette"]
    )
    
    fig.update_layout(height=CHART_CONFIG["default_height"])
    return fig

def display_dataframe_with_styling(df: pd.DataFrame, title: str = "",
                                  height: int = 400) -> None:
    """Display a styled DataFrame."""
    if title:
        st.subheader(title)
    
    # Apply basic styling
    styled_df = df.style.format({
        col: '{:.2f}' for col in df.select_dtypes(include=['float64']).columns
    })
    
    st.dataframe(styled_df, height=height, use_container_width=True)

def show_loading_spinner(text: str = "Loading..."):
    """Show a loading spinner with custom text."""
    return st.spinner(text)

def display_alert(message: str, alert_type: str = "info"):
    """Display an alert message."""
    if alert_type == "success":
        st.success(message)
    elif alert_type == "warning":
        st.warning(message)
    elif alert_type == "error":
        st.error(message)
    else:
        st.info(message)

def check_auto_refresh(tab_name: str) -> bool:
    """Check if auto-refresh should occur for a tab."""
    if not st.session_state.get("auto_refresh_enabled", False):
        return False
    
    interval = AUTO_REFRESH_CONFIG["intervals"].get(tab_name, 60)
    last_refresh = st.session_state.get("last_refresh", {}).get(tab_name, 0)
    current_time = time.time()
    
    if current_time - last_refresh > interval:
        if "last_refresh" not in st.session_state:
            st.session_state.last_refresh = {}
        st.session_state.last_refresh[tab_name] = current_time
        return True
    
    return False

def setup_auto_refresh(tab_name: str):
    """Setup auto-refresh for a specific tab."""
    if st.session_state.get("auto_refresh_enabled", False):
        interval = AUTO_REFRESH_CONFIG["intervals"].get(tab_name, 60)
        st_autorefresh(interval=interval * 1000, key=f"refresh_{tab_name}")

def export_data_to_csv(df: pd.DataFrame, filename: str):
    """Export DataFrame to CSV for download."""
    csv = df.to_csv(index=False)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name=filename,
        mime="text/csv"
    )

def export_data_to_json(data: Dict[str, Any], filename: str):
    """Export data to JSON for download."""
    json_str = json.dumps(data, indent=2, default=str)
    st.download_button(
        label="Download JSON",
        data=json_str,
        file_name=filename,
        mime="application/json"
    )

def create_filter_sidebar(df: pd.DataFrame, columns: List[str]) -> Dict[str, Any]:
    """Create filter controls in sidebar."""
    filters = {}
    
    st.sidebar.subheader("Filters")
    
    for col in columns:
        if df[col].dtype in ['object', 'category']:
            unique_values = df[col].unique().tolist()
            selected_values = st.sidebar.multiselect(
                f"Filter by {col.replace('_', ' ').title()}",
                options=unique_values,
                default=unique_values
            )
            filters[col] = selected_values
        elif df[col].dtype in ['int64', 'float64']:
            min_val, max_val = float(df[col].min()), float(df[col].max())
            selected_range = st.sidebar.slider(
                f"Filter by {col.replace('_', ' ').title()}",
                min_value=min_val,
                max_value=max_val,
                value=(min_val, max_val)
            )
            filters[col] = selected_range
    
    return filters

def apply_filters(df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
    """Apply filters to DataFrame."""
    filtered_df = df.copy()
    
    for col, filter_value in filters.items():
        if isinstance(filter_value, list):
            filtered_df = filtered_df[filtered_df[col].isin(filter_value)]
        elif isinstance(filter_value, tuple):
            filtered_df = filtered_df[
                (filtered_df[col] >= filter_value[0]) & 
                (filtered_df[col] <= filter_value[1])
            ]
    
    return filtered_df

def format_timestamp(timestamp: Union[str, datetime], format_str: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Format timestamp for display."""
    if isinstance(timestamp, str):
        timestamp = pd.to_datetime(timestamp)
    return timestamp.strftime(format_str)

def calculate_percentage_change(current: float, previous: float) -> float:
    """Calculate percentage change between two values."""
    if previous == 0:
        return 0
    return ((current - previous) / previous) * 100

def create_comparison_metrics(current_data: Dict[str, float], 
                            previous_data: Dict[str, float]) -> Dict[str, Dict[str, Any]]:
    """Create comparison metrics with percentage changes."""
    comparison = {}
    
    for key in current_data.keys():
        if key in previous_data:
            current_val = current_data[key]
            previous_val = previous_data[key]
            pct_change = calculate_percentage_change(current_val, previous_val)
            
            comparison[key] = {
                "current": current_val,
                "previous": previous_val,
                "change": pct_change,
                "delta_color": "normal" if abs(pct_change) < 5 else ("inverse" if pct_change < 0 else "normal")
            }
    
    return comparison

# Try to import streamlit-autorefresh for auto-refresh functionality
try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    # Fallback function if streamlit-autorefresh is not available
    def st_autorefresh(interval=1000, key=None):
        """Fallback for auto-refresh functionality."""
        pass