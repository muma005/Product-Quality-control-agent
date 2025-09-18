"""Application configuration settings."""

import streamlit as st
from typing import Dict, Any, List

# Dashboard Configuration
DASHBOARD_CONFIG = {
    "title": "Product Quality Control AI System",
    "page_icon": "🏭",
    "layout": "wide",
    "initial_sidebar_state": "expanded"
}

# Tab Configuration
TAB_CONFIG = {
    "live_monitoring": {
        "title": "🔴 Live Monitoring",
        "description": "Real-time quality monitoring and alerts",
        "icon": "🔴",
        "enabled": True
    },
    "executive_dashboard": {
        "title": "📊 Executive Dashboard",
        "description": "High-level KPIs and executive summary",
        "icon": "📊",
        "enabled": True
    },
    "advanced_analytics": {
        "title": "🔬 Advanced Analytics",
        "description": "Deep dive analytics and insights",
        "icon": "🔬",
        "enabled": True
    },
    "roi_analysis": {
        "title": "💰 ROI Analysis",
        "description": "Return on investment tracking",
        "icon": "💰",
        "enabled": True
    },
    "predictive_insights": {
        "title": "🔮 Predictive Insights",
        "description": "AI-powered predictions and forecasting",
        "icon": "🔮",
        "enabled": True
    },
    "automated_reports": {
        "title": "📋 Automated Reports",
        "description": "Scheduled reports and exports",
        "icon": "📋",
        "enabled": True
    },
    "system_performance": {
        "title": "⚙️ System Performance",
        "description": "System health and performance metrics",
        "icon": "⚙️",
        "enabled": True
    }
}

# Display Modes
DISPLAY_MODES = {
    "overview": {
        "name": "Overview Mode",
        "description": "High-level summary across all areas",
        "tabs": ["live_monitoring", "executive_dashboard", "system_performance"]
    },
    "detailed": {
        "name": "Detailed Mode",
        "description": "Full access to all features and analytics",
        "tabs": list(TAB_CONFIG.keys())
    },
    "analytics": {
        "name": "Analytics Mode",
        "description": "Focus on data analysis and insights",
        "tabs": ["advanced_analytics", "predictive_insights", "roi_analysis"]
    },
    "operations": {
        "name": "Operations Mode",
        "description": "Real-time monitoring and system performance",
        "tabs": ["live_monitoring", "system_performance", "automated_reports"]
    }
}

# Auto-refresh Configuration
AUTO_REFRESH_CONFIG = {
    "intervals": {
        "live_monitoring": 5,  # seconds
        "executive_dashboard": 30,
        "advanced_analytics": 60,
        "roi_analysis": 300,
        "predictive_insights": 300,
        "automated_reports": 600,
        "system_performance": 10
    },
    "default_enabled": True
}

# Color Schemes
COLOR_SCHEMES = {
    "primary": "#1f77b4",
    "success": "#2ca02c",
    "warning": "#ff7f0e",
    "danger": "#d62728",
    "info": "#17a2b8",
    "secondary": "#6c757d",
    "light": "#f8f9fa",
    "dark": "#343a40"
}

# Chart Configuration
CHART_CONFIG = {
    "default_height": 400,
    "small_height": 200,
    "large_height": 600,
    "color_palette": [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
        "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
        "#bcbd22", "#17becf"
    ]
}

def get_tab_config(tab_name: str) -> Dict[str, Any]:
    """Get configuration for a specific tab."""
    return TAB_CONFIG.get(tab_name, {})

def get_enabled_tabs(mode: str = "detailed") -> List[str]:
    """Get list of enabled tabs for the specified display mode."""
    if mode in DISPLAY_MODES:
        return DISPLAY_MODES[mode]["tabs"]
    return list(TAB_CONFIG.keys())

def initialize_session_state():
    """Initialize Streamlit session state variables."""
    if "display_mode" not in st.session_state:
        st.session_state.display_mode = "detailed"
    
    if "auto_refresh_enabled" not in st.session_state:
        st.session_state.auto_refresh_enabled = AUTO_REFRESH_CONFIG["default_enabled"]
    
    if "current_tab" not in st.session_state:
        st.session_state.current_tab = "live_monitoring"
    
    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = {}
    
    for tab_name in TAB_CONFIG.keys():
        if f"{tab_name}_data" not in st.session_state:
            st.session_state[f"{tab_name}_data"] = None