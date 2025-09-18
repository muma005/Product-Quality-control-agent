"""
Product Quality Control AI System - Streamlit Application (Modular Version)

This is the main entry point for the modular Streamlit application.
It provides a clean, organized dashboard with progressive disclosure and
improved user experience through modular page components.
"""

import streamlit as st
import sys
import os
from typing import Dict, Any

# Add the project root to the path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(project_root, '..'))

# Import configuration and utilities
from config.app_config import (
    DASHBOARD_CONFIG, TAB_CONFIG, DISPLAY_MODES, 
    initialize_session_state, get_enabled_tabs
)
from utils.ui_helpers import create_status_indicator, display_alert
from components.common_components import render_quick_actions_panel

# Import page modules
from pages.live_monitoring import render_live_monitoring_page
from pages.executive_dashboard import render_executive_dashboard_page

class QualityControlDashboard:
    """Main dashboard application class."""
    
    def __init__(self):
        """Initialize the dashboard."""
        self._configure_page()
        initialize_session_state()
        self._load_page_modules()
    
    def _configure_page(self):
        """Configure Streamlit page settings."""
        st.set_page_config(
            page_title=DASHBOARD_CONFIG["title"],
            page_icon=DASHBOARD_CONFIG["page_icon"],
            layout=DASHBOARD_CONFIG["layout"],
            initial_sidebar_state=DASHBOARD_CONFIG["initial_sidebar_state"]
        )
    
    def _load_page_modules(self):
        """Load all page modules."""
        self.page_modules = {
            "live_monitoring": render_live_monitoring_page,
            "executive_dashboard": render_executive_dashboard_page,
            # Placeholder functions for other pages (to be implemented)
            "advanced_analytics": self._render_placeholder_page,
            "roi_analysis": self._render_placeholder_page,
            "predictive_insights": self._render_placeholder_page,
            "automated_reports": self._render_placeholder_page,
            "system_performance": self._render_placeholder_page
        }
    
    def run(self):
        """Run the main dashboard application."""
        self._render_header()
        self._render_sidebar()
        self._render_main_content()
        self._render_footer()
    
    def _render_header(self):
        """Render the main header."""
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            st.title(f"{DASHBOARD_CONFIG['page_icon']} {DASHBOARD_CONFIG['title']}")
            st.markdown("*AI-Powered Quality Control and Monitoring System*")
        
        with col2:
            # System status indicator
            system_status = self._get_system_status()
            status_display = create_status_indicator(system_status, "System")
            st.markdown(f"**Status:** {status_display}")
        
        with col3:
            # Display mode selector
            display_mode = st.selectbox(
                "View Mode",
                options=list(DISPLAY_MODES.keys()),
                format_func=lambda x: DISPLAY_MODES[x]["name"],
                index=list(DISPLAY_MODES.keys()).index(st.session_state.display_mode),
                key="display_mode_selector"
            )
            
            if display_mode != st.session_state.display_mode:
                st.session_state.display_mode = display_mode
                st.experimental_rerun()
        
        st.markdown("---")
    
    def _render_sidebar(self):
        """Render the sidebar with navigation and controls."""
        with st.sidebar:
            st.header("🎛️ Dashboard Controls")
            
            # Display mode information
            current_mode = DISPLAY_MODES[st.session_state.display_mode]
            st.info(f"**{current_mode['name']}**\n\n{current_mode['description']}")
            
            st.markdown("---")
            
            # Auto-refresh controls
            st.subheader("🔄 Auto-Refresh")
            auto_refresh = st.checkbox(
                "Enable Auto-Refresh",
                value=st.session_state.auto_refresh_enabled,
                help="Automatically refresh data at specified intervals"
            )
            st.session_state.auto_refresh_enabled = auto_refresh
            
            if auto_refresh:
                refresh_rate = st.slider(
                    "Refresh Rate (seconds)",
                    min_value=5,
                    max_value=300,
                    value=30,
                    step=5
                )
                st.caption(f"Next refresh in ~{refresh_rate}s")
            
            st.markdown("---")
            
            # Navigation
            st.subheader("📋 Navigation")
            enabled_tabs = get_enabled_tabs(st.session_state.display_mode)
            
            for tab_name in enabled_tabs:
                tab_config = TAB_CONFIG.get(tab_name, {})
                if tab_config.get("enabled", True):
                    if st.button(
                        f"{tab_config.get('icon', '📄')} {tab_config.get('title', tab_name)}",
                        key=f"nav_{tab_name}",
                        help=tab_config.get('description', ''),
                        use_container_width=True
                    ):
                        st.session_state.current_tab = tab_name
                        st.experimental_rerun()
            
            st.markdown("---")
            
            # System information
            st.subheader("ℹ️ System Info")
            system_info = self._get_system_info()
            
            st.metric("Active Sessions", system_info.get("active_sessions", "N/A"))
            st.metric("Data Points", system_info.get("data_points", "N/A"))
            st.metric("Last Update", system_info.get("last_update", "N/A"))
            
            # Quick actions
            st.markdown("---")
            st.subheader("⚡ Quick Actions")
            
            if st.button("🔄 Refresh All Data", use_container_width=True):
                st.session_state.last_refresh = {}
                st.experimental_rerun()
            
            if st.button("📊 Export Dashboard", use_container_width=True):
                st.info("Export functionality coming soon...")
            
            if st.button("⚙️ Settings", use_container_width=True):
                st.info("Settings panel coming soon...")
    
    def _render_main_content(self):
        """Render the main content area."""
        current_tab = st.session_state.get("current_tab", "live_monitoring")
        enabled_tabs = get_enabled_tabs(st.session_state.display_mode)
        
        # Ensure current tab is enabled
        if current_tab not in enabled_tabs:
            current_tab = enabled_tabs[0] if enabled_tabs else "live_monitoring"
            st.session_state.current_tab = current_tab
        
        # Create tabs for enabled pages
        if len(enabled_tabs) > 1:
            tab_objects = st.tabs([
                f"{TAB_CONFIG.get(tab, {}).get('icon', '📄')} {TAB_CONFIG.get(tab, {}).get('title', tab)}"
                for tab in enabled_tabs
            ])
            
            for i, tab_name in enumerate(enabled_tabs):
                with tab_objects[i]:
                    self._render_tab_content(tab_name)
        else:
            # Single tab mode
            self._render_tab_content(current_tab)
    
    def _render_tab_content(self, tab_name: str):
        """Render content for a specific tab."""
        try:
            # Get the render function for this tab
            render_function = self.page_modules.get(tab_name)
            
            if render_function:
                render_function()
            else:
                self._render_placeholder_page(tab_name)
        
        except Exception as e:
            st.error(f"❌ Error loading {tab_name}: {str(e)}")
            st.exception(e)
    
    def _render_placeholder_page(self, tab_name: str = ""):
        """Render placeholder page for tabs not yet implemented."""
        tab_config = TAB_CONFIG.get(tab_name, {})
        
        st.header(f"{tab_config.get('icon', '📄')} {tab_config.get('title', 'Coming Soon')}")
        
        if tab_config.get('description'):
            st.markdown(f"*{tab_config['description']}*")
        
        st.info(f"""
        🚧 **This section is under development**
        
        The {tab_config.get('title', tab_name)} page is being developed as part of the 
        modular dashboard reorganization. It will include:
        
        - Interactive data visualizations
        - Real-time updates and monitoring
        - Export and filtering capabilities
        - Integration with the main quality control pipeline
        
        Check back soon for updates!
        """)
        
        # Show some sample metrics as placeholder
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Sample Metric 1", "1,234", "5.2%")
        
        with col2:
            st.metric("Sample Metric 2", "98.5%", "0.3%")
        
        with col3:
            st.metric("Sample Metric 3", "$45.6K", "-2.1%")
        
        with col4:
            st.metric("Sample Metric 4", "87", "+12")
        
        # Sample chart
        import pandas as pd
        import numpy as np
        
        sample_data = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=30, freq='D'),
            'Values': np.random.normal(100, 15, 30).cumsum()
        })
        
        st.line_chart(sample_data.set_index('Date'))
    
    def _render_footer(self):
        """Render the footer."""
        st.markdown("---")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Product Quality Control AI System**")
            st.caption("Powered by Advanced Analytics & Machine Learning")
        
        with col2:
            st.markdown("**System Status**")
            uptime = self._get_system_uptime()
            st.caption(f"Uptime: {uptime}")
        
        with col3:
            st.markdown("**Support**")
            st.caption("Contact: support@qualitycontrol.ai")
    
    def _get_system_status(self) -> str:
        """Get current system status."""
        # This would normally check actual system health
        # For now, return a sample status
        import random
        statuses = ["healthy", "healthy", "healthy", "warning", "healthy"]  # Weighted towards healthy
        return random.choice(statuses)
    
    def _get_system_info(self) -> Dict[str, Any]:
        """Get system information for sidebar."""
        from datetime import datetime
        
        return {
            "active_sessions": "1",
            "data_points": "2.3M",
            "last_update": datetime.now().strftime("%H:%M:%S")
        }
    
    def _get_system_uptime(self) -> str:
        """Get system uptime."""
        # This would normally calculate actual uptime
        return "99.7% (30 days)"

def main():
    """Main application entry point."""
    try:
        # Initialize and run the dashboard
        dashboard = QualityControlDashboard()
        dashboard.run()
        
    except Exception as e:
        st.error(f"❌ Application Error: {str(e)}")
        st.exception(e)
        
        # Show recovery options
        st.markdown("---")
        st.subheader("🔧 Recovery Options")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔄 Restart Application"):
                st.experimental_rerun()
        
        with col2:
            if st.button("🏠 Reset to Default"):
                # Clear session state
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.experimental_rerun()

if __name__ == "__main__":
    main()