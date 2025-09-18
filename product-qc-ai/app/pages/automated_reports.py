"""Automated Reports page for scheduled reporting and report management."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional, Tuple
import io
import base64
from pathlib import Path

# Import common components
from utils.ui_helpers import (
    create_metric_card, format_large_number, format_percentage,
    create_gauge_chart, create_time_series_chart, create_scatter_plot
)
from components.common_components import (
    render_header_section, render_time_range_selector, render_export_options,
    render_loading_state, render_empty_state
)
from config.app_config import COLOR_SCHEMES, CHART_CONFIG

class AutomatedReportsPage:
    """Automated Reports page class."""
    
    def __init__(self):
        """Initialize Automated Reports page."""
        self.report_templates = {}
        self.scheduled_reports = []
        self.report_history = []
    
    def render(self):
        """Render the Automated Reports page."""
        render_header_section(
            title="Automated Reports",
            description="Scheduled reporting, automated insights, and comprehensive report management",
            icon="📊"
        )
        
        # Main reports sections
        self._render_reports_overview()
        self._render_report_scheduler()
        self._render_report_templates()
        self._render_report_generation()
        self._render_report_history()
        self._render_distribution_management()
        self._render_report_analytics()
        self._render_custom_reports()
    
    def _render_reports_overview(self):
        """Render reports overview section."""
        st.subheader("📋 Reports Overview")
        
        # Key reporting metrics
        reports_data = self._get_reports_overview_data()
        
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            total_reports = reports_data.get("total_reports_generated", 0)
            create_metric_card(
                title="Reports Generated",
                value=format_large_number(total_reports),
                delta=f"+{reports_data.get('reports_increase', 0)}",
                delta_color="normal",
                help_text="Total reports generated this month"
            )
        
        with col2:
            active_schedules = reports_data.get("active_schedules", 0)
            create_metric_card(
                title="Active Schedules",
                value=str(active_schedules),
                help_text="Number of active automated report schedules"
            )
        
        with col3:
            success_rate = reports_data.get("success_rate", 0)
            create_metric_card(
                title="Success Rate",
                value=f"{success_rate:.1f}%",
                delta=f"+{reports_data.get('success_improvement', 0):.1f}%",
                delta_color="normal",
                help_text="Percentage of successful report generations"
            )
        
        with col4:
            avg_generation_time = reports_data.get("avg_generation_time", 0)
            create_metric_card(
                title="Avg Generation Time",
                value=f"{avg_generation_time:.1f}s",
                delta=f"-{reports_data.get('time_reduction', 0):.1f}s",
                delta_color="normal",
                help_text="Average time to generate reports"
            )
        
        with col5:
            total_recipients = reports_data.get("total_recipients", 0)
            create_metric_card(
                title="Recipients",
                value=str(total_recipients),
                help_text="Total number of report recipients"
            )
        
        with col6:
            storage_used = reports_data.get("storage_used_gb", 0)
            create_metric_card(
                title="Storage Used",
                value=f"{storage_used:.1f} GB",
                help_text="Total storage used for reports"
            )
        
        # Report generation trend
        trend_data = self._get_report_trend_data()
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=trend_data['date'],
            y=trend_data['reports_generated'],
            mode='lines+markers',
            name='Reports Generated',
            line=dict(color=COLOR_SCHEMES["primary"], width=3),
            marker=dict(size=6)
        ))
        
        fig.add_trace(go.Scatter(
            x=trend_data['date'],
            y=trend_data['successful_deliveries'],
            mode='lines+markers',
            name='Successful Deliveries',
            line=dict(color=COLOR_SCHEMES["success"], width=2),
            marker=dict(size=4)
        ))
        
        fig.update_layout(
            title='Report Generation and Delivery Trends',
            xaxis_title='Date',
            yaxis_title='Number of Reports',
            height=400,
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("---")
    
    def _render_report_scheduler(self):
        """Render report scheduler section."""
        st.subheader("⏰ Report Scheduler")
        
        # Scheduler configuration
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("### 📅 Schedule New Report")
            
            # Report scheduling form
            with st.form("schedule_report_form"):
                col_a, col_b = st.columns(2)
                
                with col_a:
                    report_name = st.text_input("Report Name", placeholder="e.g., Weekly Quality Summary")
                    report_type = st.selectbox(
                        "Report Type",
                        options=["Quality Summary", "Performance Dashboard", "ROI Analysis", 
                                "Predictive Insights", "Custom Report"],
                        index=0
                    )
                    frequency = st.selectbox(
                        "Frequency",
                        options=["Daily", "Weekly", "Monthly", "Quarterly", "On-Demand"],
                        index=1
                    )
                
                with col_b:
                    recipients = st.text_area(
                        "Recipients (one per line)",
                        placeholder="manager@company.com\nteam@company.com",
                        height=100
                    )
                    format_type = st.selectbox(
                        "Format",
                        options=["PDF", "Excel", "PowerPoint", "Email Summary"],
                        index=0
                    )
                    start_date = st.date_input("Start Date", value=datetime.now().date())
                
                # Advanced settings
                with st.expander("🔧 Advanced Settings"):
                    time_of_day = st.time_input("Generation Time", value=datetime.strptime("08:00", "%H:%M").time())
                    timezone = st.selectbox("Timezone", options=["UTC", "EST", "PST", "GMT"], index=1)
                    
                    include_charts = st.checkbox("Include Charts", value=True)
                    include_raw_data = st.checkbox("Include Raw Data", value=False)
                    compress_files = st.checkbox("Compress Large Files", value=True)
                    
                    custom_filters = st.text_area(
                        "Custom Filters (JSON format)",
                        placeholder='{"quality_threshold": 0.85, "production_line": "all"}',
                        height=60
                    )
                
                submitted = st.form_submit_button("🚀 Schedule Report", type="primary")
                
                if submitted:
                    if report_name and recipients:
                        st.success(f"✅ Report '{report_name}' scheduled successfully!")
                        st.info(f"📧 Next generation: {start_date} at {time_of_day}")
                    else:
                        st.error("❌ Please fill in required fields (Report Name and Recipients)")
        
        with col2:
            st.markdown("### 📊 Active Schedules")
            
            active_schedules = self._get_active_schedules()
            
            for schedule in active_schedules:
                with st.container():
                    st.markdown(f"**{schedule['name']}**")
                    st.write(f"📅 {schedule['frequency']} at {schedule['time']}")
                    st.write(f"👥 {schedule['recipients']} recipients")
                    st.write(f"📄 Next: {schedule['next_run']}")
                    
                    col_x, col_y, col_z = st.columns(3)
                    with col_x:
                        if st.button("▶️", key=f"run_{schedule['id']}", help="Run Now"):
                            st.success("Report queued!")
                    with col_y:
                        if st.button("⏸️", key=f"pause_{schedule['id']}", help="Pause"):
                            st.info("Schedule paused")
                    with col_z:
                        if st.button("🗑️", key=f"delete_{schedule['id']}", help="Delete"):
                            st.warning("Schedule deleted")
                    
                    st.divider()
        
        st.markdown("---")
    
    def _render_report_templates(self):
        """Render report templates section."""
        st.subheader("📝 Report Templates")
        
        # Template categories
        template_categories = ["Quality Reports", "Performance Reports", "Financial Reports", "Custom Templates"]
        selected_category = st.selectbox("Template Category", template_categories, index=0)
        
        templates = self._get_report_templates(selected_category)
        
        # Display templates in grid
        cols = st.columns(3)
        
        for i, template in enumerate(templates):
            with cols[i % 3]:
                with st.container():
                    st.markdown(f"### {template['name']}")
                    st.write(template['description'])
                    
                    # Template preview
                    if template.get('preview_chart'):
                        # Simple preview chart
                        preview_data = np.random.normal(0.85, 0.1, 10)
                        fig = px.line(y=preview_data, title="Preview", height=200)
                        fig.update_layout(showlegend=False, margin=dict(l=0,r=0,t=30,b=0))
                        st.plotly_chart(fig, use_container_width=True)
                    
                    col_a, col_b = st.columns(2)
                    with col_a:
                        if st.button("📋 Use Template", key=f"use_{template['id']}"):
                            st.success(f"Template '{template['name']}' selected!")
                    with col_b:
                        if st.button("👁️ Preview", key=f"preview_{template['id']}"):
                            self._show_template_preview(template)
                    
                    # Template stats
                    st.caption(f"Used {template['usage_count']} times | Rating: {'⭐' * template['rating']}")
        
        # Custom template builder
        with st.expander("🏗️ Create Custom Template"):
            st.markdown("### Template Builder")
            
            col1, col2 = st.columns(2)
            
            with col1:
                template_name = st.text_input("Template Name")
                template_description = st.text_area("Description", height=100)
                
                # Sections selection
                st.markdown("**Include Sections:**")
                sections = {
                    "Executive Summary": st.checkbox("Executive Summary", value=True),
                    "Key Metrics": st.checkbox("Key Metrics", value=True),
                    "Charts & Graphs": st.checkbox("Charts & Graphs", value=True),
                    "Trend Analysis": st.checkbox("Trend Analysis", value=False),
                    "Recommendations": st.checkbox("Recommendations", value=False),
                    "Raw Data": st.checkbox("Raw Data Appendix", value=False)
                }
            
            with col2:
                # Layout options
                st.markdown("**Layout Options:**")
                layout_style = st.selectbox("Layout Style", ["Standard", "Executive", "Technical", "Summary"])
                color_scheme = st.selectbox("Color Scheme", ["Corporate Blue", "Modern Green", "Classic Gray", "Custom"])
                
                # Chart preferences
                st.markdown("**Chart Preferences:**")
                default_chart_type = st.selectbox("Default Chart Type", ["Line", "Bar", "Area", "Mixed"])
                include_data_tables = st.checkbox("Include Data Tables", value=True)
                chart_animations = st.checkbox("Chart Animations", value=False)
            
            if st.button("💾 Save Template"):
                if template_name:
                    st.success(f"✅ Template '{template_name}' saved successfully!")
                else:
                    st.error("❌ Please provide a template name")
        
        st.markdown("---")
    
    def _render_report_generation(self):
        """Render report generation section."""
        st.subheader("⚡ Generate Reports")
        
        # Quick report generation
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("### 🚀 Quick Report Generation")
            
            # Report configuration
            report_config = {}
            
            col_a, col_b = st.columns(2)
            
            with col_a:
                report_config['type'] = st.selectbox(
                    "Report Type",
                    options=["Quality Dashboard", "Performance Summary", "ROI Analysis", 
                            "Predictive Insights", "Custom Report"],
                    index=0
                )
                
                report_config['date_range'] = st.selectbox(
                    "Date Range",
                    options=["Last 7 days", "Last 30 days", "Last 90 days", "Custom Range"],
                    index=1
                )
                
                if report_config['date_range'] == "Custom Range":
                    start_date = st.date_input("Start Date")
                    end_date = st.date_input("End Date")
                    report_config['custom_start'] = start_date
                    report_config['custom_end'] = end_date
            
            with col_b:
                report_config['format'] = st.selectbox(
                    "Output Format",
                    options=["PDF", "Excel", "PowerPoint", "HTML"],
                    index=0
                )
                
                report_config['quality'] = st.selectbox(
                    "Report Quality",
                    options=["Draft", "Standard", "High Quality", "Presentation Ready"],
                    index=1
                )
                
                report_config['include_charts'] = st.checkbox("Include Charts", value=True)
                report_config['include_summary'] = st.checkbox("Include Executive Summary", value=True)
            
            # Advanced options
            with st.expander("🔧 Advanced Options"):
                col_x, col_y = st.columns(2)
                
                with col_x:
                    report_config['filters'] = st.multiselect(
                        "Apply Filters",
                        options=["Production Line A", "Production Line B", "High Priority Items", "Quality Issues"],
                        default=[]
                    )
                    
                    report_config['language'] = st.selectbox("Language", ["English", "Spanish", "French", "German"], index=0)
                
                with col_y:
                    report_config['delivery_method'] = st.selectbox(
                        "Delivery Method",
                        options=["Download", "Email", "Shared Drive", "Cloud Storage"],
                        index=0
                    )
                    
                    if report_config['delivery_method'] == "Email":
                        report_config['email_recipients'] = st.text_input("Email Recipients")
            
            # Generate button
            if st.button("📊 Generate Report", type="primary"):
                with st.spinner("Generating report..."):
                    # Simulate report generation
                    import time
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    steps = [
                        "Collecting data...",
                        "Processing analytics...",
                        "Creating charts...",
                        "Generating report...",
                        "Finalizing document..."
                    ]
                    
                    for i, step in enumerate(steps):
                        status_text.text(step)
                        time.sleep(0.5)  # Simulate processing time
                        progress_bar.progress((i + 1) / len(steps))
                    
                    status_text.text("Report generated successfully!")
                    st.success("✅ Report generated successfully!")
                    
                    # Provide download link
                    self._create_download_link(report_config)
        
        with col2:
            st.markdown("### 📊 Generation Queue")
            
            # Show current generation queue
            queue_items = self._get_generation_queue()
            
            if queue_items:
                for item in queue_items:
                    with st.container():
                        st.write(f"**{item['report_name']}**")
                        st.write(f"Status: {item['status']}")
                        st.write(f"Progress: {item['progress']}%")
                        
                        if item['status'] == "Processing":
                            st.progress(item['progress'] / 100)
                        
                        st.caption(f"ETA: {item['eta']}")
                        st.divider()
            else:
                st.info("No reports in queue")
            
            # Recent completions
            st.markdown("### ✅ Recent Completions")
            
            recent_reports = self._get_recent_completions()
            
            for report in recent_reports:
                with st.container():
                    st.write(f"**{report['name']}**")
                    st.write(f"Completed: {report['completed_time']}")
                    
                    col_x, col_y = st.columns(2)
                    with col_x:
                        if st.button("📥", key=f"download_{report['id']}", help="Download"):
                            st.success("Download started")
                    with col_y:
                        if st.button("📧", key=f"resend_{report['id']}", help="Resend"):
                            st.info("Report resent")
                    
                    st.divider()
        
        st.markdown("---")
    
    def _render_report_history(self):
        """Render report history section."""
        st.subheader("📚 Report History")
        
        # History filters
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            date_filter = st.selectbox("Date Range", ["All Time", "Last 30 days", "Last 90 days", "This Year"])
        
        with col2:
            type_filter = st.multiselect("Report Type", ["Quality", "Performance", "ROI", "Predictive", "Custom"])
        
        with col3:
            status_filter = st.multiselect("Status", ["Completed", "Failed", "In Progress", "Scheduled"])
        
        with col4:
            format_filter = st.multiselect("Format", ["PDF", "Excel", "PowerPoint", "HTML"])
        
        # Report history table
        history_data = self._get_report_history()
        
        # Apply filters
        if type_filter:
            history_data = history_data[history_data['type'].isin(type_filter)]
        if status_filter:
            history_data = history_data[history_data['status'].isin(status_filter)]
        if format_filter:
            history_data = history_data[history_data['format'].isin(format_filter)]
        
        # Display history table with actions
        st.markdown("### 📋 Report History")
        
        for index, report in history_data.iterrows():
            with st.expander(f"{report['name']} - {report['generated_date']}", expanded=False):
                col1, col2, col3 = st.columns([2, 1, 1])
                
                with col1:
                    st.write(f"**Type:** {report['type']}")
                    st.write(f"**Format:** {report['format']}")
                    st.write(f"**Size:** {report['file_size']}")
                    st.write(f"**Recipients:** {report['recipients']}")
                    st.write(f"**Generation Time:** {report['generation_time']}s")
                
                with col2:
                    # Status badge
                    status_color = {
                        "Completed": "🟢",
                        "Failed": "🔴", 
                        "In Progress": "🟡",
                        "Scheduled": "🔵"
                    }
                    st.write(f"**Status:** {status_color.get(report['status'], '⚪')} {report['status']}")
                    
                    if report['error_message']:
                        st.error(f"Error: {report['error_message']}")
                
                with col3:
                    # Action buttons
                    if report['status'] == "Completed":
                        if st.button("📥 Download", key=f"hist_download_{index}"):
                            st.success("Download started")
                        
                        if st.button("📧 Resend", key=f"hist_resend_{index}"):
                            st.info("Report resent")
                        
                        if st.button("🔄 Regenerate", key=f"hist_regen_{index}"):
                            st.info("Report queued for regeneration")
                    
                    elif report['status'] == "Failed":
                        if st.button("🔄 Retry", key=f"hist_retry_{index}"):
                            st.info("Report queued for retry")
                    
                    if st.button("🗑️ Delete", key=f"hist_delete_{index}"):
                        st.warning("Report deleted")
        
        # History analytics
        col1, col2 = st.columns(2)
        
        with col1:
            # Success rate over time
            success_data = self._get_success_rate_trend()
            
            fig = px.line(
                success_data,
                x='date',
                y='success_rate',
                title='Report Success Rate Trend',
                labels={'success_rate': 'Success Rate (%)', 'date': 'Date'}
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Report type distribution
            type_distribution = history_data['type'].value_counts()
            
            fig = px.pie(
                values=type_distribution.values,
                names=type_distribution.index,
                title='Report Type Distribution',
                height=300
            )
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
    
    def _render_distribution_management(self):
        """Render distribution management section."""
        st.subheader("📤 Distribution Management")
        
        # Distribution channels
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 📧 Email Distribution")
            
            # Email groups management
            email_groups = self._get_email_groups()
            
            selected_group = st.selectbox("Email Groups", ["All Groups"] + list(email_groups.keys()))
            
            if selected_group != "All Groups":
                group_members = email_groups[selected_group]
                st.write(f"**Members ({len(group_members)}):**")
                for member in group_members:
                    st.write(f"• {member}")
            
            # Add new group
            with st.expander("➕ Add New Group"):
                group_name = st.text_input("Group Name")
                group_members = st.text_area("Members (one per line)", height=100)
                
                if st.button("Create Group"):
                    if group_name and group_members:
                        st.success(f"Group '{group_name}' created!")
            
            # Email settings
            st.markdown("**Email Settings:**")
            email_template = st.selectbox("Email Template", ["Standard", "Executive", "Technical"])
            include_attachment = st.checkbox("Include Report as Attachment", value=True)
            embed_charts = st.checkbox("Embed Charts in Email", value=False)
        
        with col2:
            st.markdown("### ☁️ Cloud Storage")
            
            # Cloud storage options
            storage_options = ["Google Drive", "SharePoint", "Dropbox", "AWS S3", "Custom FTP"]
            
            for storage in storage_options:
                with st.container():
                    col_a, col_b, col_c = st.columns([2, 1, 1])
                    
                    with col_a:
                        st.write(f"**{storage}**")
                        status = "🟢 Connected" if storage in ["Google Drive", "SharePoint"] else "🔴 Not Connected"
                        st.caption(status)
                    
                    with col_b:
                        if st.button("⚙️", key=f"config_{storage}", help="Configure"):
                            st.info(f"Configuring {storage}")
                    
                    with col_c:
                        if st.button("🔗", key=f"connect_{storage}", help="Connect"):
                            st.success(f"Connected to {storage}")
            
            # Storage settings
            st.markdown("**Storage Settings:**")
            auto_organize = st.checkbox("Auto-organize by Date", value=True)
            retention_period = st.selectbox("Retention Period", ["30 days", "90 days", "1 year", "Forever"])
            compress_files = st.checkbox("Compress Files", value=True)
        
        # Distribution analytics
        st.markdown("### 📊 Distribution Analytics")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Delivery success rate
            delivery_data = self._get_delivery_analytics()
            
            fig = px.bar(
                x=list(delivery_data.keys()),
                y=list(delivery_data.values()),
                title='Delivery Success Rate by Channel',
                labels={'x': 'Channel', 'y': 'Success Rate (%)'}
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Email open rates
            open_rates = self._get_email_open_rates()
            
            fig = px.line(
                open_rates,
                x='date',
                y='open_rate',
                title='Email Open Rates',
                labels={'open_rate': 'Open Rate (%)', 'date': 'Date'}
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        
        with col3:
            # Storage usage
            storage_usage = self._get_storage_usage()
            
            fig = px.pie(
                values=list(storage_usage.values()),
                names=list(storage_usage.keys()),
                title='Storage Usage by Type',
                height=300
            )
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
    
    def _render_report_analytics(self):
        """Render report analytics section."""
        st.subheader("📈 Report Analytics")
        
        # Analytics overview
        analytics_data = self._get_report_analytics()
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            create_metric_card(
                title="Most Popular Report",
                value=analytics_data['most_popular'],
                help_text="Most frequently generated report type"
            )
        
        with col2:
            create_metric_card(
                title="Avg View Time",
                value=f"{analytics_data['avg_view_time']:.1f} min",
                help_text="Average time spent viewing reports"
            )
        
        with col3:
            create_metric_card(
                title="Download Rate",
                value=f"{analytics_data['download_rate']:.1f}%",
                help_text="Percentage of reports downloaded"
            )
        
        with col4:
            create_metric_card(
                title="User Satisfaction",
                value=f"{analytics_data['satisfaction_score']:.1f}/5",
                help_text="Average user satisfaction rating"
            )
        
        # Detailed analytics
        col1, col2 = st.columns(2)
        
        with col1:
            # Report usage patterns
            usage_data = self._get_usage_patterns()
            
            fig = px.heatmap(
                usage_data,
                x='hour',
                y='day_of_week',
                color='usage_count',
                title='Report Generation Patterns',
                labels={'usage_count': 'Reports Generated', 'hour': 'Hour of Day', 'day_of_week': 'Day of Week'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # User engagement metrics
            engagement_data = self._get_engagement_metrics()
            
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                name='Views',
                x=engagement_data['report_type'],
                y=engagement_data['views'],
                yaxis='y'
            ))
            
            fig.add_trace(go.Scatter(
                name='Engagement Score',
                x=engagement_data['report_type'],
                y=engagement_data['engagement_score'],
                yaxis='y2',
                mode='lines+markers',
                line=dict(color='red')
            ))
            
            fig.update_layout(
                title='Report Engagement Metrics',
                yaxis=dict(title='Views', side='right'),
                yaxis2=dict(title='Engagement Score', side='left', overlaying='y'),
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
    
    def _render_custom_reports(self):
        """Render custom reports section."""
        st.subheader("🛠️ Custom Reports")
        
        # Custom report builder
        st.markdown("### 🏗️ Report Builder")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Data source selection
            st.markdown("**1. Select Data Sources**")
            data_sources = st.multiselect(
                "Available Data Sources",
                options=["Quality Metrics", "Production Data", "Performance KPIs", "Financial Data", 
                        "Predictive Analytics", "Historical Trends", "Real-time Monitoring"],
                default=["Quality Metrics", "Production Data"]
            )
            
            # Metrics selection
            st.markdown("**2. Choose Metrics**")
            
            available_metrics = {
                "Quality Metrics": ["Quality Score", "Defect Rate", "Compliance Rate", "Customer Satisfaction"],
                "Production Data": ["Throughput", "Efficiency", "Downtime", "Yield"],
                "Performance KPIs": ["OEE", "MTBF", "MTTR", "Availability"],
                "Financial Data": ["Cost per Unit", "ROI", "Revenue", "Savings"]
            }
            
            selected_metrics = []
            for source in data_sources:
                if source in available_metrics:
                    metrics = st.multiselect(f"{source} Metrics", available_metrics[source])
                    selected_metrics.extend(metrics)
            
            # Visualization options
            st.markdown("**3. Visualization Options**")
            
            col_a, col_b = st.columns(2)
            
            with col_a:
                chart_types = st.multiselect(
                    "Chart Types",
                    options=["Line Chart", "Bar Chart", "Pie Chart", "Heatmap", "Gauge", "Table"],
                    default=["Line Chart", "Bar Chart"]
                )
                
                time_range = st.selectbox("Default Time Range", ["Last 7 days", "Last 30 days", "Last 90 days"])
            
            with col_b:
                grouping = st.selectbox("Group By", ["None", "Production Line", "Shift", "Product Type"])
                
                aggregation = st.selectbox("Aggregation", ["Average", "Sum", "Count", "Min", "Max"])
            
            # Report layout
            st.markdown("**4. Report Layout**")
            
            layout_cols = st.columns(3)
            
            with layout_cols[0]:
                include_summary = st.checkbox("Executive Summary", value=True)
                include_charts = st.checkbox("Charts Section", value=True)
            
            with layout_cols[1]:
                include_tables = st.checkbox("Data Tables", value=False)
                include_trends = st.checkbox("Trend Analysis", value=True)
            
            with layout_cols[2]:
                include_recommendations = st.checkbox("Recommendations", value=False)
                include_appendix = st.checkbox("Data Appendix", value=False)
        
        with col2:
            # Report preview
            st.markdown("**📊 Report Preview**")
            
            if selected_metrics:
                # Generate sample preview
                preview_data = pd.DataFrame({
                    'Date': pd.date_range('2024-09-01', periods=10, freq='D'),
                    'Value': np.random.normal(0.85, 0.1, 10)
                })
                
                if "Line Chart" in chart_types:
                    fig = px.line(preview_data, x='Date', y='Value', title='Sample Chart Preview')
                    fig.update_layout(height=250, margin=dict(l=0,r=0,t=30,b=0))
                    st.plotly_chart(fig, use_container_width=True)
                
                st.write("**Selected Metrics:**")
                for metric in selected_metrics[:5]:  # Show first 5
                    st.write(f"• {metric}")
                
                if len(selected_metrics) > 5:
                    st.write(f"... and {len(selected_metrics) - 5} more")
            else:
                st.info("Select metrics to preview report structure")
        
        # Save custom report
        if st.button("💾 Save Custom Report", type="primary"):
            if selected_metrics:
                report_name = st.text_input("Report Name", value="Custom Report")
                if report_name:
                    st.success(f"✅ Custom report '{report_name}' saved successfully!")
            else:
                st.error("❌ Please select at least one metric")
        
        # Existing custom reports
        st.markdown("### 📋 Saved Custom Reports")
        
        custom_reports = self._get_custom_reports()
        
        if custom_reports:
            for report in custom_reports:
                with st.expander(f"{report['name']} - Created {report['created_date']}"):
                    col1, col2, col3 = st.columns([2, 1, 1])
                    
                    with col1:
                        st.write(f"**Metrics:** {', '.join(report['metrics'][:3])}...")
                        st.write(f"**Data Sources:** {', '.join(report['data_sources'])}")
                        st.write(f"**Last Generated:** {report['last_generated']}")
                    
                    with col2:
                        if st.button("📊 Generate", key=f"gen_custom_{report['id']}"):
                            st.success("Report generation started!")
                        
                        if st.button("✏️ Edit", key=f"edit_custom_{report['id']}"):
                            st.info("Opening report editor...")
                    
                    with col3:
                        if st.button("📧 Schedule", key=f"schedule_custom_{report['id']}"):
                            st.info("Opening scheduler...")
                        
                        if st.button("🗑️ Delete", key=f"delete_custom_{report['id']}"):
                            st.warning("Report deleted")
        else:
            st.info("No custom reports saved yet")
        
        # Export options
        st.markdown("---")
        reports_summary = self._get_reports_overview_data()
        render_export_options(
            data=reports_summary,
            filename_prefix="automated_reports"
        )
    
    # Helper methods for data generation and UI components
    def _get_reports_overview_data(self) -> Dict[str, Any]:
        """Get reports overview data."""
        return {
            "total_reports_generated": 2847,
            "reports_increase": 234,
            "active_schedules": 18,
            "success_rate": 97.3,
            "success_improvement": 1.8,
            "avg_generation_time": 12.4,
            "time_reduction": 2.1,
            "total_recipients": 156,
            "storage_used_gb": 4.7
        }
    
    def _get_report_trend_data(self) -> pd.DataFrame:
        """Generate report trend data."""
        dates = pd.date_range('2024-08-01', periods=30, freq='D')
        
        # Generate realistic report generation pattern
        base_reports = 25
        reports_generated = base_reports + np.random.poisson(10, 30)
        successful_deliveries = reports_generated * np.random.uniform(0.92, 0.99, 30)
        
        return pd.DataFrame({
            'date': dates,
            'reports_generated': reports_generated,
            'successful_deliveries': successful_deliveries.astype(int)
        })
    
    def _get_active_schedules(self) -> List[Dict[str, Any]]:
        """Get active report schedules."""
        return [
            {
                'id': 1,
                'name': 'Weekly Quality Summary',
                'frequency': 'Weekly',
                'time': '08:00 AM',
                'recipients': 12,
                'next_run': '2024-09-20 08:00'
            },
            {
                'id': 2,
                'name': 'Daily Performance Report',
                'frequency': 'Daily',
                'time': '06:30 AM',
                'recipients': 8,
                'next_run': '2024-09-17 06:30'
            },
            {
                'id': 3,
                'name': 'Monthly ROI Analysis',
                'frequency': 'Monthly',
                'time': '09:00 AM',
                'recipients': 5,
                'next_run': '2024-10-01 09:00'
            }
        ]
    
    def _get_report_templates(self, category: str) -> List[Dict[str, Any]]:
        """Get report templates for a category."""
        templates = {
            "Quality Reports": [
                {'id': 1, 'name': 'Quality Dashboard', 'description': 'Comprehensive quality metrics and trends', 'usage_count': 145, 'rating': 5, 'preview_chart': True},
                {'id': 2, 'name': 'Defect Analysis', 'description': 'Detailed defect tracking and root cause analysis', 'usage_count': 89, 'rating': 4, 'preview_chart': True},
                {'id': 3, 'name': 'Compliance Report', 'description': 'Regulatory compliance and audit readiness', 'usage_count': 67, 'rating': 5, 'preview_chart': False}
            ],
            "Performance Reports": [
                {'id': 4, 'name': 'OEE Dashboard', 'description': 'Overall Equipment Effectiveness metrics', 'usage_count': 123, 'rating': 5, 'preview_chart': True},
                {'id': 5, 'name': 'Production Summary', 'description': 'Daily production performance and KPIs', 'usage_count': 156, 'rating': 4, 'preview_chart': True},
                {'id': 6, 'name': 'Efficiency Analysis', 'description': 'Process efficiency and optimization opportunities', 'usage_count': 78, 'rating': 4, 'preview_chart': True}
            ],
            "Financial Reports": [
                {'id': 7, 'name': 'ROI Dashboard', 'description': 'Return on investment analysis and projections', 'usage_count': 45, 'rating': 5, 'preview_chart': True},
                {'id': 8, 'name': 'Cost Analysis', 'description': 'Detailed cost breakdown and trends', 'usage_count': 67, 'rating': 4, 'preview_chart': True},
                {'id': 9, 'name': 'Budget Report', 'description': 'Budget vs actual performance tracking', 'usage_count': 34, 'rating': 4, 'preview_chart': False}
            ],
            "Custom Templates": [
                {'id': 10, 'name': 'Executive Summary', 'description': 'High-level overview for executives', 'usage_count': 89, 'rating': 5, 'preview_chart': False},
                {'id': 11, 'name': 'Technical Report', 'description': 'Detailed technical analysis and data', 'usage_count': 56, 'rating': 4, 'preview_chart': True},
                {'id': 12, 'name': 'Customer Report', 'description': 'Customer-facing quality and service metrics', 'usage_count': 23, 'rating': 3, 'preview_chart': False}
            ]
        }
        
        return templates.get(category, [])
    
    def _show_template_preview(self, template: Dict[str, Any]):
        """Show template preview in modal."""
        st.info(f"Preview for '{template['name']}' would open in a modal window")
    
    def _create_download_link(self, config: Dict[str, Any]):
        """Create download link for generated report."""
        # Simulate file creation
        if config['format'] == 'PDF':
            st.download_button(
                label="📥 Download PDF Report",
                data=b"PDF content would be here",
                file_name=f"quality_report_{datetime.now().strftime('%Y%m%d')}.pdf",
                mime="application/pdf"
            )
        elif config['format'] == 'Excel':
            st.download_button(
                label="📥 Download Excel Report", 
                data=b"Excel content would be here",
                file_name=f"quality_report_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    
    def _get_generation_queue(self) -> List[Dict[str, Any]]:
        """Get current generation queue."""
        return [
            {'report_name': 'Weekly Quality Summary', 'status': 'Processing', 'progress': 75, 'eta': '2 minutes'},
            {'report_name': 'Monthly ROI Analysis', 'status': 'Queued', 'progress': 0, 'eta': '8 minutes'},
            {'report_name': 'Custom Performance Report', 'status': 'Processing', 'progress': 25, 'eta': '5 minutes'}
        ]
    
    def _get_recent_completions(self) -> List[Dict[str, Any]]:
        """Get recently completed reports."""
        return [
            {'id': 1, 'name': 'Daily Quality Report', 'completed_time': '10:30 AM'},
            {'id': 2, 'name': 'Performance Dashboard', 'completed_time': '09:45 AM'},
            {'id': 3, 'name': 'Executive Summary', 'completed_time': '08:15 AM'}
        ]
    
    def _get_report_history(self) -> pd.DataFrame:
        """Generate report history data."""
        return pd.DataFrame({
            'name': [
                'Weekly Quality Summary',
                'Daily Performance Report', 
                'Monthly ROI Analysis',
                'Custom Analytics Report',
                'Executive Dashboard',
                'Compliance Report'
            ],
            'type': ['Quality', 'Performance', 'ROI', 'Custom', 'Executive', 'Quality'],
            'format': ['PDF', 'Excel', 'PowerPoint', 'PDF', 'PDF', 'Excel'],
            'status': ['Completed', 'Completed', 'Failed', 'Completed', 'Completed', 'Completed'],
            'generated_date': ['2024-09-15', '2024-09-15', '2024-09-14', '2024-09-14', '2024-09-13', '2024-09-13'],
            'file_size': ['2.3 MB', '1.8 MB', 'N/A', '4.1 MB', '1.2 MB', '3.2 MB'],
            'recipients': [12, 8, 5, 3, 15, 6],
            'generation_time': [8.2, 5.4, 0, 12.1, 6.8, 9.3],
            'error_message': ['', '', 'Data source timeout', '', '', '']
        })
    
    def _get_success_rate_trend(self) -> pd.DataFrame:
        """Generate success rate trend data."""
        dates = pd.date_range('2024-08-01', periods=30, freq='D')
        success_rates = 95 + np.random.normal(0, 2, 30)
        success_rates = np.clip(success_rates, 85, 100)
        
        return pd.DataFrame({
            'date': dates,
            'success_rate': success_rates
        })
    
    def _get_email_groups(self) -> Dict[str, List[str]]:
        """Get email distribution groups."""
        return {
            "Executive Team": ["ceo@company.com", "coo@company.com", "cfo@company.com"],
            "Quality Managers": ["quality.mgr1@company.com", "quality.mgr2@company.com", "quality.lead@company.com"],
            "Production Team": ["prod.mgr@company.com", "shift.lead1@company.com", "shift.lead2@company.com"],
            "Engineering": ["eng.director@company.com", "process.eng@company.com", "qa.eng@company.com"]
        }
    
    def _get_delivery_analytics(self) -> Dict[str, float]:
        """Get delivery success rates by channel."""
        return {
            "Email": 98.5,
            "SharePoint": 99.2,
            "Google Drive": 97.8,
            "FTP": 95.1,
            "Cloud Storage": 99.0
        }
    
    def _get_email_open_rates(self) -> pd.DataFrame:
        """Generate email open rate data."""
        dates = pd.date_range('2024-08-01', periods=30, freq='D')
        open_rates = 65 + np.random.normal(0, 8, 30)
        open_rates = np.clip(open_rates, 45, 85)
        
        return pd.DataFrame({
            'date': dates,
            'open_rate': open_rates
        })
    
    def _get_storage_usage(self) -> Dict[str, float]:
        """Get storage usage by file type."""
        return {
            "PDF Reports": 2.1,
            "Excel Files": 1.3,
            "PowerPoint": 0.8,
            "Images": 0.5
        }
    
    def _get_report_analytics(self) -> Dict[str, Any]:
        """Get report analytics data."""
        return {
            "most_popular": "Quality Dashboard",
            "avg_view_time": 4.2,
            "download_rate": 78.5,
            "satisfaction_score": 4.3
        }
    
    def _get_usage_patterns(self) -> pd.DataFrame:
        """Generate usage pattern heatmap data."""
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        hours = list(range(24))
        
        # Create realistic usage patterns (more during business hours)
        data = []
        for day in days:
            for hour in hours:
                if day in ['Saturday', 'Sunday']:
                    usage = np.random.poisson(2)  # Low weekend usage
                elif 8 <= hour <= 17:
                    usage = np.random.poisson(15)  # High business hours usage
                else:
                    usage = np.random.poisson(5)  # Medium off-hours usage
                
                data.append({'day_of_week': day, 'hour': hour, 'usage_count': usage})
        
        df = pd.DataFrame(data)
        return df.pivot(index='day_of_week', columns='hour', values='usage_count')
    
    def _get_engagement_metrics(self) -> pd.DataFrame:
        """Generate engagement metrics data."""
        return pd.DataFrame({
            'report_type': ['Quality', 'Performance', 'ROI', 'Predictive', 'Custom'],
            'views': [1250, 980, 456, 678, 234],
            'engagement_score': [8.2, 7.5, 6.8, 7.9, 5.4]
        })
    
    def _get_custom_reports(self) -> List[Dict[str, Any]]:
        """Get saved custom reports."""
        return [
            {
                'id': 1,
                'name': 'Production Line Analysis',
                'metrics': ['Throughput', 'Quality Score', 'Efficiency', 'Downtime'],
                'data_sources': ['Production Data', 'Quality Metrics'],
                'created_date': '2024-09-10',
                'last_generated': '2024-09-15'
            },
            {
                'id': 2,
                'name': 'Executive KPI Dashboard',
                'metrics': ['Overall ROI', 'Quality Score', 'Customer Satisfaction'],
                'data_sources': ['Financial Data', 'Quality Metrics'],
                'created_date': '2024-09-05',
                'last_generated': '2024-09-14'
            }
        ]

def render_automated_reports_page():
    """Render the Automated Reports page."""
    page = AutomatedReportsPage()
    page.render()