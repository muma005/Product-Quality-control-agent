"""Executive Dashboard page for high-level KPIs and business insights."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import numpy as np
from typing import Dict, Any, List, Optional

# Import common components
from utils.ui_helpers import (
    create_metric_card, format_large_number, format_currency, format_percentage,
    create_gauge_chart, create_pie_chart, create_bar_chart
)
from components.common_components import (
    render_header_section, render_kpi_metrics, render_time_range_selector,
    render_export_options, render_trend_indicators
)
from config.app_config import COLOR_SCHEMES, CHART_CONFIG

class ExecutiveDashboardPage:
    """Executive Dashboard page class."""
    
    def __init__(self):
        """Initialize Executive Dashboard page."""
        self.current_period = self._get_current_period()
        self.previous_period = self._get_previous_period()
    
    def render(self):
        """Render the Executive Dashboard page."""
        render_header_section(
            title="Executive Dashboard",
            description="High-level KPIs and strategic business insights",
            icon="📊"
        )
        
        # Time range selector
        start_date, end_date = render_time_range_selector("executive")
        
        # Main dashboard sections
        self._render_executive_summary()
        self._render_key_performance_indicators()
        self._render_business_impact_metrics()
        self._render_quality_trends()
        self._render_financial_overview()
        self._render_operational_efficiency()
        self._render_strategic_insights()
    
    def _render_executive_summary(self):
        """Render executive summary section."""
        st.subheader("📋 Executive Summary")
        
        summary_data = self._get_executive_summary_data()
        
        # High-level status indicators
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            overall_health = summary_data.get("overall_health", 0)
            if overall_health >= 90:
                status_color = "🟢"
                status_text = "Excellent"
            elif overall_health >= 80:
                status_color = "🟡"
                status_text = "Good"
            else:
                status_color = "🔴"
                status_text = "Needs Attention"
            
            st.markdown(f"""
            **Overall System Health**  
            {status_color} {status_text} ({overall_health:.1f}%)
            """)
        
        with col2:
            quality_trend = summary_data.get("quality_trend", 0)
            trend_arrow = "📈" if quality_trend > 0 else "📉" if quality_trend < 0 else "➡️"
            st.markdown(f"""
            **Quality Trend**  
            {trend_arrow} {quality_trend:+.1f}% vs last period
            """)
        
        with col3:
            cost_savings = summary_data.get("cost_savings", 0)
            st.markdown(f"""
            **Cost Savings (YTD)**  
            💰 {format_currency(cost_savings)}
            """)
        
        with col4:
            roi = summary_data.get("roi", 0)
            st.markdown(f"""
            **ROI on QC Investment**  
            📊 {format_percentage(roi)}
            """)
        
        # Executive summary text
        st.markdown("---")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("""
            **Key Highlights:**
            - Quality control system operating at optimal efficiency
            - Defect detection rate improved by 15% this quarter
            - Cost savings of $2.3M achieved through early defect detection
            - All production lines meeting quality targets
            - New AI model deployment showing 8% improvement in accuracy
            """)
        
        with col2:
            # Quick action items
            st.markdown("**Action Items:**")
            action_items = [
                "Review Line C performance metrics",
                "Schedule Q2 system upgrade",
                "Approve budget for new sensors"
            ]
            
            for item in action_items:
                st.markdown(f"• {item}")
        
        st.markdown("---")
    
    def _render_key_performance_indicators(self):
        """Render key performance indicators."""
        st.subheader("🎯 Key Performance Indicators")
        
        kpi_data = self._get_kpi_data()
        
        # Primary KPIs
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            create_metric_card(
                title="Overall Equipment Effectiveness",
                value=f"{kpi_data['oee']:.1f}%",
                delta=f"{kpi_data['oee_change']:+.1f}%",
                delta_color="normal" if kpi_data['oee_change'] > 0 else "inverse",
                help_text="Composite metric of availability, performance, and quality"
            )
        
        with col2:
            create_metric_card(
                title="First Pass Yield",
                value=f"{kpi_data['first_pass_yield']:.1f}%",
                delta=f"{kpi_data['fpy_change']:+.1f}%",
                delta_color="normal" if kpi_data['fpy_change'] > 0 else "inverse",
                help_text="Percentage of products that pass quality inspection on first attempt"
            )
        
        with col3:
            create_metric_card(
                title="Defect Detection Rate",
                value=f"{kpi_data['defect_detection_rate']:.1f}%",
                delta=f"{kpi_data['ddr_change']:+.1f}%",
                delta_color="normal" if kpi_data['ddr_change'] > 0 else "inverse",
                help_text="Percentage of defects caught by quality control system"
            )
        
        with col4:
            create_metric_card(
                title="Customer Complaints",
                value=kpi_data['customer_complaints'],
                delta=kpi_data['complaints_change'],
                delta_color="inverse" if kpi_data['complaints_change'] > 0 else "normal",
                help_text="Number of quality-related customer complaints this period"
            )
        
        with col5:
            create_metric_card(
                title="Quality Cost as % Revenue",
                value=f"{kpi_data['quality_cost_percent']:.2f}%",
                delta=f"{kpi_data['qc_change']:+.2f}%",
                delta_color="inverse" if kpi_data['qc_change'] > 0 else "normal",
                help_text="Total quality costs as percentage of revenue"
            )
        
        # KPI gauges
        st.markdown("### Performance Gauges")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            oee_gauge = create_gauge_chart(
                value=kpi_data['oee'],
                title="Overall Equipment Effectiveness (%)",
                threshold_good=85,
                threshold_warning=75
            )
            st.plotly_chart(oee_gauge, use_container_width=True)
        
        with col2:
            quality_gauge = create_gauge_chart(
                value=kpi_data['first_pass_yield'],
                title="First Pass Yield (%)",
                threshold_good=95,
                threshold_warning=90
            )
            st.plotly_chart(quality_gauge, use_container_width=True)
        
        with col3:
            detection_gauge = create_gauge_chart(
                value=kpi_data['defect_detection_rate'],
                title="Defect Detection Rate (%)",
                threshold_good=98,
                threshold_warning=95
            )
            st.plotly_chart(detection_gauge, use_container_width=True)
        
        st.markdown("---")
    
    def _render_business_impact_metrics(self):
        """Render business impact metrics."""
        st.subheader("💼 Business Impact")
        
        impact_data = self._get_business_impact_data()
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            create_metric_card(
                title="Revenue Protected",
                value=format_currency(impact_data['revenue_protected']),
                delta=f"{impact_data['revenue_protected_change']:+.1f}%",
                help_text="Revenue protected through early defect detection"
            )
        
        with col2:
            create_metric_card(
                title="Cost Avoidance",
                value=format_currency(impact_data['cost_avoidance']),
                delta=f"{impact_data['cost_avoidance_change']:+.1f}%",
                help_text="Costs avoided through preventive quality measures"
            )
        
        with col3:
            create_metric_card(
                title="Productivity Gain",
                value=f"{impact_data['productivity_gain']:.1f}%",
                delta=f"{impact_data['productivity_change']:+.1f}%",
                help_text="Productivity improvement from quality initiatives"
            )
        
        with col4:
            create_metric_card(
                title="Customer Satisfaction",
                value=f"{impact_data['customer_satisfaction']:.1f}/10",
                delta=f"{impact_data['satisfaction_change']:+.1f}",
                help_text="Average customer satisfaction rating"
            )
        
        # Business impact visualization
        col1, col2 = st.columns(2)
        
        with col1:
            # Cost breakdown pie chart
            cost_data = pd.DataFrame({
                'Category': ['Prevention', 'Appraisal', 'Internal Failure', 'External Failure'],
                'Cost': [impact_data['prevention_cost'], impact_data['appraisal_cost'], 
                        impact_data['internal_failure_cost'], impact_data['external_failure_cost']]
            })
            
            fig = create_pie_chart(
                df=cost_data,
                values_col='Cost',
                names_col='Category',
                title='Quality Cost Distribution'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # ROI trend chart
            roi_data = self._get_roi_trend_data()
            fig = px.line(
                roi_data,
                x='month',
                y='roi',
                title='ROI Trend - Last 12 Months',
                markers=True
            )
            fig.update_layout(
                yaxis_title="ROI (%)",
                xaxis_title="Month"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
    
    def _render_quality_trends(self):
        """Render quality trends section."""
        st.subheader("📈 Quality Performance Trends")
        
        # Quality trends over time
        quality_trends = self._get_quality_trends_data()
        
        # Multi-metric trend chart
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=['Defect Rate Trend', 'Quality Score Trend', 
                           'Customer Complaints', 'First Pass Yield'],
            vertical_spacing=0.12
        )
        
        # Defect Rate
        fig.add_trace(
            go.Scatter(
                x=quality_trends['date'],
                y=quality_trends['defect_rate'],
                mode='lines+markers',
                name='Defect Rate',
                line=dict(color='red', width=3)
            ),
            row=1, col=1
        )
        
        # Quality Score
        fig.add_trace(
            go.Scatter(
                x=quality_trends['date'],
                y=quality_trends['quality_score'],
                mode='lines+markers',
                name='Quality Score',
                line=dict(color='green', width=3)
            ),
            row=1, col=2
        )
        
        # Customer Complaints
        fig.add_trace(
            go.Bar(
                x=quality_trends['date'],
                y=quality_trends['complaints'],
                name='Complaints',
                marker_color='orange'
            ),
            row=2, col=1
        )
        
        # First Pass Yield
        fig.add_trace(
            go.Scatter(
                x=quality_trends['date'],
                y=quality_trends['first_pass_yield'],
                mode='lines+markers',
                name='First Pass Yield',
                line=dict(color='blue', width=3)
            ),
            row=2, col=2
        )
        
        fig.update_layout(
            height=600,
            showlegend=False,
            title_text="Quality Performance Trends - Last 6 Months"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Trend analysis insights
        with st.expander("📊 Trend Analysis Insights"):
            st.markdown("""
            **Key Insights from Quality Trends:**
            
            - **Defect Rate**: Consistent downward trend (-23% over 6 months)
            - **Quality Score**: Steady improvement with 12% increase
            - **Customer Complaints**: Reduced by 45% since implementing new QC measures
            - **First Pass Yield**: Achieved target of >95% consistently for last 3 months
            
            **Recommendations:**
            - Continue current quality improvement initiatives
            - Focus on maintaining consistency in performance
            - Consider expanding successful practices to other production lines
            """)
        
        st.markdown("---")
    
    def _render_financial_overview(self):
        """Render financial overview section."""
        st.subheader("💰 Financial Overview")
        
        financial_data = self._get_financial_data()
        
        # Financial metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            create_metric_card(
                title="Quality Investment",
                value=format_currency(financial_data['quality_investment']),
                delta=f"{financial_data['investment_change']:+.1f}%",
                help_text="Total investment in quality control systems"
            )
        
        with col2:
            create_metric_card(
                title="Cost of Poor Quality",
                value=format_currency(financial_data['copq']),
                delta=f"{financial_data['copq_change']:+.1f}%",
                delta_color="inverse" if financial_data['copq_change'] > 0 else "normal",
                help_text="Total cost of poor quality (COPQ)"
            )
        
        with col3:
            create_metric_card(
                title="Savings Generated",
                value=format_currency(financial_data['savings_generated']),
                delta=f"{financial_data['savings_change']:+.1f}%",
                help_text="Total savings from quality improvements"
            )
        
        with col4:
            create_metric_card(
                title="Payback Period",
                value=f"{financial_data['payback_period']:.1f} months",
                delta=f"{financial_data['payback_change']:+.1f}",
                delta_color="inverse" if financial_data['payback_change'] > 0 else "normal",
                help_text="Time to recover quality control investment"
            )
        
        # Financial breakdown chart
        col1, col2 = st.columns(2)
        
        with col1:
            # Investment vs Savings
            comparison_data = pd.DataFrame({
                'Category': ['Investment', 'Savings', 'Net Benefit'],
                'Amount': [
                    financial_data['quality_investment'],
                    financial_data['savings_generated'],
                    financial_data['savings_generated'] - financial_data['quality_investment']
                ]
            })
            
            fig = px.bar(
                comparison_data,
                x='Category',
                y='Amount',
                title='Investment vs Returns',
                color='Category',
                color_discrete_sequence=['red', 'green', 'blue']
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Monthly financial impact
            monthly_impact = self._get_monthly_financial_impact()
            fig = px.line(
                monthly_impact,
                x='month',
                y='cumulative_savings',
                title='Cumulative Savings Over Time',
                markers=True
            )
            fig.update_layout(
                yaxis_title="Cumulative Savings ($)",
                xaxis_title="Month"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
    
    def _render_operational_efficiency(self):
        """Render operational efficiency section."""
        st.subheader("⚙️ Operational Efficiency")
        
        efficiency_data = self._get_operational_efficiency_data()
        
        # Efficiency metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            create_metric_card(
                title="Production Efficiency",
                value=f"{efficiency_data['production_efficiency']:.1f}%",
                delta=f"{efficiency_data['production_change']:+.1f}%",
                help_text="Overall production line efficiency"
            )
        
        with col2:
            create_metric_card(
                title="Inspection Throughput",
                value=f"{format_large_number(efficiency_data['inspection_throughput'])}/hr",
                delta=f"{efficiency_data['throughput_change']:+.1f}%",
                help_text="Number of items inspected per hour"
            )
        
        with col3:
            create_metric_card(
                title="System Uptime",
                value=f"{efficiency_data['system_uptime']:.2f}%",
                delta=f"{efficiency_data['uptime_change']:+.2f}%",
                help_text="Quality control system availability"
            )
        
        # Efficiency trends
        efficiency_trends = self._get_efficiency_trends()
        
        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=['Production Efficiency', 'System Performance']
        )
        
        # Production efficiency
        fig.add_trace(
            go.Scatter(
                x=efficiency_trends['date'],
                y=efficiency_trends['production_efficiency'],
                mode='lines+markers',
                name='Production Efficiency',
                line=dict(color='blue', width=3)
            ),
            row=1, col=1
        )
        
        # System performance
        fig.add_trace(
            go.Scatter(
                x=efficiency_trends['date'],
                y=efficiency_trends['system_performance'],
                mode='lines+markers',
                name='System Performance',
                line=dict(color='green', width=3)
            ),
            row=1, col=2
        )
        
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
    
    def _render_strategic_insights(self):
        """Render strategic insights section."""
        st.subheader("🎯 Strategic Insights & Recommendations")
        
        # Insights tabs
        tab1, tab2, tab3 = st.tabs(["📊 Performance Analysis", "🔮 Predictive Insights", "📋 Action Plan"])
        
        with tab1:
            self._render_performance_analysis()
        
        with tab2:
            self._render_predictive_insights()
        
        with tab3:
            self._render_action_plan()
        
        # Export options
        st.markdown("---")
        render_export_options(
            data=self._get_executive_summary_data(),
            filename_prefix="executive_dashboard"
        )
    
    def _render_performance_analysis(self):
        """Render performance analysis."""
        st.markdown("""
        ### 📈 Performance Analysis
        
        **Strengths:**
        - Defect detection rate consistently above 98%
        - Cost savings exceeded projections by 15%
        - Customer satisfaction improved significantly
        - System uptime maintained above 99.5%
        
        **Areas for Improvement:**
        - Line C showing higher variance in quality metrics
        - Inspection throughput could be optimized
        - Alert response time needs reduction
        
        **Benchmarking:**
        - Performance ranks in top 10% of industry standards
        - ROI exceeds industry average by 23%
        - Quality costs 18% below industry benchmark
        """)
        
        # Performance comparison chart
        benchmark_data = pd.DataFrame({
            'Metric': ['Defect Detection', 'First Pass Yield', 'System Uptime', 'ROI'],
            'Our Performance': [98.5, 96.2, 99.7, 245],
            'Industry Average': [95.2, 92.8, 98.1, 198],
            'Industry Best': [99.1, 98.5, 99.9, 280]
        })
        
        fig = px.bar(
            benchmark_data.melt(id_vars='Metric', var_name='Category', value_name='Value'),
            x='Metric',
            y='Value',
            color='Category',
            barmode='group',
            title='Performance vs Industry Benchmarks'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_predictive_insights(self):
        """Render predictive insights."""
        st.markdown("""
        ### 🔮 Predictive Insights
        
        **Quality Forecasts (Next Quarter):**
        - Defect rate projected to decrease by additional 8%
        - First pass yield expected to reach 97.5%
        - Customer complaints forecasted to drop by 25%
        
        **Investment Recommendations:**
        - Additional sensor deployment could improve detection by 12%
        - ML model upgrades projected to reduce false positives by 30%
        - Automated inspection expansion could increase throughput by 40%
        
        **Risk Assessment:**
        - Low risk of quality degradation
        - Medium risk of capacity constraints during peak season
        - Minimal risk of system failures
        """)
        
        # Forecasting chart
        forecast_data = self._get_forecast_data()
        
        fig = px.line(
            forecast_data,
            x='date',
            y=['actual', 'forecast'],
            title='Quality Score Forecast (Next 6 Months)',
            color_discrete_map={'actual': 'blue', 'forecast': 'red'}
        )
        fig.add_vrect(
            x0=forecast_data['date'].iloc[-6],
            x1=forecast_data['date'].iloc[-1],
            fillcolor='lightgray',
            opacity=0.3,
            annotation_text="Forecast Period"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_action_plan(self):
        """Render action plan."""
        st.markdown("""
        ### 📋 Strategic Action Plan
        
        **Immediate Actions (Next 30 Days):**
        1. Conduct Line C performance review
        2. Implement optimized inspection algorithms
        3. Deploy additional monitoring sensors
        4. Update alert response procedures
        
        **Short-term Goals (Next Quarter):**
        1. Achieve 99% defect detection rate
        2. Reduce quality costs by additional 10%
        3. Implement predictive maintenance
        4. Launch operator training program
        
        **Long-term Vision (Next Year):**
        1. Achieve industry-leading quality metrics
        2. Expand AI-driven quality control
        3. Implement fully automated inspection
        4. Establish center of excellence
        """)
        
        # Action plan progress tracking
        progress_data = pd.DataFrame({
            'Action': ['Sensor Deployment', 'Algorithm Update', 'Training Program', 'Process Optimization'],
            'Progress': [85, 60, 40, 75],
            'Target': [100, 100, 100, 100]
        })
        
        fig = px.bar(
            progress_data,
            x='Action',
            y=['Progress', 'Target'],
            title='Action Plan Progress',
            barmode='group',
            color_discrete_map={'Progress': 'green', 'Target': 'lightgray'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Helper methods for data generation
    def _get_current_period(self):
        """Get current period date range."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        return start_date, end_date
    
    def _get_previous_period(self):
        """Get previous period date range."""
        end_date = datetime.now() - timedelta(days=30)
        start_date = end_date - timedelta(days=30)
        return start_date, end_date
    
    def _get_executive_summary_data(self):
        """Get executive summary data."""
        return {
            "overall_health": 94.2,
            "quality_trend": 5.3,
            "cost_savings": 2_300_000,
            "roi": 245.8
        }
    
    def _get_kpi_data(self):
        """Get KPI data."""
        return {
            "oee": 87.3,
            "oee_change": 2.1,
            "first_pass_yield": 96.2,
            "fpy_change": 1.8,
            "defect_detection_rate": 98.5,
            "ddr_change": 0.9,
            "customer_complaints": 12,
            "complaints_change": -3,
            "quality_cost_percent": 1.85,
            "qc_change": -0.15
        }
    
    def _get_business_impact_data(self):
        """Get business impact data."""
        return {
            "revenue_protected": 8_500_000,
            "revenue_protected_change": 12.3,
            "cost_avoidance": 1_200_000,
            "cost_avoidance_change": 8.7,
            "productivity_gain": 15.6,
            "productivity_change": 2.3,
            "customer_satisfaction": 8.7,
            "satisfaction_change": 0.4,
            "prevention_cost": 400_000,
            "appraisal_cost": 300_000,
            "internal_failure_cost": 200_000,
            "external_failure_cost": 100_000
        }
    
    def _get_roi_trend_data(self):
        """Get ROI trend data."""
        dates = pd.date_range(start="2023-01-01", end="2023-12-01", freq="M")
        roi_values = np.cumsum(np.random.normal(15, 5, len(dates))) + 150
        
        return pd.DataFrame({
            "month": dates.strftime("%b %Y"),
            "roi": roi_values
        })
    
    def _get_quality_trends_data(self):
        """Get quality trends data."""
        dates = pd.date_range(start="2023-07-01", end="2024-01-01", freq="M")
        
        return pd.DataFrame({
            "date": dates,
            "defect_rate": np.random.normal(2.5, 0.3, len(dates)),
            "quality_score": np.random.normal(95, 2, len(dates)),
            "complaints": np.random.poisson(15, len(dates)),
            "first_pass_yield": np.random.normal(96, 1, len(dates))
        })
    
    def _get_financial_data(self):
        """Get financial data."""
        return {
            "quality_investment": 1_500_000,
            "investment_change": 5.2,
            "copq": 800_000,
            "copq_change": -18.5,
            "savings_generated": 2_300_000,
            "savings_change": 22.1,
            "payback_period": 8.2,
            "payback_change": -1.3
        }
    
    def _get_monthly_financial_impact(self):
        """Get monthly financial impact data."""
        dates = pd.date_range(start="2023-01-01", end="2024-01-01", freq="M")
        cumulative_savings = np.cumsum(np.random.normal(200_000, 50_000, len(dates)))
        
        return pd.DataFrame({
            "month": dates.strftime("%b %Y"),
            "cumulative_savings": cumulative_savings
        })
    
    def _get_operational_efficiency_data(self):
        """Get operational efficiency data."""
        return {
            "production_efficiency": 89.2,
            "production_change": 3.1,
            "inspection_throughput": 1850,
            "throughput_change": 7.2,
            "system_uptime": 99.74,
            "uptime_change": 0.12
        }
    
    def _get_efficiency_trends(self):
        """Get efficiency trends data."""
        dates = pd.date_range(start="2023-07-01", end="2024-01-01", freq="W")
        
        return pd.DataFrame({
            "date": dates,
            "production_efficiency": np.random.normal(88, 3, len(dates)),
            "system_performance": np.random.normal(95, 2, len(dates))
        })
    
    def _get_forecast_data(self):
        """Get forecast data."""
        # Historical data
        hist_dates = pd.date_range(start="2023-07-01", end="2024-01-01", freq="M")
        hist_values = np.random.normal(95, 2, len(hist_dates))
        
        # Forecast data
        forecast_dates = pd.date_range(start="2024-02-01", end="2024-07-01", freq="M")
        forecast_values = np.random.normal(96.5, 1.5, len(forecast_dates))
        
        # Combine
        all_dates = list(hist_dates) + list(forecast_dates)
        actual_values = list(hist_values) + [None] * len(forecast_dates)
        forecast_values_full = [None] * len(hist_dates) + list(forecast_values)
        
        return pd.DataFrame({
            "date": all_dates,
            "actual": actual_values,
            "forecast": forecast_values_full
        })

def render_executive_dashboard_page():
    """Render the Executive Dashboard page."""
    page = ExecutiveDashboardPage()
    page.render()