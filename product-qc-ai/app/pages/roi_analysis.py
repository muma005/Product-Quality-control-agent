"""ROI Analysis page for return on investment tracking and financial analysis."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional
import calendar

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

class ROIAnalysisPage:
    """ROI Analysis page class."""
    
    def __init__(self):
        """Initialize ROI Analysis page."""
        self.financial_data = None
        self.roi_calculations = {}
    
    def render(self):
        """Render the ROI Analysis page."""
        render_header_section(
            title="ROI Analysis",
            description="Return on Investment tracking, cost-benefit analysis, and financial performance metrics",
            icon="💰"
        )
        
        # Time range selector
        start_date, end_date = render_time_range_selector("roi")
        
        # Main ROI sections
        self._render_roi_overview()
        self._render_cost_benefit_analysis()
        self._render_investment_tracking()
        self._render_savings_analysis()
        self._render_payback_analysis()
        self._render_financial_projections()
        self._render_roi_breakdown()
        self._render_recommendations()
    
    def _render_roi_overview(self):
        """Render ROI overview section."""
        st.subheader("📊 ROI Overview")
        
        # Key ROI metrics
        roi_data = self._get_roi_overview_data()
        
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            total_roi = roi_data.get("total_roi_percentage", 0)
            create_metric_card(
                title="Total ROI",
                value=f"{total_roi:.1f}%",
                delta=f"+{roi_data.get('roi_change', 0):.1f}%",
                delta_color="normal" if total_roi > 0 else "inverse",
                help_text="Overall return on investment"
            )
        
        with col2:
            total_investment = roi_data.get("total_investment", 0)
            create_metric_card(
                title="Total Investment",
                value=f"${format_large_number(total_investment)}",
                help_text="Total capital invested in quality initiatives"
            )
        
        with col3:
            total_savings = roi_data.get("total_savings", 0)
            create_metric_card(
                title="Total Savings",
                value=f"${format_large_number(total_savings)}",
                delta=f"+${format_large_number(roi_data.get('savings_increase', 0))}",
                delta_color="normal",
                help_text="Total cost savings achieved"
            )
        
        with col4:
            payback_period = roi_data.get("payback_months", 0)
            create_metric_card(
                title="Payback Period",
                value=f"{payback_period:.1f} mo",
                delta_color="normal" if payback_period < 24 else "inverse",
                help_text="Time to recover investment"
            )
        
        with col5:
            npv = roi_data.get("net_present_value", 0)
            create_metric_card(
                title="Net Present Value",
                value=f"${format_large_number(npv)}",
                delta_color="normal" if npv > 0 else "inverse",
                help_text="NPV of quality investments"
            )
        
        with col6:
            irr = roi_data.get("internal_rate_of_return", 0)
            create_metric_card(
                title="Internal Rate of Return",
                value=f"{irr:.1f}%",
                delta_color="normal" if irr > 10 else "inverse",
                help_text="Internal rate of return"
            )
        
        # ROI trend chart
        roi_trend = self._get_roi_trend_data()
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=roi_trend['month'],
            y=roi_trend['cumulative_roi'],
            mode='lines+markers',
            name='Cumulative ROI %',
            line=dict(color=COLOR_SCHEMES["primary"], width=3),
            marker=dict(size=8)
        ))
        
        fig.add_hline(y=0, line_dash="dash", line_color="gray", 
                     annotation_text="Break-even")
        
        fig.update_layout(
            title='ROI Trend Over Time',
            xaxis_title='Month',
            yaxis_title='ROI (%)',
            height=400,
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("---")
    
    def _render_cost_benefit_analysis(self):
        """Render cost-benefit analysis section."""
        st.subheader("⚖️ Cost-Benefit Analysis")
        
        # Create tabs for different cost-benefit analyses
        tab1, tab2, tab3, tab4 = st.tabs([
            "💸 Cost Breakdown", 
            "💰 Benefits Analysis", 
            "📊 Comparison Matrix",
            "🎯 Project ROI"
        ])
        
        with tab1:
            self._render_cost_breakdown()
        
        with tab2:
            self._render_benefits_analysis()
        
        with tab3:
            self._render_comparison_matrix()
        
        with tab4:
            self._render_project_roi()
        
        st.markdown("---")
    
    def _render_cost_breakdown(self):
        """Render cost breakdown analysis."""
        st.markdown("### 💸 Cost Breakdown")
        
        cost_data = self._get_cost_breakdown_data()
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Cost categories pie chart
            fig = px.pie(
                values=cost_data['amount'],
                names=cost_data['category'],
                title='Cost Distribution by Category',
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Cost breakdown table
            st.markdown("**Detailed Cost Breakdown:**")
            
            cost_summary = cost_data.copy()
            cost_summary['percentage'] = (cost_summary['amount'] / cost_summary['amount'].sum() * 100).round(1)
            cost_summary['amount_formatted'] = cost_summary['amount'].apply(lambda x: f"${format_large_number(x)}")
            
            display_df = cost_summary[['category', 'amount_formatted', 'percentage']].copy()
            display_df.columns = ['Category', 'Amount', 'Percentage (%)']
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            # Total cost
            total_cost = cost_data['amount'].sum()
            st.markdown(f"**Total Cost: ${format_large_number(total_cost)}**")
    
    def _render_benefits_analysis(self):
        """Render benefits analysis."""
        st.markdown("### 💰 Benefits Analysis")
        
        benefits_data = self._get_benefits_data()
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Benefits waterfall chart
            categories = benefits_data['category'].tolist()
            values = benefits_data['annual_benefit'].tolist()
            
            fig = go.Figure(go.Waterfall(
                name="Benefits",
                orientation="v",
                measure=["relative"] * len(categories) + ["total"],
                x=categories + ["Total Benefits"],
                textposition="outside",
                text=[f"${format_large_number(v)}" for v in values] + [f"${format_large_number(sum(values))}"],
                y=values + [sum(values)],
                connector={"line": {"color": "rgb(63, 63, 63)"}},
            ))
            
            fig.update_layout(
                title="Annual Benefits Waterfall",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Benefits details table
            st.markdown("**Benefits Breakdown:**")
            
            benefits_summary = benefits_data.copy()
            benefits_summary['benefit_formatted'] = benefits_summary['annual_benefit'].apply(
                lambda x: f"${format_large_number(x)}"
            )
            benefits_summary['monthly'] = (benefits_summary['annual_benefit'] / 12).apply(
                lambda x: f"${format_large_number(x)}"
            )
            
            display_df = benefits_summary[['category', 'benefit_formatted', 'monthly', 'confidence']].copy()
            display_df.columns = ['Benefit Category', 'Annual', 'Monthly', 'Confidence']
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    def _render_comparison_matrix(self):
        """Render cost-benefit comparison matrix."""
        st.markdown("### 📊 Cost vs Benefits Comparison")
        
        comparison_data = self._get_comparison_data()
        
        # Create scatter plot comparing costs vs benefits by project
        fig = px.scatter(
            comparison_data,
            x='cost',
            y='benefit',
            size='roi_absolute',
            color='payback_months',
            hover_name='project',
            title='Cost vs Benefit Analysis by Project',
            labels={
                'cost': 'Investment Cost ($)',
                'benefit': 'Annual Benefit ($)',
                'payback_months': 'Payback (Months)'
            },
            color_continuous_scale='RdYlGn_r'
        )
        
        # Add break-even line (where benefit = cost)
        max_val = max(comparison_data['cost'].max(), comparison_data['benefit'].max())
        fig.add_trace(go.Scatter(
            x=[0, max_val],
            y=[0, max_val],
            mode='lines',
            name='Break-even Line',
            line=dict(color='red', dash='dash')
        ))
        
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        
        # Summary metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            positive_roi_count = (comparison_data['roi_percentage'] > 0).sum()
            st.metric("Projects with Positive ROI", f"{positive_roi_count}/{len(comparison_data)}")
        
        with col2:
            avg_payback = comparison_data['payback_months'].mean()
            st.metric("Average Payback Period", f"{avg_payback:.1f} months")
        
        with col3:
            total_net_benefit = (comparison_data['benefit'] - comparison_data['cost']).sum()
            st.metric("Total Net Benefit", f"${format_large_number(total_net_benefit)}")
    
    def _render_project_roi(self):
        """Render individual project ROI analysis."""
        st.markdown("### 🎯 Project ROI Analysis")
        
        project_data = self._get_project_roi_data()
        
        # Project selector
        selected_project = st.selectbox(
            "Select Project for Detailed Analysis",
            options=project_data['project_name'].tolist(),
            index=0
        )
        
        project_details = project_data[project_data['project_name'] == selected_project].iloc[0]
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"**{selected_project} - Financial Summary**")
            
            # Project metrics
            metrics = [
                ("Initial Investment", f"${format_large_number(project_details['initial_cost'])}"),
                ("Annual Savings", f"${format_large_number(project_details['annual_savings'])}"),
                ("ROI Percentage", f"{project_details['roi_percentage']:.1f}%"),
                ("Payback Period", f"{project_details['payback_months']:.1f} months"),
                ("NPV (3 years)", f"${format_large_number(project_details['npv_3_years'])}"),
                ("Risk Level", project_details['risk_level'])
            ]
            
            for metric, value in metrics:
                st.write(f"• **{metric}**: {value}")
        
        with col2:
            # Cash flow projection
            months = list(range(1, 37))  # 3 years
            cumulative_cashflow = []
            
            initial_cost = project_details['initial_cost']
            monthly_savings = project_details['annual_savings'] / 12
            
            running_total = -initial_cost
            for month in months:
                running_total += monthly_savings
                cumulative_cashflow.append(running_total)
            
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=months,
                y=cumulative_cashflow,
                mode='lines+markers',
                name='Cumulative Cash Flow',
                line=dict(color=COLOR_SCHEMES["primary"], width=3)
            ))
            
            fig.add_hline(y=0, line_dash="dash", line_color="red", 
                         annotation_text="Break-even")
            
            fig.update_layout(
                title=f'{selected_project} - Cash Flow Projection',
                xaxis_title='Month',
                yaxis_title='Cumulative Cash Flow ($)',
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    def _render_investment_tracking(self):
        """Render investment tracking section."""
        st.subheader("📈 Investment Tracking")
        
        investment_data = self._get_investment_tracking_data()
        
        # Investment timeline
        fig = go.Figure()
        
        # Planned investments
        fig.add_trace(go.Scatter(
            x=investment_data['date'],
            y=investment_data['planned_investment'],
            mode='lines+markers',
            name='Planned Investment',
            line=dict(color=COLOR_SCHEMES["secondary"], dash='dash'),
            marker=dict(symbol='circle')
        ))
        
        # Actual investments
        fig.add_trace(go.Scatter(
            x=investment_data['date'],
            y=investment_data['actual_investment'],
            mode='lines+markers',
            name='Actual Investment',
            line=dict(color=COLOR_SCHEMES["primary"], width=3),
            marker=dict(symbol='square')
        ))
        
        # Cumulative planned
        fig.add_trace(go.Scatter(
            x=investment_data['date'],
            y=investment_data['cumulative_planned'],
            mode='lines',
            name='Cumulative Planned',
            line=dict(color=COLOR_SCHEMES["secondary"], dash='dot'),
            yaxis='y2'
        ))
        
        # Cumulative actual
        fig.add_trace(go.Scatter(
            x=investment_data['date'],
            y=investment_data['cumulative_actual'],
            mode='lines',
            name='Cumulative Actual',
            line=dict(color=COLOR_SCHEMES["primary"], dash='dot'),
            yaxis='y2'
        ))
        
        fig.update_layout(
            title='Investment Tracking - Planned vs Actual',
            xaxis_title='Date',
            yaxis=dict(title='Monthly Investment ($)', side='left'),
            yaxis2=dict(title='Cumulative Investment ($)', side='right', overlaying='y'),
            height=500,
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Investment summary
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_planned = investment_data['cumulative_planned'].iloc[-1]
            st.metric("Total Planned", f"${format_large_number(total_planned)}")
        
        with col2:
            total_actual = investment_data['cumulative_actual'].iloc[-1]
            variance = total_actual - total_planned
            st.metric(
                "Total Actual", 
                f"${format_large_number(total_actual)}",
                delta=f"${format_large_number(variance)}"
            )
        
        with col3:
            variance_pct = (variance / total_planned) * 100 if total_planned > 0 else 0
            st.metric("Budget Variance", f"{variance_pct:+.1f}%")
        
        with col4:
            efficiency = (total_planned / total_actual) * 100 if total_actual > 0 else 0
            st.metric("Investment Efficiency", f"{efficiency:.1f}%")
        
        st.markdown("---")
    
    def _render_savings_analysis(self):
        """Render savings analysis section."""
        st.subheader("💸 Savings Analysis")
        
        savings_data = self._get_savings_analysis_data()
        
        # Savings categories analysis
        col1, col2 = st.columns(2)
        
        with col1:
            # Savings by category
            fig = px.bar(
                savings_data,
                x='category',
                y='monthly_savings',
                title='Monthly Savings by Category',
                color='category',
                color_discrete_sequence=px.colors.qualitative.Set2
            )
            fig.update_layout(height=400, showlegend=False)
            fig.update_xaxis(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Cumulative savings trend
            cumulative_savings = savings_data.groupby('month')['monthly_savings'].sum().cumsum()
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=cumulative_savings.index,
                y=cumulative_savings.values,
                mode='lines+markers',
                name='Cumulative Savings',
                line=dict(color=COLOR_SCHEMES["success"], width=3),
                fill='tonexty'
            ))
            
            fig.update_layout(
                title='Cumulative Savings Over Time',
                xaxis_title='Month',
                yaxis_title='Cumulative Savings ($)',
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Detailed savings breakdown
        st.markdown("**Savings Breakdown by Category:**")
        
        savings_summary = savings_data.groupby('category').agg({
            'monthly_savings': ['sum', 'mean'],
            'confidence_level': 'mean'
        }).round(2)
        
        savings_summary.columns = ['Total Monthly', 'Average Monthly', 'Confidence %']
        savings_summary['Annual Projection'] = savings_summary['Total Monthly'] * 12
        
        # Format currency columns
        currency_cols = ['Total Monthly', 'Average Monthly', 'Annual Projection']
        for col in currency_cols:
            savings_summary[col] = savings_summary[col].apply(lambda x: f"${format_large_number(x)}")
        
        st.dataframe(savings_summary, use_container_width=True)
        
        st.markdown("---")
    
    def _render_payback_analysis(self):
        """Render payback analysis section."""
        st.subheader("⏱️ Payback Analysis")
        
        payback_data = self._get_payback_analysis_data()
        
        # Payback period distribution
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.histogram(
                payback_data,
                x='payback_months',
                nbins=20,
                title='Payback Period Distribution',
                color_discrete_sequence=[COLOR_SCHEMES["primary"]]
            )
            fig.add_vline(x=12, line_dash="dash", line_color="red", 
                         annotation_text="1 Year Target")
            fig.add_vline(x=24, line_dash="dash", line_color="orange", 
                         annotation_text="2 Year Limit")
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Payback vs ROI scatter
            fig = px.scatter(
                payback_data,
                x='payback_months',
                y='roi_percentage',
                size='investment_amount',
                color='project_category',
                title='Payback Period vs ROI',
                hover_name='project'
            )
            fig.add_hline(y=0, line_dash="dash", line_color="gray")
            fig.add_vline(x=24, line_dash="dash", line_color="red")
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Payback summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_payback = payback_data['payback_months'].mean()
            st.metric("Average Payback", f"{avg_payback:.1f} months")
        
        with col2:
            median_payback = payback_data['payback_months'].median()
            st.metric("Median Payback", f"{median_payback:.1f} months")
        
        with col3:
            quick_payback = (payback_data['payback_months'] <= 12).sum()
            total_projects = len(payback_data)
            st.metric("Quick Payback (<1yr)", f"{quick_payback}/{total_projects}")
        
        with col4:
            acceptable_payback = (payback_data['payback_months'] <= 24).sum()
            st.metric("Acceptable Payback (<2yr)", f"{acceptable_payback}/{total_projects}")
        
        st.markdown("---")
    
    def _render_financial_projections(self):
        """Render financial projections section."""
        st.subheader("🔮 Financial Projections")
        
        projection_years = st.slider("Projection Period (Years)", 1, 10, 5)
        
        projections = self._get_financial_projections(projection_years)
        
        # Create projection charts
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=['Cumulative ROI', 'Annual Cash Flow', 'Investment Recovery', 'Risk Analysis'],
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": True}, {"secondary_y": False}]]
        )
        
        years = list(range(1, projection_years + 1))
        
        # Cumulative ROI
        fig.add_trace(
            go.Scatter(x=years, y=projections['cumulative_roi'], 
                      mode='lines+markers', name='Cumulative ROI %'),
            row=1, col=1
        )
        
        # Annual Cash Flow
        fig.add_trace(
            go.Bar(x=years, y=projections['annual_cashflow'], name='Annual Cash Flow'),
            row=1, col=2
        )
        
        # Investment Recovery
        fig.add_trace(
            go.Scatter(x=years, y=projections['investment_recovery'], 
                      mode='lines', name='Investment Recovery %'),
            row=2, col=1
        )
        fig.add_trace(
            go.Scatter(x=years, y=projections['savings_projection'], 
                      mode='lines', name='Cumulative Savings', line=dict(dash='dash')),
            row=2, col=1, secondary_y=True
        )
        
        # Risk Analysis
        fig.add_trace(
            go.Scatter(x=years, y=projections['risk_adjusted_roi'], 
                      mode='lines+markers', name='Risk-Adjusted ROI',
                      line=dict(color='red')),
            row=2, col=2
        )
        
        fig.update_layout(height=600, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        
        # Projection summary
        st.markdown("**Projection Summary:**")
        
        summary_data = {
            'Year': years,
            'Cumulative ROI (%)': [f"{x:.1f}%" for x in projections['cumulative_roi']],
            'Annual Cash Flow': [f"${format_large_number(x)}" for x in projections['annual_cashflow']],
            'Investment Recovery (%)': [f"{x:.1f}%" for x in projections['investment_recovery']],
            'Risk Factor': [f"{x:.2f}" for x in projections['risk_factor']]
        }
        
        summary_df = pd.DataFrame(summary_data)
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
        
        st.markdown("---")
    
    def _render_roi_breakdown(self):
        """Render detailed ROI breakdown section."""
        st.subheader("🔍 ROI Breakdown")
        
        # ROI components analysis
        breakdown_data = self._get_roi_breakdown_data()
        
        # Sankey diagram for ROI flow
        fig = go.Figure(data=[go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                line=dict(color="black", width=0.5),
                label=breakdown_data['labels'],
                color="blue"
            ),
            link=dict(
                source=breakdown_data['source'],
                target=breakdown_data['target'],
                value=breakdown_data['values']
            )
        )])
        
        fig.update_layout(
            title_text="ROI Flow Analysis",
            font_size=10,
            height=500
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # ROI drivers table
        st.markdown("**Key ROI Drivers:**")
        
        drivers_data = self._get_roi_drivers_data()
        
        st.dataframe(
            drivers_data.style.format({
                'impact_amount': '${:,.0f}',
                'roi_contribution': '{:.1f}%',
                'confidence': '{:.0f}%'
            }),
            use_container_width=True
        )
        
        st.markdown("---")
    
    def _render_recommendations(self):
        """Render ROI recommendations section."""
        st.subheader("💡 ROI Recommendations")
        
        recommendations = [
            {
                "priority": "High",
                "title": "Accelerate Equipment Modernization",
                "description": "Current equipment upgrades show 156% ROI with 8-month payback. Recommend increasing investment by 40%.",
                "financial_impact": "+$2.3M annual savings",
                "implementation": "Q2 2024",
                "risk": "Low"
            },
            {
                "priority": "High", 
                "title": "Expand Process Automation",
                "description": "Automation projects demonstrate consistent 120%+ ROI. Scale successful pilots to remaining production lines.",
                "financial_impact": "+$1.8M annual savings",
                "implementation": "Q3 2024",
                "risk": "Medium"
            },
            {
                "priority": "Medium",
                "title": "Optimize Quality Control Intervals",
                "description": "Analysis shows 15% reduction in inspection frequency maintains quality while reducing costs by 22%.",
                "financial_impact": "+$650K annual savings",
                "implementation": "Q1 2024",
                "risk": "Low"
            },
            {
                "priority": "Medium",
                "title": "Implement Predictive Maintenance",
                "description": "Predictive maintenance shows 89% ROI through reduced downtime and maintenance costs.",
                "financial_impact": "+$1.1M annual savings", 
                "implementation": "Q4 2024",
                "risk": "Medium"
            },
            {
                "priority": "Low",
                "title": "Training Program Enhancement",
                "description": "Enhanced training correlates with 12% quality improvement but requires careful cost management.",
                "financial_impact": "+$450K annual savings",
                "implementation": "Ongoing",
                "risk": "Low"
            }
        ]
        
        priority_colors = {"High": "🔴", "Medium": "🟡", "Low": "🟢"}
        risk_colors = {"Low": "🟢", "Medium": "🟡", "High": "🔴"}
        
        for i, rec in enumerate(recommendations):
            with st.expander(f"{priority_colors[rec['priority']]} {rec['title']}", expanded=i<2):
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.markdown(f"**Description:** {rec['description']}")
                    st.markdown(f"**Financial Impact:** {rec['financial_impact']}")
                    st.markdown(f"**Implementation Timeline:** {rec['implementation']}")
                
                with col2:
                    st.markdown(f"**Priority:** {priority_colors[rec['priority']]} {rec['priority']}")
                    st.markdown(f"**Risk Level:** {risk_colors[rec['risk']]} {rec['risk']}")
        
        # Export options
        st.markdown("---")
        roi_summary = self._get_roi_overview_data()
        render_export_options(
            data=roi_summary,
            filename_prefix="roi_analysis"
        )
    
    # Helper methods for data generation
    def _get_roi_overview_data(self) -> Dict[str, Any]:
        """Get ROI overview data."""
        return {
            "total_roi_percentage": 124.5,
            "roi_change": 15.3,
            "total_investment": 8500000,
            "total_savings": 10582000,
            "savings_increase": 1250000,
            "payback_months": 13.2,
            "net_present_value": 15750000,
            "internal_rate_of_return": 28.7
        }
    
    def _get_roi_trend_data(self) -> pd.DataFrame:
        """Generate ROI trend data."""
        months = pd.date_range('2023-01-01', periods=12, freq='M')
        roi_values = [10, 25, 45, 62, 78, 95, 108, 118, 124, 127, 129, 124.5]
        
        return pd.DataFrame({
            'month': months,
            'cumulative_roi': roi_values
        })
    
    def _get_cost_breakdown_data(self) -> pd.DataFrame:
        """Generate cost breakdown data."""
        return pd.DataFrame({
            'category': [
                'Equipment & Technology',
                'Process Improvement',
                'Training & Development', 
                'Quality Systems',
                'Maintenance & Upgrades',
                'Consulting & Services'
            ],
            'amount': [3200000, 1800000, 650000, 1200000, 950000, 700000]
        })
    
    def _get_benefits_data(self) -> pd.DataFrame:
        """Generate benefits data."""
        return pd.DataFrame({
            'category': [
                'Reduced Defect Costs',
                'Improved Efficiency',
                'Lower Maintenance',
                'Reduced Waste',
                'Compliance Savings',
                'Customer Retention'
            ],
            'annual_benefit': [4200000, 2800000, 1500000, 1200000, 800000, 1082000],
            'confidence': [95, 90, 85, 90, 100, 75]
        })
    
    def _get_comparison_data(self) -> pd.DataFrame:
        """Generate cost vs benefit comparison data."""
        np.random.seed(42)
        n_projects = 15
        
        projects = [f"Project {i+1}" for i in range(n_projects)]
        costs = np.random.lognormal(13, 0.8, n_projects)  # Log-normal distribution for costs
        benefits = costs * np.random.normal(1.5, 0.5, n_projects)  # Benefits correlated with costs
        
        roi_absolute = benefits - costs
        roi_percentage = (roi_absolute / costs) * 100
        payback_months = (costs / (benefits / 12))
        
        return pd.DataFrame({
            'project': projects,
            'cost': costs,
            'benefit': benefits,
            'roi_absolute': roi_absolute,
            'roi_percentage': roi_percentage,
            'payback_months': payback_months
        })
    
    def _get_project_roi_data(self) -> pd.DataFrame:
        """Generate individual project ROI data."""
        return pd.DataFrame({
            'project_name': [
                'Automated Quality Inspection',
                'Process Optimization System',
                'Predictive Maintenance',
                'Employee Training Program',
                'Equipment Modernization'
            ],
            'initial_cost': [2500000, 1800000, 1200000, 450000, 3200000],
            'annual_savings': [3200000, 2100000, 1400000, 540000, 4800000],
            'roi_percentage': [128, 117, 117, 120, 150],
            'payback_months': [9.4, 10.3, 10.3, 10.0, 8.0],
            'npv_3_years': [6890000, 4420000, 2980000, 1170000, 11200000],
            'risk_level': ['Low', 'Medium', 'Medium', 'Low', 'Low']
        })
    
    def _get_investment_tracking_data(self) -> pd.DataFrame:
        """Generate investment tracking data."""
        dates = pd.date_range('2023-01-01', periods=12, freq='M')
        
        # Generate realistic investment patterns
        planned = [500000, 750000, 600000, 800000, 1200000, 900000, 
                  700000, 600000, 800000, 950000, 650000, 450000]
        actual = [520000, 680000, 720000, 750000, 1350000, 880000,
                 750000, 580000, 820000, 980000, 620000, 470000]
        
        cumulative_planned = np.cumsum(planned)
        cumulative_actual = np.cumsum(actual)
        
        return pd.DataFrame({
            'date': dates,
            'planned_investment': planned,
            'actual_investment': actual,
            'cumulative_planned': cumulative_planned,
            'cumulative_actual': cumulative_actual
        })
    
    def _get_savings_analysis_data(self) -> pd.DataFrame:
        """Generate savings analysis data."""
        months = list(range(1, 13))
        categories = ['Defect Reduction', 'Efficiency Gains', 'Waste Reduction', 'Maintenance Savings']
        
        data = []
        for month in months:
            for category in categories:
                base_savings = {'Defect Reduction': 250000, 'Efficiency Gains': 180000, 
                              'Waste Reduction': 120000, 'Maintenance Savings': 90000}
                
                # Add some monthly variation
                monthly_variation = 1 + (month - 6) * 0.02 + np.random.normal(0, 0.1)
                savings = base_savings[category] * monthly_variation
                
                data.append({
                    'month': month,
                    'category': category,
                    'monthly_savings': max(0, savings),
                    'confidence_level': np.random.uniform(80, 95)
                })
        
        return pd.DataFrame(data)
    
    def _get_payback_analysis_data(self) -> pd.DataFrame:
        """Generate payback analysis data."""
        np.random.seed(42)
        n_projects = 25
        
        return pd.DataFrame({
            'project': [f"Initiative {i+1}" for i in range(n_projects)],
            'payback_months': np.random.gamma(2, 6, n_projects),
            'roi_percentage': np.random.normal(85, 40, n_projects),
            'investment_amount': np.random.lognormal(12, 1, n_projects),
            'project_category': np.random.choice(['Technology', 'Process', 'Training', 'Equipment'], n_projects)
        })
    
    def _get_financial_projections(self, years: int) -> Dict[str, List[float]]:
        """Generate financial projections."""
        base_roi = 25
        base_cashflow = 2000000
        base_risk = 1.0
        
        projections = {
            'cumulative_roi': [],
            'annual_cashflow': [],
            'investment_recovery': [],
            'savings_projection': [],
            'risk_adjusted_roi': [],
            'risk_factor': []
        }
        
        for year in range(1, years + 1):
            # ROI grows but at decreasing rate
            cumulative_roi = base_roi * year * (1 + 0.15) ** year * (0.95 ** year)
            
            # Cash flow grows with some volatility
            annual_cashflow = base_cashflow * (1.1 ** year) * (1 + np.sin(year) * 0.1)
            
            # Investment recovery
            investment_recovery = min(100, year * 35)
            
            # Cumulative savings
            savings_projection = annual_cashflow * year * 0.8
            
            # Risk increases over time
            risk_factor = base_risk * (1.05 ** year)
            risk_adjusted_roi = cumulative_roi / risk_factor
            
            projections['cumulative_roi'].append(cumulative_roi)
            projections['annual_cashflow'].append(annual_cashflow)
            projections['investment_recovery'].append(investment_recovery)
            projections['savings_projection'].append(savings_projection)
            projections['risk_adjusted_roi'].append(risk_adjusted_roi)
            projections['risk_factor'].append(risk_factor)
        
        return projections
    
    def _get_roi_breakdown_data(self) -> Dict[str, List]:
        """Generate ROI breakdown data for Sankey diagram."""
        return {
            'labels': [
                'Total Investment', 'Technology', 'Process', 'Training',
                'Defect Reduction', 'Efficiency Gains', 'Waste Reduction',
                'Total ROI'
            ],
            'source': [0, 0, 0, 1, 2, 3, 4, 5, 6],
            'target': [1, 2, 3, 4, 5, 6, 7, 7, 7],
            'values': [4000000, 2500000, 1000000, 3200000, 2800000, 1200000, 4200000, 2800000, 1200000]
        }
    
    def _get_roi_drivers_data(self) -> pd.DataFrame:
        """Generate ROI drivers data."""
        return pd.DataFrame({
            'driver': [
                'Automated Quality Control',
                'Process Optimization',
                'Predictive Analytics',
                'Equipment Efficiency',
                'Waste Reduction',
                'Training Programs'
            ],
            'impact_amount': [2800000, 2100000, 1500000, 1800000, 900000, 650000],
            'roi_contribution': [28.5, 21.3, 15.2, 18.3, 9.1, 6.6],
            'confidence': [95, 90, 85, 92, 88, 75],
            'trend': ['↑', '↑', '→', '↑', '↑', '→']
        })

def render_roi_analysis_page():
    """Render the ROI Analysis page."""
    page = ROIAnalysisPage()
    page.render()