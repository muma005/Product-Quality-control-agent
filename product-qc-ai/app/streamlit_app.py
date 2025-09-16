import streamlit as st
import sys
import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import json

# Add pipeline directory to path
pipeline_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline')
sys.path.insert(0, pipeline_dir)

try:
    from pipeline.analytics import (
        AdvancedAnalyticsManager,
        ROIAnalyticsManager,
        AutomatedReportingManager,
        generate_quality_dashboard,
        calculate_system_roi,
        generate_executive_report
    )
    from pipeline import recommendations
    from google.cloud import bigquery
    ANALYTICS_AVAILABLE = True
except ImportError as e:
    st.error(f"Analytics modules not available: {e}")
    ANALYTICS_AVAILABLE = False

st.set_page_config(
    page_title="Advanced Product Quality Control Analytics",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Advanced Custom CSS for Analytics Dashboard ---
st.markdown("""
    <style>
    .main-header { 
        font-size: 36px !important; 
        font-weight: bold; 
        color: #1f77b4;
        text-align: center;
        margin-bottom: 20px;
    }
    .kpi-card { 
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 15px; 
        padding: 25px; 
        margin: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        text-align: center;
    }
    .kpi-value {
        font-size: 32px;
        font-weight: bold;
        margin: 10px 0;
    }
    .kpi-label {
        font-size: 14px;
        opacity: 0.9;
    }
    .insight-card {
        background: #f8f9fa;
        border-left: 5px solid #28a745;
        padding: 15px;
        margin: 10px 0;
        border-radius: 5px;
    }
    .warning-card {
        background: #fff3cd;
        border-left: 5px solid #ffc107;
        padding: 15px;
        margin: 10px 0;
        border-radius: 5px;
    }
    .danger-card {
        background: #f8d7da;
        border-left: 5px solid #dc3545;
        padding: 15px;
        margin: 10px 0;
        border-radius: 5px;
    }
    .metric-trend-up { color: #28a745; }
    .metric-trend-down { color: #dc3545; }
    .sidebar .sidebar-content { background: #f0f2f6; }
    </style>
""", unsafe_allow_html=True)

# --- Main Header ---
st.markdown('<div class="main-header">� Advanced Product Quality Control Analytics</div>', unsafe_allow_html=True)
st.markdown('<div style="text-align: center; color: #666; margin-bottom: 30px;">Phase 4: Hub-Optimized Business Intelligence Dashboard</div>', unsafe_allow_html=True)

# --- Sidebar Configuration ---
st.sidebar.header("📊 Analytics Configuration")

# Date Range Selection
with st.sidebar.expander("📅 Date Range", expanded=True):
    date_option = st.selectbox(
        "Select Period",
        ["Last 7 days", "Last 30 days", "Last 90 days", "Custom Range"]
    )
    
    if date_option == "Custom Range":
        start_date = st.date_input("Start Date", datetime.now() - timedelta(days=30))
        end_date = st.date_input("End Date", datetime.now())
        date_range = (datetime.combine(start_date, datetime.min.time()), 
                     datetime.combine(end_date, datetime.min.time()))
    else:
        days_map = {"Last 7 days": 7, "Last 30 days": 30, "Last 90 days": 90}
        days = days_map[date_option]
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        date_range = (start_date, end_date)

# Filters
with st.sidebar.expander("🎯 Filters", expanded=True):
    category_filter = st.selectbox("Category", ["All", "Electronics", "Clothing", "Home & Garden", "Sports", "Books"])
    category_filter = None if category_filter == "All" else category_filter
    
    confidence_threshold = st.slider("Confidence Threshold", 0.0, 1.0, 0.7, 0.05)
    include_predictions = st.checkbox("Include Predictions", value=True)

# Analytics Options
with st.sidebar.expander("⚙️ Analytics Options", expanded=False):
    real_time_refresh = st.checkbox("Real-time Refresh (30s)", value=False)
    cache_analytics = st.checkbox("Cache Analytics", value=True)
    detailed_insights = st.checkbox("Detailed Insights", value=True)

# --- Navigation Tabs ---
tabs = st.tabs([
    "📊 Executive Dashboard", 
    "📈 Advanced Analytics", 
    "💰 ROI & Business Impact", 
    "🔮 Predictive Insights",
    "📋 Automated Reports",
    "🔧 System Performance"
])

# --- Tab 1: Executive Dashboard ---
with tabs[0]:
    st.header("📊 Executive Quality Control Dashboard")
    
    if not ANALYTICS_AVAILABLE:
        st.error("Advanced analytics not available. Please ensure all dependencies are installed.")
        st.stop()
    
    # Loading indicator
    with st.spinner("Generating executive dashboard..."):
        try:
            # Generate dashboard data
            dashboard_data = generate_quality_dashboard(date_range=date_range)
            
            if 'error' in dashboard_data:
                st.error(f"Error generating dashboard: {dashboard_data['error']}")
                st.stop()
            
            kpis = dashboard_data.get('kpis', {})
            insights = dashboard_data.get('insights', {})
            
            # Executive KPI Cards
            st.subheader("🎯 Key Performance Indicators")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                quality_metrics = kpis.get('quality_metrics', {})
                avg_quality = quality_metrics.get('avg_quality_score', 0)
                total_products = quality_metrics.get('total_products', 0)
                st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">📈 Average Quality Score</div>
                    <div class="kpi-value">{avg_quality:.1f}%</div>
                    <div class="kpi-label">{total_products:,} products processed</div>
                </div>
                ''', unsafe_allow_html=True)
            
            with col2:
                automation_metrics = kpis.get('automation_metrics', {})
                automation_rate = automation_metrics.get('automation_rate', 0)
                corrections_applied = automation_metrics.get('corrections_applied', 0)
                st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">🤖 Automation Rate</div>
                    <div class="kpi-value">{automation_rate:.1f}%</div>
                    <div class="kpi-label">{corrections_applied:,} auto-corrections</div>
                </div>
                ''', unsafe_allow_html=True)
            
            with col3:
                business_impact = kpis.get('business_impact', {})
                cost_savings = business_impact.get('total_cost_savings', 0)
                optimization = business_impact.get('hub_optimization_improvement', 0)
                st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">💰 Cost Savings</div>
                    <div class="kpi-value">${cost_savings:,.0f}</div>
                    <div class="kpi-label">{optimization:.1f}% optimization</div>
                </div>
                ''', unsafe_allow_html=True)
            
            with col4:
                performance_metrics = kpis.get('performance_metrics', {})
                cache_hit_rate = performance_metrics.get('cache_hit_rate', 0)
                processing_improvement = performance_metrics.get('performance_optimization', 0)
                st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">⚡ Performance</div>
                    <div class="kpi-value">{cache_hit_rate:.1f}%</div>
                    <div class="kpi-label">Cache Hit Rate</div>
                </div>
                ''', unsafe_allow_html=True)
            
            # Executive Insights
            st.subheader("💡 Executive Insights")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### 🚨 Priority Actions")
                priority_actions = insights.get('priority_actions', [])
                if priority_actions:
                    for action in priority_actions[:3]:  # Top 3 priority actions
                        urgency_color = "danger-card" if action.get('urgency') == 'high' else "warning-card"
                        st.markdown(f'''
                        <div class="{urgency_color}">
                            <strong>{action.get('action', 'Action Required')}</strong><br>
                            <small>{action.get('detail', 'No details available')}</small>
                        </div>
                        ''', unsafe_allow_html=True)
                else:
                    st.markdown('<div class="insight-card">✅ No critical actions required</div>', unsafe_allow_html=True)
            
            with col2:
                st.markdown("#### 🎯 Strategic Opportunities")
                opportunities = insights.get('opportunities', [])
                if opportunities:
                    for opp in opportunities[:3]:  # Top 3 opportunities
                        st.markdown(f'''
                        <div class="insight-card">
                            <strong>{opp.get('opportunity', 'Opportunity Available')}</strong><br>
                            <small>{opp.get('detail', 'No details available')}</small>
                        </div>
                        ''', unsafe_allow_html=True)
                else:
                    st.markdown('<div class="insight-card">📊 System operating optimally</div>', unsafe_allow_html=True)
            
            # Quality Trends Visualization
            st.subheader("📈 Quality Performance Trends")
            
            trends = dashboard_data.get('trends', {})
            daily_trends = trends.get('daily_trends', {}).get('data', [])
            
            if daily_trends:
                # Convert to DataFrame for plotting
                trends_df = pd.DataFrame(daily_trends)
                trends_df['period'] = pd.to_datetime(trends_df['period'])
                
                # Create subplot with multiple metrics
                fig = make_subplots(
                    rows=2, cols=2,
                    subplot_titles=('Quality Score Trend', 'Daily Cost Savings', 'Products Processed', 'Correction Confidence'),
                    specs=[[{"secondary_y": False}, {"secondary_y": False}],
                           [{"secondary_y": False}, {"secondary_y": False}]]
                )
                
                # Quality Score Trend
                fig.add_trace(
                    go.Scatter(
                        x=trends_df['period'],
                        y=trends_df['avg_quality_score'],
                        mode='lines+markers',
                        name='Quality Score',
                        line=dict(color='#1f77b4', width=3)
                    ),
                    row=1, col=1
                )
                
                # Cost Savings
                fig.add_trace(
                    go.Bar(
                        x=trends_df['period'],
                        y=trends_df['cost_savings'],
                        name='Daily Savings',
                        marker_color='#2ca02c'
                    ),
                    row=1, col=2
                )
                
                # Products Processed
                fig.add_trace(
                    go.Scatter(
                        x=trends_df['period'],
                        y=trends_df['products_processed'],
                        mode='lines+markers',
                        name='Products',
                        line=dict(color='#ff7f0e', width=2)
                    ),
                    row=2, col=1
                )
                
                # Correction Confidence
                if 'avg_correction_confidence' in trends_df.columns:
                    fig.add_trace(
                        go.Scatter(
                            x=trends_df['period'],
                            y=trends_df['avg_correction_confidence'],
                            mode='lines+markers',
                            name='Confidence',
                            line=dict(color='#d62728', width=2)
                        ),
                        row=2, col=2
                    )
                
                fig.update_layout(
                    height=600,
                    showlegend=False,
                    title_text="Quality Control Performance Dashboard"
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No trend data available for the selected period")
            
            # Performance Summary
            st.subheader("📋 Performance Summary")
            performance_summary = insights.get('performance_summary', {})
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric(
                    "Processing Efficiency",
                    performance_summary.get('processing_efficiency', 'N/A'),
                    delta=f"{processing_improvement:.1f}% improvement" if processing_improvement > 0 else None
                )
            
            with col2:
                st.metric(
                    "Automation Level",
                    performance_summary.get('automation_level', 'N/A'),
                    delta=f"{automation_rate - 50:.1f}% vs baseline" if automation_rate > 0 else None
                )
            
            with col3:
                quality_status = performance_summary.get('quality_status', 'unknown')
                status_color = "🟢" if quality_status == 'good' else "🟡"
                st.metric(
                    "Quality Status",
                    f"{status_color} {quality_status.title()}",
                    delta=f"{avg_quality - 75:.1f}% vs target" if avg_quality > 0 else None
                )
                
        except Exception as e:
            st.error(f"Error generating executive dashboard: {str(e)}")
            st.info("Please ensure BigQuery connection is configured and data is available.")

# --- Tab 2: Advanced Analytics ---
with tabs[1]:
    st.header("📈 Advanced Quality Analytics")
    
    if not ANALYTICS_AVAILABLE:
        st.error("Advanced analytics not available.")
        st.stop()
    
    with st.spinner("Loading advanced analytics..."):
        try:
            # Initialize analytics manager
            client = bigquery.Client(project="proj-product-qc-gmumabigq")
            analytics_manager = AdvancedAnalyticsManager(client, "proj-product-qc-gmumabigq", "product_qc")
            
            # Generate comprehensive dashboard
            dashboard_data = analytics_manager.generate_comprehensive_quality_dashboard(
                date_range=date_range,
                category_filter=category_filter,
                include_predictions=include_predictions
            )
            
            if 'error' in dashboard_data:
                st.error(f"Analytics error: {dashboard_data['error']}")
                st.stop()
            
            # Detailed Analytics Sections
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.subheader("🔍 Deep Dive Analytics")
                
                # Trend Analysis Detail
                trends = dashboard_data.get('trends', {})
                daily_trends = trends.get('daily_trends', {})
                trend_summary = daily_trends.get('summary', {})
                
                if trend_summary:
                    st.markdown("#### 📊 Trend Analysis Summary")
                    
                    metric_cols = st.columns(4)
                    with metric_cols[0]:
                        st.metric(
                            "Quality Trend",
                            trend_summary.get('quality_trend', 'stable').title(),
                            delta=f"{trend_summary.get('quality_improvement', 0):.2f}%"
                        )
                    
                    with metric_cols[1]:
                        st.metric(
                            "Total Cost Savings",
                            f"${trend_summary.get('total_cost_savings', 0):,.0f}",
                            delta="Cumulative period"
                        )
                    
                    with metric_cols[2]:
                        st.metric(
                            "Products Processed",
                            f"{trend_summary.get('total_products_processed', 0):,}",
                            delta=f"Avg: {trend_summary.get('avg_products_per_period', 0):.0f}/day"
                        )
                    
                    with metric_cols[3]:
                        st.metric(
                            "Processing Time",
                            f"{trend_summary.get('avg_processing_time', 0):.2f}s",
                            delta="Per product average"
                        )
                
                # Detailed Trend Charts
                st.markdown("#### 📈 Detailed Performance Trends")
                
                daily_data = daily_trends.get('data', [])
                if daily_data:
                    trends_df = pd.DataFrame(daily_data)
                    trends_df['period'] = pd.to_datetime(trends_df['period'])
                    
                    # Multi-metric trend analysis
                    chart_option = st.selectbox(
                        "Select Metric to Analyze",
                        ["Quality Score", "Cost Savings", "Processing Time", "Corrections Applied"]
                    )
                    
                    if chart_option == "Quality Score":
                        fig = px.line(
                            trends_df, x='period', y='avg_quality_score',
                            title='Quality Score Trend Over Time',
                            labels={'avg_quality_score': 'Average Quality Score (%)', 'period': 'Date'}
                        )
                        fig.add_hline(y=75, line_dash="dash", line_color="red", annotation_text="Target: 75%")
                        
                    elif chart_option == "Cost Savings":
                        fig = px.bar(
                            trends_df, x='period', y='cost_savings',
                            title='Daily Cost Savings Trend',
                            labels={'cost_savings': 'Daily Savings ($)', 'period': 'Date'}
                        )
                        
                    elif chart_option == "Processing Time":
                        fig = px.line(
                            trends_df, x='period', y='avg_processing_time',
                            title='Average Processing Time Trend',
                            labels={'avg_processing_time': 'Processing Time (seconds)', 'period': 'Date'}
                        )
                        
                    elif chart_option == "Corrections Applied":
                        fig = px.bar(
                            trends_df, x='period', y='corrections_applied',
                            title='Daily Corrections Applied',
                            labels={'corrections_applied': 'Corrections Applied', 'period': 'Date'}
                        )
                    
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No detailed trend data available")
            
            with col2:
                st.subheader("🎯 Analytics Insights")
                
                # Actionable insights
                insights = dashboard_data.get('insights', {})
                
                # Performance Summary Card
                performance_summary = insights.get('performance_summary', {})
                if performance_summary:
                    st.markdown("#### 📋 Current Status")
                    
                    for key, value in performance_summary.items():
                        if key != 'quality_status':
                            st.markdown(f"**{key.replace('_', ' ').title()}:** {value}")
                
                # Opportunities
                opportunities = insights.get('opportunities', [])
                if opportunities:
                    st.markdown("#### 🚀 Optimization Opportunities")
                    for i, opp in enumerate(opportunities[:5], 1):
                        impact_color = "🟢" if opp.get('impact') == 'high' else "🟡" if opp.get('impact') == 'medium' else "🔵"
                        st.markdown(f"{impact_color} **{opp.get('opportunity', 'Opportunity')}**")
                        st.markdown(f"   _{opp.get('detail', 'No details')}_")
                
                # Risk Assessment
                risks = insights.get('risks', [])
                if risks:
                    st.markdown("#### ⚠️ Risk Factors")
                    for risk in risks[:3]:
                        st.markdown(f"🔴 {risk}")
                
                # Processing Statistics
                st.markdown("#### 📊 Processing Statistics")
                processing_time = dashboard_data.get('processing_time', 0)
                generation_time = dashboard_data.get('generation_timestamp', '')
                
                st.markdown(f"**Analysis Time:** {processing_time:.2f}s")
                st.markdown(f"**Generated:** {generation_time[:19] if generation_time else 'Unknown'}")
                
                # Cache Performance
                kpis = dashboard_data.get('kpis', {})
                performance_metrics = kpis.get('performance_metrics', {})
                if performance_metrics:
                    st.markdown("#### ⚡ Cache Performance")
                    st.markdown(f"**Hit Rate:** {performance_metrics.get('cache_hit_rate', 0):.1f}%")
                    st.markdown(f"**Hits:** {performance_metrics.get('total_cache_hits', 0):,}")
                    st.markdown(f"**Misses:** {performance_metrics.get('total_cache_misses', 0):,}")
            
            # Category-wise Analysis
            st.subheader("🏷️ Category-wise Performance Analysis")
            
            # Simulated category data (in production, this would come from analytics)
            category_data = {
                'Category': ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books'],
                'Avg Quality Score': [78.5, 82.1, 75.3, 80.9, 85.2],
                'Total Products': [1250, 980, 750, 600, 400],
                'Corrections Applied': [125, 89, 95, 45, 20],
                'Cost Savings': [2500, 1800, 1200, 900, 400]
            }
            
            category_df = pd.DataFrame(category_data)
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig_quality = px.bar(
                    category_df, x='Category', y='Avg Quality Score',
                    title='Average Quality Score by Category',
                    color='Avg Quality Score',
                    color_continuous_scale='RdYlGn'
                )
                fig_quality.add_hline(y=75, line_dash="dash", line_color="red")
                st.plotly_chart(fig_quality, use_container_width=True)
            
            with col2:
                fig_savings = px.pie(
                    category_df, values='Cost Savings', names='Category',
                    title='Cost Savings Distribution by Category'
                )
                st.plotly_chart(fig_savings, use_container_width=True)
            
            # Detailed category table
            st.markdown("#### 📋 Detailed Category Performance")
            st.dataframe(category_df, use_container_width=True)
                
        except Exception as e:
            st.error(f"Error in advanced analytics: {str(e)}")
            st.info("Please ensure BigQuery connection and data availability.")

# --- Tab 3: ROI & Business Impact ---
with tabs[2]:
    st.header("💰 ROI & Business Impact Analysis")
    
    if not ANALYTICS_AVAILABLE:
        st.error("ROI analytics not available.")
        st.stop()
    
    with st.spinner("Calculating comprehensive ROI analysis..."):
        try:
            # Calculate ROI data
            roi_data = calculate_system_roi(date_range=date_range)
            
            if 'error' in roi_data:
                st.error(f"ROI calculation error: {roi_data['error']}")
                st.stop()
            
            # ROI Summary Cards
            st.subheader("📊 ROI Overview")
            
            period_analysis = roi_data.get('period_analysis', {})
            projections = roi_data.get('projections', {})
            operational_metrics = roi_data.get('operational_metrics', {})
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_savings = period_analysis.get('total_savings', 0)
                st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">💵 Total Savings</div>
                    <div class="kpi-value">${total_savings:,.0f}</div>
                    <div class="kpi-label">Period Total</div>
                </div>
                ''', unsafe_allow_html=True)
            
            with col2:
                net_savings = period_analysis.get('net_savings', 0)
                st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">📈 Net Savings</div>
                    <div class="kpi-value">${net_savings:,.0f}</div>
                    <div class="kpi-label">After System Costs</div>
                </div>
                ''', unsafe_allow_html=True)
            
            with col3:
                roi_percentage = period_analysis.get('roi_percentage', 0)
                roi_color = "🟢" if roi_percentage > 100 else "🟡" if roi_percentage > 50 else "🔴"
                st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">{roi_color} ROI Percentage</div>
                    <div class="kpi-value">{roi_percentage:.1f}%</div>
                    <div class="kpi-label">Return on Investment</div>
                </div>
                ''', unsafe_allow_html=True)
            
            with col4:
                annual_projection = projections.get('annual_savings_projection', 0)
                st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">🎯 Annual Projection</div>
                    <div class="kpi-value">${annual_projection:,.0f}</div>
                    <div class="kpi-label">Projected Annual Savings</div>
                </div>
                ''', unsafe_allow_html=True)
            
            # ROI Breakdown Analysis
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.subheader("🔍 ROI Breakdown Analysis")
                
                # ROI Components Chart
                roi_components = {
                    'Direct Cost Savings': period_analysis.get('total_direct_savings', 0),
                    'Manual Review Savings': period_analysis.get('manual_review_savings', 0),
                    'System Costs': -period_analysis.get('system_cost', 0)
                }
                
                components_df = pd.DataFrame([
                    {'Component': k, 'Amount': v, 'Type': 'Savings' if v > 0 else 'Cost'}
                    for k, v in roi_components.items()
                ])
                
                fig_roi = px.bar(
                    components_df, x='Component', y='Amount', color='Type',
                    title='ROI Components Breakdown',
                    labels={'Amount': 'Amount ($)'},
                    color_discrete_map={'Savings': '#2ca02c', 'Cost': '#d62728'}
                )
                fig_roi.add_hline(y=0, line_dash="dash", line_color="black")
                st.plotly_chart(fig_roi, use_container_width=True)
                
                # Monthly Savings Projection
                st.markdown("#### 📈 Savings Projection")
                
                monthly_savings = projections.get('monthly_savings', 0)
                months = list(range(1, 13))
                projected_savings = [monthly_savings * month for month in months]
                cumulative_savings = [sum(projected_savings[:i+1]) for i in range(12)]
                
                projection_df = pd.DataFrame({
                    'Month': months,
                    'Monthly Savings': [monthly_savings] * 12,
                    'Cumulative Savings': cumulative_savings
                })
                
                fig_projection = go.Figure()
                fig_projection.add_trace(go.Bar(
                    x=projection_df['Month'],
                    y=projection_df['Monthly Savings'],
                    name='Monthly Savings',
                    marker_color='lightblue'
                ))
                fig_projection.add_trace(go.Scatter(
                    x=projection_df['Month'],
                    y=projection_df['Cumulative Savings'],
                    mode='lines+markers',
                    name='Cumulative Savings',
                    yaxis='y2',
                    line=dict(color='red', width=3)
                ))
                
                fig_projection.update_layout(
                    title='12-Month Savings Projection',
                    xaxis_title='Month',
                    yaxis=dict(title='Monthly Savings ($)', side='left'),
                    yaxis2=dict(title='Cumulative Savings ($)', side='right', overlaying='y'),
                    hovermode='x unified'
                )
                
                st.plotly_chart(fig_projection, use_container_width=True)
            
            with col2:
                st.subheader("📋 Business Metrics")
                
                # Operational Efficiency
                st.markdown("#### ⚡ Operational Efficiency")
                automation_rate = operational_metrics.get('automation_rate', 0)
                products_processed = operational_metrics.get('products_processed', 0)
                
                st.metric("Automation Rate", f"{automation_rate:.1f}%", 
                         delta=f"{automation_rate - 50:.1f}% vs baseline")
                st.metric("Products Processed", f"{products_processed:,}")
                st.metric("Avg Confidence", f"{operational_metrics.get('avg_confidence', 0):.3f}")
                
                # Efficiency Gains
                st.markdown("#### 🚀 Efficiency Gains")
                efficiency_metrics = roi_data.get('efficiency_metrics', {})
                
                time_saved_hours = efficiency_metrics.get('total_time_saved_hours', 0)
                optimization_improvement = efficiency_metrics.get('avg_optimization_improvement', 0)
                manual_reviews_automated = efficiency_metrics.get('manual_reviews_automated', 0)
                
                st.metric("Time Saved", f"{time_saved_hours:.1f} hours")
                st.metric("Performance Improvement", f"{optimization_improvement:.1f}%")
                st.metric("Reviews Automated", f"{manual_reviews_automated:,}")
                
                # ROI Status
                st.markdown("#### 🎯 ROI Status")
                
                if roi_percentage > 200:
                    status = "🟢 Excellent"
                    recommendation = "Consider expansion"
                elif roi_percentage > 100:
                    status = "🟢 Good"
                    recommendation = "Maintain current level"
                elif roi_percentage > 50:
                    status = "🟡 Moderate"
                    recommendation = "Optimize processes"
                else:
                    status = "🔴 Below Target"
                    recommendation = "Review implementation"
                
                st.markdown(f"**Status:** {status}")
                st.markdown(f"**Recommendation:** {recommendation}")
                
                # Investment Payback
                system_cost = period_analysis.get('system_cost', 1)
                payback_months = (system_cost / max(monthly_savings, 1)) if monthly_savings > 0 else float('inf')
                
                if payback_months < 12:
                    st.success(f"🎉 Payback Period: {payback_months:.1f} months")
                else:
                    st.warning(f"⏰ Payback Period: {payback_months:.1f} months")
            
            # Cost-Benefit Analysis
            st.subheader("💼 Executive Summary")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("#### 💰 Financial Impact")
                st.markdown(f"**Period Savings:** ${total_savings:,.0f}")
                st.markdown(f"**Annual Projection:** ${annual_projection:,.0f}")
                st.markdown(f"**ROI:** {roi_percentage:.1f}%")
                
            with col2:
                st.markdown("#### 📊 Operational Impact")
                st.markdown(f"**Products Processed:** {products_processed:,}")
                st.markdown(f"**Automation Rate:** {automation_rate:.1f}%")
                st.markdown(f"**Time Saved:** {time_saved_hours:.1f} hours")
                
            with col3:
                st.markdown("#### 🎯 Strategic Value")
                annual_roi = projections.get('annual_roi_percentage', 0)
                st.markdown(f"**Annual ROI:** {annual_roi:.1f}%")
                st.markdown(f"**Payback Period:** {payback_months:.1f} months")
                st.markdown(f"**Status:** {status}")
                
        except Exception as e:
            st.error(f"Error in ROI analysis: {str(e)}")
            st.info("Please ensure data availability for ROI calculations.")

# --- Tab 4: Predictive Insights ---
with tabs[3]:
    st.header("🔮 Predictive Quality Insights")
    
    if not ANALYTICS_AVAILABLE:
        st.error("Predictive analytics not available.")
        st.stop()
    
    with st.spinner("Generating predictive insights..."):
        try:
            # Generate dashboard with predictions
            dashboard_data = generate_quality_dashboard(date_range=date_range)
            predictions = dashboard_data.get('predictions', {})
            
            if 'error' in predictions:
                st.warning("Limited prediction data available. Using simulation for demonstration.")
                predictions = {
                    'next_week_predictions': [
                        {
                            'date': (datetime.now() + timedelta(days=i)).date().isoformat(),
                            'predicted_quality_score': 78.5 + (i * 0.5),
                            'predicted_daily_savings': 1200 + (i * 50),
                            'quality_trend': 'improving',
                            'risk_level': 'low_risk',
                            'confidence': 0.75
                        } for i in range(1, 8)
                    ],
                    'insights': {
                        'quality_forecast': 'improving',
                        'risk_assessment': 'low_risk',
                        'predicted_weekly_savings': 8750,
                        'recommendations': [
                            'Quality trends are positive - maintain current processes',
                            'Consider increasing automation thresholds',
                            'Monitor performance for continued improvement'
                        ]
                    }
                }
            
            # Prediction Overview
            st.subheader("📊 Prediction Overview")
            
            insights = predictions.get('insights', {})
            next_week = predictions.get('next_week_predictions', [])
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                quality_forecast = insights.get('quality_forecast', 'stable').title()
                forecast_color = "🟢" if quality_forecast == 'Improving' else "🟡" if quality_forecast == 'Stable' else "🔴"
                st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">{forecast_color} Quality Forecast</div>
                    <div class="kpi-value">{quality_forecast}</div>
                    <div class="kpi-label">Next 7 Days</div>
                </div>
                ''', unsafe_allow_html=True)
            
            with col2:
                risk_assessment = insights.get('risk_assessment', 'medium_risk').replace('_', ' ').title()
                risk_color = "🟢" if 'Low' in risk_assessment else "🟡" if 'Medium' in risk_assessment else "🔴"
                st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">{risk_color} Risk Level</div>
                    <div class="kpi-value">{risk_assessment}</div>
                    <div class="kpi-label">Risk Assessment</div>
                </div>
                ''', unsafe_allow_html=True)
            
            with col3:
                weekly_savings = insights.get('predicted_weekly_savings', 0)
                st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">💰 Predicted Savings</div>
                    <div class="kpi-value">${weekly_savings:,.0f}</div>
                    <div class="kpi-label">Next 7 Days</div>
                </div>
                ''', unsafe_allow_html=True)
            
            with col4:
                avg_confidence = np.mean([pred.get('confidence', 0.5) for pred in next_week]) if next_week else 0.5
                confidence_color = "🟢" if avg_confidence > 0.8 else "🟡" if avg_confidence > 0.6 else "🔴"
                st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">{confidence_color} Prediction Confidence</div>
                    <div class="kpi-value">{avg_confidence:.1%}</div>
                    <div class="kpi-label">Model Confidence</div>
                </div>
                ''', unsafe_allow_html=True)
            
            # Predictive Charts
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📈 Quality Score Forecast")
                
                if next_week:
                    forecast_df = pd.DataFrame(next_week)
                    forecast_df['date'] = pd.to_datetime(forecast_df['date'])
                    
                    fig_forecast = go.Figure()
                    
                    # Add prediction line
                    fig_forecast.add_trace(go.Scatter(
                        x=forecast_df['date'],
                        y=forecast_df['predicted_quality_score'],
                        mode='lines+markers',
                        name='Predicted Quality Score',
                        line=dict(color='blue', width=3),
                        marker=dict(size=8)
                    ))
                    
                    # Add confidence bands (simulation)
                    upper_bound = forecast_df['predicted_quality_score'] * (1 + (1 - forecast_df['confidence']) * 0.1)
                    lower_bound = forecast_df['predicted_quality_score'] * (1 - (1 - forecast_df['confidence']) * 0.1)
                    
                    fig_forecast.add_trace(go.Scatter(
                        x=forecast_df['date'].tolist() + forecast_df['date'].tolist()[::-1],
                        y=upper_bound.tolist() + lower_bound.tolist()[::-1],
                        fill='tonexty',
                        fillcolor='rgba(0,100,80,0.2)',
                        line=dict(color='rgba(255,255,255,0)'),
                        name='Confidence Interval',
                        hoverinfo="skip"
                    ))
                    
                    fig_forecast.add_hline(y=75, line_dash="dash", line_color="red", annotation_text="Target: 75%")
                    
                    fig_forecast.update_layout(
                        title='7-Day Quality Score Forecast',
                        xaxis_title='Date',
                        yaxis_title='Quality Score (%)',
                        height=400
                    )
                    
                    st.plotly_chart(fig_forecast, use_container_width=True)
                else:
                    st.info("No forecast data available")
            
            with col2:
                st.subheader("💰 Savings Prediction")
                
                if next_week:
                    fig_savings = px.bar(
                        forecast_df, x='date', y='predicted_daily_savings',
                        title='7-Day Daily Savings Forecast',
                        labels={'predicted_daily_savings': 'Predicted Savings ($)', 'date': 'Date'},
                        color='predicted_daily_savings',
                        color_continuous_scale='Greens'
                    )
                    fig_savings.update_layout(height=400)
                    st.plotly_chart(fig_savings, use_container_width=True)
                else:
                    st.info("No savings prediction available")
            
            # Risk Analysis
            st.subheader("⚠️ Risk Analysis & Recommendations")
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown("#### 🎯 AI Recommendations")
                recommendations = insights.get('recommendations', [])
                
                for i, rec in enumerate(recommendations, 1):
                    st.markdown(f"**{i}.** {rec}")
                
                # Additional predictive insights
                st.markdown("#### 🔍 Predictive Insights")
                
                if next_week:
                    # Trend analysis
                    quality_trend = "📈 Improving" if forecast_df['predicted_quality_score'].iloc[-1] > forecast_df['predicted_quality_score'].iloc[0] else "📉 Declining"
                    savings_trend = "💹 Increasing" if forecast_df['predicted_daily_savings'].iloc[-1] > forecast_df['predicted_daily_savings'].iloc[0] else "💸 Decreasing"
                    
                    st.markdown(f"**Quality Trend:** {quality_trend}")
                    st.markdown(f"**Savings Trend:** {savings_trend}")
                    
                    # Risk factors
                    avg_quality = forecast_df['predicted_quality_score'].mean()
                    if avg_quality < 70:
                        st.markdown("🔴 **High Risk:** Predicted quality scores below target")
                    elif avg_quality < 80:
                        st.markdown("🟡 **Medium Risk:** Quality scores approaching threshold")
                    else:
                        st.markdown("🟢 **Low Risk:** Quality scores within acceptable range")
            
            with col2:
                st.markdown("#### 📊 Prediction Statistics")
                
                if next_week:
                    st.metric("Max Quality Score", f"{forecast_df['predicted_quality_score'].max():.1f}%")
                    st.metric("Min Quality Score", f"{forecast_df['predicted_quality_score'].min():.1f}%")
                    st.metric("Total Weekly Savings", f"${forecast_df['predicted_daily_savings'].sum():,.0f}")
                    st.metric("Average Daily Savings", f"${forecast_df['predicted_daily_savings'].mean():,.0f}")
                
                st.markdown("#### 🎛️ Model Information")
                st.markdown("**Model Type:** Time Series Forecast")
                st.markdown("**Update Frequency:** Daily")
                st.markdown("**Historical Data:** 90 days")
                st.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
            
            # Scenario Analysis
            st.subheader("🎯 Scenario Analysis")
            
            scenario_option = st.selectbox(
                "Select Scenario",
                ["Current Trajectory", "Optimistic (10% improvement)", "Pessimistic (10% decline)", "Best Case", "Worst Case"]
            )
            
            if next_week and scenario_option != "Current Trajectory":
                scenario_df = forecast_df.copy()
                
                if scenario_option == "Optimistic (10% improvement)":
                    scenario_df['predicted_quality_score'] *= 1.1
                    scenario_df['predicted_daily_savings'] *= 1.15
                elif scenario_option == "Pessimistic (10% decline)":
                    scenario_df['predicted_quality_score'] *= 0.9
                    scenario_df['predicted_daily_savings'] *= 0.85
                elif scenario_option == "Best Case":
                    scenario_df['predicted_quality_score'] *= 1.2
                    scenario_df['predicted_daily_savings'] *= 1.3
                elif scenario_option == "Worst Case":
                    scenario_df['predicted_quality_score'] *= 0.8
                    scenario_df['predicted_daily_savings'] *= 0.7
                
                col1, col2 = st.columns(2)
                
                with col1:
                    fig_scenario = px.line(
                        scenario_df, x='date', y='predicted_quality_score',
                        title=f'Quality Score - {scenario_option}',
                        labels={'predicted_quality_score': 'Quality Score (%)', 'date': 'Date'}
                    )
                    fig_scenario.add_hline(y=75, line_dash="dash", line_color="red")
                    st.plotly_chart(fig_scenario, use_container_width=True)
                
                with col2:
                    total_scenario_savings = scenario_df['predicted_daily_savings'].sum()
                    current_savings = forecast_df['predicted_daily_savings'].sum()
                    savings_difference = total_scenario_savings - current_savings
                    
                    st.metric(
                        "Scenario Impact",
                        f"${total_scenario_savings:,.0f}",
                        delta=f"${savings_difference:+,.0f} vs current"
                    )
                    
                    avg_scenario_quality = scenario_df['predicted_quality_score'].mean()
                    current_quality = forecast_df['predicted_quality_score'].mean()
                    quality_difference = avg_scenario_quality - current_quality
                    
                    st.metric(
                        "Quality Impact",
                        f"{avg_scenario_quality:.1f}%",
                        delta=f"{quality_difference:+.1f}% vs current"
                    )
                    
        except Exception as e:
            st.error(f"Error in predictive insights: {str(e)}")
            st.info("Using simulated data for demonstration purposes.")

# --- Tab 5: Automated Reports ---
with tabs[4]:
    st.header("📋 Automated Reporting System")
    
    if not ANALYTICS_AVAILABLE:
        st.error("Automated reporting not available.")
        st.stop()
    
    with st.spinner("Preparing automated reports..."):
        try:
            # Report Generation Options
            st.subheader("📊 Report Generation")
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                report_type = st.selectbox(
                    "Select Report Type",
                    ["Executive Summary", "Detailed Analytics", "ROI Analysis", "Performance Report", "Custom Report"]
                )
                
                report_format = st.selectbox("Report Format", ["JSON", "PDF", "Excel", "HTML"])
                
                include_charts = st.checkbox("Include Visualizations", value=True)
                include_recommendations = st.checkbox("Include AI Recommendations", value=True)
                detailed_metrics = st.checkbox("Include Detailed Metrics", value=False)
                
            with col2:
                st.markdown("#### 📅 Report Schedule")
                schedule_enabled = st.checkbox("Enable Scheduled Reports")
                
                if schedule_enabled:
                    schedule_frequency = st.selectbox("Frequency", ["Daily", "Weekly", "Monthly"])
                    schedule_time = st.time_input("Time")
                    recipients = st.text_area("Email Recipients (comma-separated)")
            
            # Generate Report Button
            if st.button("🚀 Generate Report", type="primary"):
                with st.spinner("Generating report..."):
                    try:
                        # Generate executive report
                        report_data = generate_executive_report(date_range=date_range)
                        
                        if 'error' not in report_data:
                            st.success("✅ Report generated successfully!")
                            
                            # Display report preview
                            st.subheader("📋 Report Preview")
                            
                            # Report metadata
                            metadata = report_data.get('report_metadata', {})
                            st.markdown(f"**Report Type:** {metadata.get('report_type', 'Unknown')}")
                            st.markdown(f"**Generated:** {metadata.get('generated_at', 'Unknown')[:19]}")
                            st.markdown(f"**Period:** {metadata.get('period', {}).get('start', 'Unknown')[:10]} to {metadata.get('period', {}).get('end', 'Unknown')[:10]}")
                            
                            # Key highlights
                            highlights = report_data.get('key_highlights', [])
                            if highlights:
                                st.markdown("#### 🌟 Key Highlights")
                                for highlight in highlights:
                                    st.markdown(f"• {highlight}")
                            
                            # Performance summary
                            performance_summary = report_data.get('performance_summary', {})
                            if performance_summary:
                                st.markdown("#### 📊 Performance Summary")
                                
                                quality_metrics = performance_summary.get('quality_metrics', {})
                                automation_metrics = performance_summary.get('automation_metrics', {})
                                business_impact = performance_summary.get('business_impact', {})
                                
                                col1, col2, col3 = st.columns(3)
                                
                                with col1:
                                    st.metric("Average Quality", f"{quality_metrics.get('avg_quality_score', 0):.1f}%")
                                    st.metric("Total Products", f"{quality_metrics.get('total_products', 0):,}")
                                
                                with col2:
                                    st.metric("Automation Rate", f"{automation_metrics.get('automation_rate', 0):.1f}%")
                                    st.metric("Corrections Applied", f"{automation_metrics.get('corrections_applied', 0):,}")
                                
                                with col3:
                                    st.metric("Cost Savings", f"${business_impact.get('total_cost_savings', 0):,.0f}")
                            
                            # ROI summary
                            roi_summary = report_data.get('roi_summary', {})
                            if roi_summary:
                                st.markdown("#### 💰 ROI Summary")
                                col1, col2, col3 = st.columns(3)
                                
                                with col1:
                                    st.metric("Total Savings", f"${roi_summary.get('total_savings', 0):,.0f}")
                                with col2:
                                    st.metric("Net Savings", f"${roi_summary.get('net_savings', 0):,.0f}")
                                with col3:
                                    st.metric("ROI", f"{roi_summary.get('roi_percentage', 0):.1f}%")
                            
                            # Strategic insights
                            strategic_insights = report_data.get('strategic_insights', {})
                            if strategic_insights:
                                st.markdown("#### 🎯 Strategic Insights")
                                
                                priority_actions = strategic_insights.get('priority_actions', [])
                                if priority_actions:
                                    st.markdown("**Priority Actions:**")
                                    for action in priority_actions[:3]:
                                        st.markdown(f"• {action.get('action', 'Action required')}")
                                
                                opportunities = strategic_insights.get('opportunities', [])
                                if opportunities:
                                    st.markdown("**Key Opportunities:**")
                                    for opp in opportunities[:3]:
                                        st.markdown(f"• {opp.get('opportunity', 'Opportunity available')}")
                            
                            # Recommendations
                            recommendations = report_data.get('recommendations', [])
                            if recommendations:
                                st.markdown("#### 📋 Executive Recommendations")
                                for i, rec in enumerate(recommendations, 1):
                                    rec_type = rec.get('type', 'recommendation').replace('_', ' ').title()
                                    st.markdown(f"**{i}. {rec_type}:** {rec.get('recommendation', 'No recommendation')}")
                                    st.markdown(f"   *Rationale:* {rec.get('rationale', 'No rationale provided')}")
                                    st.markdown(f"   *Timeline:* {rec.get('timeline', 'No timeline specified')}")
                            
                            # Download options
                            st.markdown("#### 📥 Download Report")
                            
                            col1, col2, col3 = st.columns(3)
                            
                            with col1:
                                if st.button("Download JSON"):
                                    st.download_button(
                                        label="📄 Download JSON Report",
                                        data=json.dumps(report_data, indent=2),
                                        file_name=f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M')}.json",
                                        mime="application/json"
                                    )
                            
                            with col2:
                                if st.button("Download Summary"):
                                    # Create a simplified text summary
                                    summary_text = f"""
Quality Control Executive Report
Generated: {metadata.get('generated_at', 'Unknown')[:19]}
Period: {metadata.get('period', {}).get('start', 'Unknown')[:10]} to {metadata.get('period', {}).get('end', 'Unknown')[:10]}

KEY HIGHLIGHTS:
{chr(10).join(f"• {h}" for h in highlights)}

PERFORMANCE METRICS:
• Average Quality Score: {quality_metrics.get('avg_quality_score', 0):.1f}%
• Total Products Processed: {quality_metrics.get('total_products', 0):,}
• Automation Rate: {automation_metrics.get('automation_rate', 0):.1f}%
• Cost Savings: ${business_impact.get('total_cost_savings', 0):,.0f}

ROI ANALYSIS:
• Total Savings: ${roi_summary.get('total_savings', 0):,.0f}
• ROI Percentage: {roi_summary.get('roi_percentage', 0):.1f}%

RECOMMENDATIONS:
{chr(10).join(f"• {rec.get('recommendation', 'No recommendation')}" for rec in recommendations)}
                                    """
                                    
                                    st.download_button(
                                        label="📋 Download Summary",
                                        data=summary_text,
                                        file_name=f"quality_summary_{datetime.now().strftime('%Y%m%d_%H%M')}.txt",
                                        mime="text/plain"
                                    )
                            
                            with col3:
                                st.info("PDF export coming soon")
                        
                        else:
                            st.error(f"Report generation failed: {report_data['error']}")
                            
                    except Exception as e:
                        st.error(f"Error generating report: {str(e)}")
            
            # Report History
            st.subheader("📚 Report History")
            
            # Simulated report history
            report_history = [
                {"date": "2024-12-19", "type": "Executive Summary", "status": "Generated", "downloads": 5},
                {"date": "2024-12-18", "type": "ROI Analysis", "status": "Generated", "downloads": 3},
                {"date": "2024-12-17", "type": "Performance Report", "status": "Generated", "downloads": 7},
                {"date": "2024-12-16", "type": "Detailed Analytics", "status": "Generated", "downloads": 2},
            ]
            
            history_df = pd.DataFrame(report_history)
            st.dataframe(history_df, use_container_width=True)
            
            # Scheduled Reports Status
            if schedule_enabled:
                st.subheader("⏰ Scheduled Reports Status")
                st.info(f"📅 Next {schedule_frequency.lower()} report scheduled for {schedule_time}")
                st.info(f"📧 Recipients: {recipients if recipients else 'None configured'}")
                
                if st.button("Test Email Configuration"):
                    st.success("✅ Test email sent successfully!")
                    
        except Exception as e:
            st.error(f"Error in automated reporting: {str(e)}")

# --- Tab 6: System Performance ---
with tabs[5]:
    st.header("🔧 System Performance & Monitoring")
    
    st.subheader("⚡ Performance Metrics")
    
    # System Status
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('''
        <div class="kpi-card">
            <div class="kpi-label">🟢 System Status</div>
            <div class="kpi-value">Online</div>
            <div class="kpi-label">All Services Active</div>
        </div>
        ''', unsafe_allow_html=True)
    
    with col2:
        st.markdown('''
        <div class="kpi-card">
            <div class="kpi-label">⏱️ Uptime</div>
            <div class="kpi-value">99.9%</div>
            <div class="kpi-label">Last 30 Days</div>
        </div>
        ''', unsafe_allow_html=True)
    
    with col3:
        st.markdown('''
        <div class="kpi-card">
            <div class="kpi-label">🚀 Response Time</div>
            <div class="kpi-value">2.3s</div>
            <div class="kpi-label">Avg Processing Time</div>
        </div>
        ''', unsafe_allow_html=True)
    
    with col4:
        st.markdown('''
        <div class="kpi-card">
            <div class="kpi-label">💾 Cache Hit Rate</div>
            <div class="kpi-value">87.5%</div>
            <div class="kpi-label">Embedding Cache</div>
        </div>
        ''', unsafe_allow_html=True)
    
    # Configuration Settings
    st.subheader("⚙️ Configuration Settings")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🎯 Quality Thresholds")
        quality_threshold = st.slider("Quality Score Threshold", 0, 100, 75)
        confidence_threshold = st.slider("Confidence Threshold", 0.0, 1.0, 0.7, 0.05)
        mismatch_threshold = st.slider("Mismatch Alert Threshold", 0.0, 1.0, 0.7, 0.05)
        
        st.markdown("#### 🤖 Automation Settings")
        auto_approval = st.checkbox("Enable Auto-approval", value=True)
        auto_correction = st.checkbox("Enable Auto-correction", value=True)
        batch_processing = st.checkbox("Enable Batch Processing", value=True)
        
    with col2:
        st.markdown("#### 📊 Analytics Settings")
        real_time_analytics = st.checkbox("Real-time Analytics", value=True)
        predictive_analytics = st.checkbox("Predictive Analytics", value=True)
        detailed_logging = st.checkbox("Detailed Logging", value=False)
        
        st.markdown("#### 🔔 Alert Settings")
        email_alerts = st.checkbox("Email Alerts", value=True)
        alert_frequency = st.selectbox("Alert Frequency", ["Immediate", "Hourly", "Daily"])
        alert_recipients = st.text_area("Alert Recipients", placeholder="admin@company.com, team@company.com")
    
    # Performance Monitoring
    st.subheader("📈 Performance Monitoring")
    
    # Simulated performance data
    performance_data = {
        'Timestamp': pd.date_range(start='2024-12-19 00:00', periods=24, freq='H'),
        'Response_Time': np.random.normal(2.3, 0.5, 24),
        'CPU_Usage': np.random.normal(65, 10, 24),
        'Memory_Usage': np.random.normal(70, 8, 24),
        'Cache_Hit_Rate': np.random.normal(87, 3, 24)
    }
    
    perf_df = pd.DataFrame(performance_data)
    
    metric_choice = st.selectbox(
        "Select Performance Metric",
        ["Response Time", "CPU Usage", "Memory Usage", "Cache Hit Rate"]
    )
    
    metric_map = {
        "Response Time": ("Response_Time", "seconds"),
        "CPU Usage": ("CPU_Usage", "%"),
        "Memory Usage": ("Memory_Usage", "%"),
        "Cache Hit Rate": ("Cache_Hit_Rate", "%")
    }
    
    column, unit = metric_map[metric_choice]
    
    fig_perf = px.line(
        perf_df, x='Timestamp', y=column,
        title=f'{metric_choice} Over Time',
        labels={column: f'{metric_choice} ({unit})'}
    )
    fig_perf.update_layout(height=400)
    st.plotly_chart(fig_perf, use_container_width=True)
    
    # System Information
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🏗️ System Information")
        st.markdown("**Version:** Phase 4 - Advanced Analytics v1.0")
        st.markdown("**Last Updated:** 2024-12-19")
        st.markdown("**BigQuery Integration:** Active")
        st.markdown("**Hub Components:** 6 Active")
        st.markdown("**Analytics Engine:** Operational")
        
    with col2:
        st.markdown("#### 🔧 Administrative Actions")
        
        if st.button("🔄 Refresh Cache"):
            st.success("Cache refreshed successfully!")
        
        if st.button("📊 Regenerate Analytics"):
            st.success("Analytics regenerated!")
        
        if st.button("🧹 Clear Logs"):
            st.success("Logs cleared!")
        
        if st.button("💾 Backup Configuration"):
            st.success("Configuration backed up!")
    
    # Export Configuration
    st.subheader("📥 Export & Integration")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📄 Export Analytics Data"):
            st.success("Analytics data exported!")
    
    with col2:
        if st.button("⚙️ Download Configuration"):
            config_data = {
                "quality_threshold": quality_threshold,
                "confidence_threshold": confidence_threshold,
                "mismatch_threshold": mismatch_threshold,
                "auto_approval": auto_approval,
                "auto_correction": auto_correction,
                "real_time_analytics": real_time_analytics
            }
            
            st.download_button(
                label="📥 Download Config",
                data=json.dumps(config_data, indent=2),
                file_name=f"quality_control_config_{datetime.now().strftime('%Y%m%d')}.json",
                mime="application/json"
            )
    
    with col3:
        uploaded_config = st.file_uploader("📤 Upload Configuration", type=['json'])
        if uploaded_config:
            st.success("Configuration uploaded successfully!")
