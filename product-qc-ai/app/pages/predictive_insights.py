"""Predictive Insights page for AI-powered predictions and forecasting."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional, Tuple
import scipy.stats as stats
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

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

class PredictiveInsightsPage:
    """Predictive Insights page class."""
    
    def __init__(self):
        """Initialize Predictive Insights page."""
        self.prediction_models = {}
        self.model_performance = {}
        self.forecast_horizon = 30  # days
    
    def render(self):
        """Render the Predictive Insights page."""
        render_header_section(
            title="Predictive Insights",
            description="AI-powered predictions, forecasting, and trend analysis for quality control",
            icon="🔮"
        )
        
        # Configuration section
        self._render_prediction_config()
        
        # Main prediction sections
        self._render_predictions_overview()
        self._render_quality_forecasting()
        self._render_failure_prediction()
        self._render_trend_analysis()
        self._render_demand_forecasting()
        self._render_risk_assessment()
        self._render_model_performance()
        self._render_actionable_insights()
    
    def _render_prediction_config(self):
        """Render prediction configuration section."""
        st.subheader("⚙️ Prediction Configuration")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            self.forecast_horizon = st.selectbox(
                "Forecast Horizon",
                options=[7, 14, 30, 60, 90],
                index=2,
                help="Number of days to forecast ahead"
            )
        
        with col2:
            confidence_level = st.selectbox(
                "Confidence Level",
                options=[80, 85, 90, 95, 99],
                index=3,
                help="Statistical confidence level for predictions"
            )
        
        with col3:
            model_type = st.selectbox(
                "Primary Model",
                options=["Ensemble", "Random Forest", "Gradient Boosting", "Neural Network"],
                index=0,
                help="Primary prediction model to use"
            )
        
        with col4:
            update_frequency = st.selectbox(
                "Update Frequency",
                options=["Real-time", "Hourly", "Daily", "Weekly"],
                index=2,
                help="How often predictions are updated"
            )
        
        st.markdown("---")
    
    def _render_predictions_overview(self):
        """Render predictions overview section."""
        st.subheader("📊 Predictions Overview")
        
        # Key prediction metrics
        prediction_summary = self._get_prediction_summary()
        
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            accuracy = prediction_summary.get("overall_accuracy", 0)
            create_metric_card(
                title="Overall Accuracy",
                value=f"{accuracy:.1f}%",
                delta=f"+{prediction_summary.get('accuracy_improvement', 0):.1f}%",
                delta_color="normal",
                help_text="Average prediction accuracy across all models"
            )
        
        with col2:
            active_models = prediction_summary.get("active_models", 0)
            create_metric_card(
                title="Active Models",
                value=str(active_models),
                help_text="Number of active prediction models"
            )
        
        with col3:
            predictions_today = prediction_summary.get("predictions_today", 0)
            create_metric_card(
                title="Predictions Today",
                value=format_large_number(predictions_today),
                delta=f"+{prediction_summary.get('prediction_increase', 0)}",
                delta_color="normal",
                help_text="Number of predictions generated today"
            )
        
        with col4:
            alerts_predicted = prediction_summary.get("alerts_predicted", 0)
            create_metric_card(
                title="Alerts Predicted",
                value=str(alerts_predicted),
                delta_color="inverse" if alerts_predicted > 5 else "normal",
                help_text="Number of quality alerts predicted"
            )
        
        with col5:
            model_drift = prediction_summary.get("model_drift", 0)
            create_metric_card(
                title="Model Drift",
                value=f"{model_drift:.2f}%",
                delta_color="inverse" if model_drift > 5 else "normal",
                help_text="Model performance drift indicator"
            )
        
        with col6:
            next_retrain = prediction_summary.get("next_retrain_days", 0)
            create_metric_card(
                title="Next Retrain",
                value=f"{next_retrain} days",
                help_text="Days until next model retraining"
            )
        
        # Prediction accuracy trend
        accuracy_trend = self._get_accuracy_trend_data()
        
        fig = go.Figure()
        
        for model_name, data in accuracy_trend.items():
            fig.add_trace(go.Scatter(
                x=data['dates'],
                y=data['accuracy'],
                mode='lines+markers',
                name=model_name,
                line=dict(width=2)
            ))
        
        fig.add_hline(y=85, line_dash="dash", line_color="red", 
                     annotation_text="Minimum Accuracy Threshold")
        
        fig.update_layout(
            title='Model Accuracy Trends',
            xaxis_title='Date',
            yaxis_title='Accuracy (%)',
            height=400,
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("---")
    
    def _render_quality_forecasting(self):
        """Render quality forecasting section."""
        st.subheader("📈 Quality Forecasting")
        
        # Create tabs for different forecasting aspects
        tab1, tab2, tab3, tab4 = st.tabs([
            "📊 Quality Score Forecast", 
            "🎯 Defect Rate Prediction", 
            "⚡ Process Performance",
            "🔄 Seasonal Patterns"
        ])
        
        with tab1:
            self._render_quality_score_forecast()
        
        with tab2:
            self._render_defect_rate_prediction()
        
        with tab3:
            self._render_process_performance_forecast()
        
        with tab4:
            self._render_seasonal_analysis()
        
        st.markdown("---")
    
    def _render_quality_score_forecast(self):
        """Render quality score forecasting."""
        st.markdown("### 📊 Quality Score Forecast")
        
        # Generate forecast data
        forecast_data = self._generate_quality_forecast()
        
        # Create forecast visualization
        fig = go.Figure()
        
        # Historical data
        fig.add_trace(go.Scatter(
            x=forecast_data['historical_dates'],
            y=forecast_data['historical_values'],
            mode='lines+markers',
            name='Historical Quality Score',
            line=dict(color=COLOR_SCHEMES["primary"], width=2),
            marker=dict(size=4)
        ))
        
        # Forecast
        fig.add_trace(go.Scatter(
            x=forecast_data['forecast_dates'],
            y=forecast_data['forecast_values'],
            mode='lines+markers',
            name='Predicted Quality Score',
            line=dict(color=COLOR_SCHEMES["secondary"], width=2, dash='dash'),
            marker=dict(size=4)
        ))
        
        # Confidence intervals
        fig.add_trace(go.Scatter(
            x=forecast_data['forecast_dates'] + forecast_data['forecast_dates'][::-1],
            y=forecast_data['upper_bound'] + forecast_data['lower_bound'][::-1],
            fill='toself',
            fillcolor='rgba(0,100,80,0.2)',
            line=dict(color='rgba(255,255,255,0)'),
            name='95% Confidence Interval',
            showlegend=True
        ))
        
        # Add quality targets
        fig.add_hline(y=0.85, line_dash="dot", line_color="green", 
                     annotation_text="Target Quality (85%)")
        fig.add_hline(y=0.75, line_dash="dot", line_color="orange", 
                     annotation_text="Warning Threshold (75%)")
        
        fig.update_layout(
            title=f'Quality Score Forecast ({self.forecast_horizon} days)',
            xaxis_title='Date',
            yaxis_title='Quality Score',
            height=500,
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Forecast summary
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_forecast = np.mean(forecast_data['forecast_values'])
            st.metric("Avg Predicted Quality", f"{avg_forecast:.3f}")
        
        with col2:
            trend = "↑" if forecast_data['forecast_values'][-1] > forecast_data['forecast_values'][0] else "↓"
            trend_value = forecast_data['forecast_values'][-1] - forecast_data['forecast_values'][0]
            st.metric("Trend", f"{trend} {trend_value:+.3f}")
        
        with col3:
            volatility = np.std(forecast_data['forecast_values'])
            st.metric("Forecast Volatility", f"{volatility:.3f}")
        
        with col4:
            risk_days = sum(1 for v in forecast_data['forecast_values'] if v < 0.75)
            st.metric("Risk Days", f"{risk_days}/{len(forecast_data['forecast_values'])}")
    
    def _render_defect_rate_prediction(self):
        """Render defect rate prediction."""
        st.markdown("### 🎯 Defect Rate Prediction")
        
        defect_prediction = self._generate_defect_prediction()
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Defect rate forecast chart
            fig = px.line(
                defect_prediction,
                x='date',
                y='predicted_defect_rate',
                title='Predicted Defect Rate',
                labels={'predicted_defect_rate': 'Defect Rate (%)', 'date': 'Date'}
            )
            
            # Add historical average
            historical_avg = defect_prediction['historical_avg'].iloc[0]
            fig.add_hline(y=historical_avg, line_dash="dash", line_color="gray",
                         annotation_text=f"Historical Avg ({historical_avg:.2f}%)")
            
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Defect category breakdown
            category_prediction = self._get_defect_category_prediction()
            
            fig = px.bar(
                category_prediction,
                x='category',
                y='predicted_defects',
                title='Predicted Defects by Category',
                color='risk_level',
                color_discrete_map={'Low': 'green', 'Medium': 'orange', 'High': 'red'}
            )
            fig.update_layout(height=400)
            fig.update_xaxis(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        
        # Defect prediction insights
        st.markdown("**Key Insights:**")
        
        insights = defect_prediction.groupby('risk_level').size()
        for risk_level, count in insights.items():
            color = {"Low": "🟢", "Medium": "🟡", "High": "🔴"}.get(risk_level, "⚪")
            st.write(f"{color} **{risk_level} Risk Days**: {count} days predicted")
    
    def _render_process_performance_forecast(self):
        """Render process performance forecasting."""
        st.markdown("### ⚡ Process Performance Forecast")
        
        performance_data = self._generate_performance_forecast()
        
        # Multi-metric forecast
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=['Throughput', 'Efficiency', 'Downtime', 'Quality Score'],
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        metrics = ['throughput', 'efficiency', 'downtime', 'quality']
        positions = [(1,1), (1,2), (2,1), (2,2)]
        
        for metric, (row, col) in zip(metrics, positions):
            fig.add_trace(
                go.Scatter(
                    x=performance_data['date'],
                    y=performance_data[f'predicted_{metric}'],
                    mode='lines+markers',
                    name=f'Predicted {metric.title()}',
                    line=dict(width=2)
                ),
                row=row, col=col
            )
        
        fig.update_layout(height=600, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        
        # Performance summary
        st.markdown("**Performance Forecast Summary:**")
        
        summary_data = []
        for metric in metrics:
            current_val = performance_data[f'predicted_{metric}'].iloc[0]
            future_val = performance_data[f'predicted_{metric}'].iloc[-1]
            change = ((future_val - current_val) / current_val) * 100
            
            summary_data.append({
                'Metric': metric.title(),
                'Current': f"{current_val:.2f}",
                'Predicted (End of Period)': f"{future_val:.2f}",
                'Change (%)': f"{change:+.1f}%"
            })
        
        summary_df = pd.DataFrame(summary_data)
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
    
    def _render_seasonal_analysis(self):
        """Render seasonal pattern analysis."""
        st.markdown("### 🔄 Seasonal Patterns Analysis")
        
        seasonal_data = self._generate_seasonal_analysis()
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Monthly seasonality
            fig = px.line(
                seasonal_data['monthly'],
                x='month',
                y='quality_index',
                title='Monthly Quality Seasonality',
                markers=True
            )
            fig.add_hline(y=1.0, line_dash="dash", line_color="gray",
                         annotation_text="Baseline")
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Weekly seasonality
            fig = px.bar(
                seasonal_data['weekly'],
                x='day_of_week',
                y='quality_index',
                title='Weekly Quality Patterns',
                color='quality_index',
                color_continuous_scale='RdYlGn'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Seasonal insights
        st.markdown("**Seasonal Insights:**")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            best_month = seasonal_data['monthly'].loc[
                seasonal_data['monthly']['quality_index'].idxmax(), 'month'
            ]
            st.write(f"🏆 **Best Month**: {best_month}")
        
        with col2:
            worst_month = seasonal_data['monthly'].loc[
                seasonal_data['monthly']['quality_index'].idxmin(), 'month'
            ]
            st.write(f"⚠️ **Challenging Month**: {worst_month}")
        
        with col3:
            best_day = seasonal_data['weekly'].loc[
                seasonal_data['weekly']['quality_index'].idxmax(), 'day_of_week'
            ]
            st.write(f"📈 **Best Day**: {best_day}")
    
    def _render_failure_prediction(self):
        """Render failure prediction section."""
        st.subheader("⚠️ Failure Prediction")
        
        failure_data = self._generate_failure_predictions()
        
        # Failure risk heatmap
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Risk matrix heatmap
            risk_matrix = failure_data['risk_matrix']
            
            fig = px.imshow(
                risk_matrix,
                x=['Line 1', 'Line 2', 'Line 3', 'Line 4', 'Line 5'],
                y=['Equipment A', 'Equipment B', 'Equipment C', 'Equipment D'],
                title='Equipment Failure Risk Matrix (Next 30 Days)',
                color_continuous_scale='Reds',
                aspect='auto'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("**High Risk Equipment:**")
            
            high_risk_items = failure_data['high_risk_equipment']
            for item in high_risk_items:
                risk_color = "🔴" if item['risk'] > 80 else "🟡" if item['risk'] > 60 else "🟢"
                st.write(f"{risk_color} **{item['equipment']}**: {item['risk']:.0f}% risk")
                st.write(f"   📅 Predicted failure: {item['predicted_date']}")
                st.write(f"   💰 Impact: ${format_large_number(item['impact'])}")
        
        # Failure timeline
        timeline_data = failure_data['timeline']
        
        fig = go.Figure()
        
        for equipment, data in timeline_data.items():
            fig.add_trace(go.Scatter(
                x=data['dates'],
                y=[equipment] * len(data['dates']),
                mode='markers',
                marker=dict(
                    size=data['risk_scores'],
                    color=data['risk_scores'],
                    colorscale='Reds',
                    showscale=True if equipment == list(timeline_data.keys())[0] else False,
                    sizemode='diameter',
                    sizeref=2
                ),
                name=equipment,
                hovertemplate=f'<b>{equipment}</b><br>Date: %{{x}}<br>Risk: %{{marker.color:.0f}}%<extra></extra>'
            ))
        
        fig.update_layout(
            title='Failure Risk Timeline',
            xaxis_title='Date',
            yaxis_title='Equipment',
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
    
    def _render_trend_analysis(self):
        """Render trend analysis section."""
        st.subheader("📊 Trend Analysis")
        
        trend_data = self._generate_trend_analysis()
        
        # Multi-timeframe trend analysis
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=['Short-term (7 days)', 'Medium-term (30 days)', 
                          'Long-term (90 days)', 'Annual Pattern'],
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        timeframes = ['short_term', 'medium_term', 'long_term', 'annual']
        positions = [(1,1), (1,2), (2,1), (2,2)]
        
        for timeframe, (row, col) in zip(timeframes, positions):
            data = trend_data[timeframe]
            
            fig.add_trace(
                go.Scatter(
                    x=data['x'],
                    y=data['y'],
                    mode='lines+markers',
                    name=f'{timeframe.replace("_", " ").title()}',
                    line=dict(width=2)
                ),
                row=row, col=col
            )
            
            # Add trend line
            z = np.polyfit(range(len(data['y'])), data['y'], 1)
            p = np.poly1d(z)
            
            fig.add_trace(
                go.Scatter(
                    x=data['x'],
                    y=p(range(len(data['y']))),
                    mode='lines',
                    name=f'{timeframe.title()} Trend',
                    line=dict(dash='dash', width=1),
                    showlegend=False
                ),
                row=row, col=col
            )
        
        fig.update_layout(height=600, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        
        # Trend insights
        st.markdown("**Trend Analysis Summary:**")
        
        trend_summary = []
        for timeframe in timeframes:
            data = trend_data[timeframe]
            slope = np.polyfit(range(len(data['y'])), data['y'], 1)[0]
            direction = "📈 Improving" if slope > 0 else "📉 Declining" if slope < 0 else "➡️ Stable"
            
            trend_summary.append({
                'Timeframe': timeframe.replace('_', ' ').title(),
                'Direction': direction,
                'Slope': f"{slope:.4f}",
                'Confidence': f"{np.random.uniform(75, 95):.1f}%"
            })
        
        summary_df = pd.DataFrame(trend_summary)
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
        
        st.markdown("---")
    
    def _render_demand_forecasting(self):
        """Render demand forecasting section."""
        st.subheader("📦 Demand Forecasting")
        
        demand_data = self._generate_demand_forecast()
        
        # Demand forecast visualization
        fig = go.Figure()
        
        # Historical demand
        fig.add_trace(go.Scatter(
            x=demand_data['historical_dates'],
            y=demand_data['historical_demand'],
            mode='lines+markers',
            name='Historical Demand',
            line=dict(color=COLOR_SCHEMES["primary"], width=2)
        ))
        
        # Predicted demand
        fig.add_trace(go.Scatter(
            x=demand_data['forecast_dates'],
            y=demand_data['predicted_demand'],
            mode='lines+markers',
            name='Predicted Demand',
            line=dict(color=COLOR_SCHEMES["secondary"], width=2, dash='dash')
        ))
        
        # Capacity line
        avg_capacity = np.mean(demand_data['capacity'])
        fig.add_hline(y=avg_capacity, line_dash="dot", line_color="red",
                     annotation_text=f"Current Capacity ({avg_capacity:.0f} units)")
        
        fig.update_layout(
            title='Demand Forecast vs Capacity',
            xaxis_title='Date',
            yaxis_title='Units',
            height=500,
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Demand insights
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            peak_demand = max(demand_data['predicted_demand'])
            st.metric("Peak Demand", f"{peak_demand:.0f} units")
        
        with col2:
            avg_demand = np.mean(demand_data['predicted_demand'])
            st.metric("Average Demand", f"{avg_demand:.0f} units")
        
        with col3:
            capacity_utilization = (avg_demand / avg_capacity) * 100
            st.metric("Capacity Utilization", f"{capacity_utilization:.1f}%")
        
        with col4:
            overload_days = sum(1 for d in demand_data['predicted_demand'] if d > avg_capacity)
            st.metric("Overload Days", f"{overload_days}/{len(demand_data['predicted_demand'])}")
        
        st.markdown("---")
    
    def _render_risk_assessment(self):
        """Render risk assessment section."""
        st.subheader("⚡ Risk Assessment")
        
        risk_data = self._generate_risk_assessment()
        
        # Risk radar chart
        categories = list(risk_data['categories'].keys())
        values = list(risk_data['categories'].values())
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=categories,
            fill='toself',
            name='Current Risk Level',
            line=dict(color=COLOR_SCHEMES["danger"])
        ))
        
        # Add target risk levels
        target_values = [max(0, v - 20) for v in values]  # Target 20% lower
        fig.add_trace(go.Scatterpolar(
            r=target_values,
            theta=categories,
            fill='toself',
            name='Target Risk Level',
            line=dict(color=COLOR_SCHEMES["success"], dash='dash'),
            opacity=0.6
        ))
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, 100]
                )
            ),
            title='Risk Assessment Radar',
            height=500
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Risk matrix
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Risk Priority Matrix:**")
            
            risk_matrix_data = risk_data['risk_matrix']
            risk_df = pd.DataFrame(risk_matrix_data)
            
            # Style the dataframe based on risk levels
            def color_risk(val):
                if isinstance(val, str):
                    return ''
                if val >= 80:
                    return 'background-color: #ffebee'
                elif val >= 60:
                    return 'background-color: #fff3e0'
                else:
                    return 'background-color: #e8f5e8'
            
            styled_df = risk_df.style.applymap(color_risk, subset=risk_df.select_dtypes(include=[np.number]).columns)
            st.dataframe(styled_df, use_container_width=True)
        
        with col2:
            st.markdown("**Risk Mitigation Actions:**")
            
            for action in risk_data['mitigation_actions']:
                priority_color = {"High": "🔴", "Medium": "🟡", "Low": "🟢"}
                st.write(f"{priority_color.get(action['priority'], '⚪')} **{action['action']}**")
                st.write(f"   Impact: {action['impact']}")
                st.write(f"   Timeline: {action['timeline']}")
        
        st.markdown("---")
    
    def _render_model_performance(self):
        """Render model performance section."""
        st.subheader("🎯 Model Performance")
        
        performance_data = self._get_model_performance_data()
        
        # Model comparison
        col1, col2 = st.columns(2)
        
        with col1:
            # Accuracy comparison
            fig = px.bar(
                performance_data,
                x='model_name',
                y='accuracy',
                title='Model Accuracy Comparison',
                color='accuracy',
                color_continuous_scale='RdYlGn'
            )
            fig.add_hline(y=85, line_dash="dash", line_color="red",
                         annotation_text="Minimum Threshold")
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Performance metrics
            metrics_data = performance_data[['model_name', 'precision', 'recall', 'f1_score']]
            
            fig = go.Figure()
            
            for metric in ['precision', 'recall', 'f1_score']:
                fig.add_trace(go.Bar(
                    name=metric.replace('_', ' ').title(),
                    x=metrics_data['model_name'],
                    y=metrics_data[metric]
                ))
            
            fig.update_layout(
                title='Model Performance Metrics',
                barmode='group',
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Detailed performance table
        st.markdown("**Detailed Model Performance:**")
        
        # Format the performance dataframe
        display_df = performance_data.copy()
        for col in ['accuracy', 'precision', 'recall', 'f1_score']:
            display_df[col] = display_df[col].apply(lambda x: f"{x:.2f}%")
        
        display_df['training_time'] = display_df['training_time'].apply(lambda x: f"{x:.1f}s")
        display_df['last_updated'] = pd.to_datetime(display_df['last_updated']).dt.strftime('%Y-%m-%d %H:%M')
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        st.markdown("---")
    
    def _render_actionable_insights(self):
        """Render actionable insights section."""
        st.subheader("💡 Actionable Insights")
        
        insights = [
            {
                "priority": "Critical",
                "title": "Equipment Maintenance Alert",
                "insight": "Predictive models indicate 89% probability of Equipment Line-3 failure within 5 days. Vibration patterns and temperature readings suggest bearing wear.",
                "action": "Schedule immediate maintenance inspection for Line-3 bearings. Prepare replacement parts and plan 4-hour maintenance window.",
                "impact": "Prevent $45K production loss and avoid 12-hour unplanned downtime",
                "confidence": 89,
                "timeline": "Immediate (24 hours)"
            },
            {
                "priority": "High",
                "title": "Quality Degradation Trend",
                "insight": "Quality scores predicted to drop 8% over next 14 days based on raw material quality trends and process drift patterns.",
                "action": "Adjust process parameters for temperature (+2°C) and pressure (+0.3 bar). Review supplier quality for Batch #2024-089.",
                "impact": "Prevent quality drop below 85% threshold, maintain customer satisfaction",
                "confidence": 76,
                "timeline": "This week"
            },
            {
                "priority": "High",
                "title": "Demand Spike Preparation",
                "insight": "Machine learning models predict 34% demand increase starting March 15th based on seasonal patterns and market indicators.",
                "action": "Increase production capacity by 25%. Schedule additional shifts and ensure raw material inventory covers 3-week buffer.",
                "impact": "Capture additional $230K revenue opportunity, avoid stockouts",
                "confidence": 82,
                "timeline": "2 weeks"
            },
            {
                "priority": "Medium",
                "title": "Process Optimization Opportunity",
                "insight": "Analysis reveals 12% efficiency gain possible by adjusting Line-1 cycle time during low-demand periods (10 PM - 6 AM).",
                "action": "Implement dynamic scheduling algorithm. Test overnight cycle time reduction of 8 seconds per unit during identified periods.",
                "impact": "Reduce energy costs by $18K annually, improve throughput",
                "confidence": 71,
                "timeline": "Next month"
            },
            {
                "priority": "Medium",
                "title": "Seasonal Quality Pattern",
                "insight": "Historical data and ML models show quality typically drops 3-5% during summer months due to ambient temperature effects.",
                "action": "Enhance cooling systems before June. Adjust process parameters proactively based on weather forecasts.",
                "impact": "Maintain consistent quality year-round, reduce summer rework costs",
                "confidence": 85,
                "timeline": "Before June"
            }
        ]
        
        priority_colors = {"Critical": "🔴", "High": "🟡", "Medium": "🟢", "Low": "⚪"}
        
        for i, insight in enumerate(insights):
            with st.expander(f"{priority_colors[insight['priority']]} {insight['title']} (Confidence: {insight['confidence']}%)", expanded=i<2):
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.markdown(f"**🔍 Insight:** {insight['insight']}")
                    st.markdown(f"**🎯 Recommended Action:** {insight['action']}")
                    st.markdown(f"**💰 Expected Impact:** {insight['impact']}")
                
                with col2:
                    st.markdown(f"**Priority:** {priority_colors[insight['priority']]} {insight['priority']}")
                    st.markdown(f"**Confidence:** {insight['confidence']}%")
                    st.markdown(f"**Timeline:** {insight['timeline']}")
                    
                    # Confidence gauge
                    confidence_color = "green" if insight['confidence'] > 80 else "orange" if insight['confidence'] > 65 else "red"
                    st.markdown(f"**Model Confidence**")
                    st.progress(insight['confidence'] / 100)
        
        # Export options
        st.markdown("---")
        prediction_summary = self._get_prediction_summary()
        render_export_options(
            data=prediction_summary,
            filename_prefix="predictive_insights"
        )
    
    # Helper methods for data generation
    def _get_prediction_summary(self) -> Dict[str, Any]:
        """Get prediction summary data."""
        return {
            "overall_accuracy": 87.3,
            "accuracy_improvement": 2.8,
            "active_models": 8,
            "predictions_today": 15420,
            "prediction_increase": 1250,
            "alerts_predicted": 3,
            "model_drift": 2.1,
            "next_retrain_days": 12
        }
    
    def _get_accuracy_trend_data(self) -> Dict[str, Dict]:
        """Generate accuracy trend data for different models."""
        dates = pd.date_range('2024-01-01', periods=30, freq='D')
        
        models = {
            'Quality Predictor': {
                'dates': dates,
                'accuracy': 85 + np.cumsum(np.random.normal(0.1, 1, 30))
            },
            'Defect Classifier': {
                'dates': dates,
                'accuracy': 82 + np.cumsum(np.random.normal(0.15, 1, 30))
            },
            'Failure Predictor': {
                'dates': dates,
                'accuracy': 88 + np.cumsum(np.random.normal(0.05, 0.8, 30))
            }
        }
        
        # Ensure accuracy stays within reasonable bounds
        for model in models.values():
            model['accuracy'] = np.clip(model['accuracy'], 70, 98)
        
        return models
    
    def _generate_quality_forecast(self) -> Dict[str, List]:
        """Generate quality score forecast data."""
        # Historical data (30 days)
        historical_dates = pd.date_range(end='2024-09-15', periods=30, freq='D')
        np.random.seed(42)
        historical_values = 0.85 + np.cumsum(np.random.normal(0, 0.005, 30))
        historical_values = np.clip(historical_values, 0.7, 0.95)
        
        # Forecast data
        forecast_dates = pd.date_range(start='2024-09-16', periods=self.forecast_horizon, freq='D')
        
        # Generate forecast with trend and seasonality
        trend = -0.001  # Slight declining trend
        seasonal = np.sin(np.arange(self.forecast_horizon) * 2 * np.pi / 7) * 0.01  # Weekly seasonality
        noise = np.random.normal(0, 0.008, self.forecast_horizon)
        
        last_value = historical_values[-1]
        forecast_values = []
        
        for i in range(self.forecast_horizon):
            value = last_value + trend * (i + 1) + seasonal[i] + noise[i]
            forecast_values.append(np.clip(value, 0.7, 0.95))
        
        # Confidence intervals
        uncertainty = np.linspace(0.02, 0.04, self.forecast_horizon)
        upper_bound = [v + u for v, u in zip(forecast_values, uncertainty)]
        lower_bound = [v - u for v, u in zip(forecast_values, uncertainty)]
        
        return {
            'historical_dates': historical_dates,
            'historical_values': historical_values,
            'forecast_dates': forecast_dates,
            'forecast_values': forecast_values,
            'upper_bound': upper_bound,
            'lower_bound': lower_bound
        }
    
    def _generate_defect_prediction(self) -> pd.DataFrame:
        """Generate defect rate prediction data."""
        dates = pd.date_range('2024-09-16', periods=self.forecast_horizon, freq='D')
        
        # Base defect rate with some variability
        base_rate = 2.5
        defect_rates = base_rate + np.random.normal(0, 0.5, self.forecast_horizon)
        defect_rates = np.clip(defect_rates, 0.5, 8.0)
        
        # Add risk levels
        risk_levels = ['Low' if r < 2 else 'Medium' if r < 4 else 'High' for r in defect_rates]
        
        return pd.DataFrame({
            'date': dates,
            'predicted_defect_rate': defect_rates,
            'risk_level': risk_levels,
            'historical_avg': [2.3] * len(dates)
        })
    
    def _get_defect_category_prediction(self) -> pd.DataFrame:
        """Generate defect category prediction data."""
        categories = ['Surface Defects', 'Dimensional', 'Material', 'Assembly', 'Packaging']
        
        return pd.DataFrame({
            'category': categories,
            'predicted_defects': [45, 32, 28, 15, 8],
            'risk_level': ['High', 'Medium', 'Medium', 'Low', 'Low']
        })
    
    def _generate_performance_forecast(self) -> pd.DataFrame:
        """Generate process performance forecast data."""
        dates = pd.date_range('2024-09-16', periods=self.forecast_horizon, freq='D')
        
        # Generate correlated performance metrics
        base_throughput = 1000
        base_efficiency = 85
        base_downtime = 2.5
        base_quality = 0.87
        
        throughput = base_throughput + np.cumsum(np.random.normal(2, 15, self.forecast_horizon))
        efficiency = base_efficiency + np.cumsum(np.random.normal(0.1, 1, self.forecast_horizon))
        downtime = base_downtime + np.cumsum(np.random.normal(-0.02, 0.3, self.forecast_horizon))
        quality = base_quality + np.cumsum(np.random.normal(0.001, 0.01, self.forecast_horizon))
        
        # Apply realistic bounds
        throughput = np.clip(throughput, 800, 1300)
        efficiency = np.clip(efficiency, 75, 95)
        downtime = np.clip(downtime, 0.5, 8)
        quality = np.clip(quality, 0.75, 0.95)
        
        return pd.DataFrame({
            'date': dates,
            'predicted_throughput': throughput,
            'predicted_efficiency': efficiency,
            'predicted_downtime': downtime,
            'predicted_quality': quality
        })
    
    def _generate_seasonal_analysis(self) -> Dict[str, pd.DataFrame]:
        """Generate seasonal analysis data."""
        # Monthly seasonality
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        monthly_quality = [1.05, 1.03, 1.08, 1.12, 1.15, 0.92, 
                          0.88, 0.85, 0.95, 1.08, 1.10, 1.02]
        
        # Weekly seasonality
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        weekly_quality = [0.98, 1.05, 1.08, 1.03, 0.95, 0.88, 0.92]
        
        return {
            'monthly': pd.DataFrame({'month': months, 'quality_index': monthly_quality}),
            'weekly': pd.DataFrame({'day_of_week': days, 'quality_index': weekly_quality})
        }
    
    def _generate_failure_predictions(self) -> Dict[str, Any]:
        """Generate failure prediction data."""
        # Risk matrix
        np.random.seed(42)
        risk_matrix = np.random.randint(20, 90, size=(4, 5))
        
        # High risk equipment
        high_risk_equipment = [
            {'equipment': 'Line 3 - Extruder', 'risk': 89, 'predicted_date': '2024-09-21', 'impact': 45000},
            {'equipment': 'Line 1 - Cutter', 'risk': 72, 'predicted_date': '2024-09-28', 'impact': 28000},
            {'equipment': 'Line 2 - Heater', 'risk': 68, 'predicted_date': '2024-10-05', 'impact': 32000},
            {'equipment': 'Line 4 - Conveyor', 'risk': 61, 'predicted_date': '2024-10-12', 'impact': 18000}
        ]
        
        # Timeline data
        dates = pd.date_range('2024-09-16', periods=30, freq='D')
        timeline = {}
        
        equipment_list = ['Line 1', 'Line 2', 'Line 3', 'Line 4']
        for equipment in equipment_list:
            risk_scores = 30 + np.cumsum(np.random.normal(1, 3, 30))
            risk_scores = np.clip(risk_scores, 10, 95)
            
            timeline[equipment] = {
                'dates': dates,
                'risk_scores': risk_scores
            }
        
        return {
            'risk_matrix': risk_matrix,
            'high_risk_equipment': high_risk_equipment,
            'timeline': timeline
        }
    
    def _generate_trend_analysis(self) -> Dict[str, Dict]:
        """Generate trend analysis data for different timeframes."""
        # Short-term (7 days)
        short_dates = pd.date_range('2024-09-10', periods=7, freq='D')
        short_values = 0.85 + np.cumsum(np.random.normal(0.002, 0.01, 7))
        
        # Medium-term (30 days)
        medium_dates = pd.date_range('2024-08-17', periods=30, freq='D')
        medium_values = 0.83 + np.cumsum(np.random.normal(0.001, 0.008, 30))
        
        # Long-term (90 days)
        long_dates = pd.date_range('2024-06-18', periods=90, freq='D')
        long_values = 0.80 + np.cumsum(np.random.normal(0.0008, 0.006, 90))
        
        # Annual pattern
        annual_months = list(range(1, 13))
        annual_values = [0.85, 0.83, 0.88, 0.92, 0.95, 0.82, 0.78, 0.75, 0.85, 0.88, 0.90, 0.87]
        
        return {
            'short_term': {'x': short_dates, 'y': short_values},
            'medium_term': {'x': medium_dates, 'y': medium_values},
            'long_term': {'x': long_dates, 'y': long_values},
            'annual': {'x': annual_months, 'y': annual_values}
        }
    
    def _generate_demand_forecast(self) -> Dict[str, List]:
        """Generate demand forecast data."""
        # Historical demand (30 days)
        historical_dates = pd.date_range(end='2024-09-15', periods=30, freq='D')
        historical_demand = 1000 + np.cumsum(np.random.normal(5, 25, 30))
        
        # Forecast demand
        forecast_dates = pd.date_range(start='2024-09-16', periods=self.forecast_horizon, freq='D')
        forecast_trend = np.linspace(0, 200, self.forecast_horizon)  # Growing trend
        seasonal = 100 * np.sin(np.arange(self.forecast_horizon) * 2 * np.pi / 7)  # Weekly pattern
        noise = np.random.normal(0, 30, self.forecast_horizon)
        
        last_demand = historical_demand[-1]
        predicted_demand = last_demand + forecast_trend + seasonal + noise
        predicted_demand = np.clip(predicted_demand, 500, 2000)
        
        # Capacity
        capacity = [1200] * (len(historical_dates) + len(forecast_dates))
        
        return {
            'historical_dates': historical_dates,
            'historical_demand': historical_demand,
            'forecast_dates': forecast_dates,
            'predicted_demand': predicted_demand,
            'capacity': capacity
        }
    
    def _generate_risk_assessment(self) -> Dict[str, Any]:
        """Generate risk assessment data."""
        categories = {
            'Equipment Failure': 65,
            'Quality Issues': 45,
            'Supply Chain': 55,
            'Demand Volatility': 40,
            'Regulatory': 25,
            'Cyber Security': 35,
            'Environmental': 30
        }
        
        # Risk matrix
        risk_matrix = {
            'Risk Factor': ['Equipment Failure', 'Quality Issues', 'Supply Chain', 'Demand Volatility'],
            'Probability': [65, 45, 55, 40],
            'Impact': [85, 70, 60, 50],
            'Risk Score': [55, 32, 33, 20]
        }
        
        # Mitigation actions
        mitigation_actions = [
            {
                'action': 'Implement Predictive Maintenance',
                'priority': 'High',
                'impact': 'Reduce equipment failure risk by 40%',
                'timeline': '3 months'
            },
            {
                'action': 'Enhance Quality Monitoring',
                'priority': 'Medium',
                'impact': 'Improve early defect detection by 60%',
                'timeline': '2 months'
            },
            {
                'action': 'Diversify Supplier Base',
                'priority': 'Medium',
                'impact': 'Reduce supply chain risk by 30%',
                'timeline': '6 months'
            }
        ]
        
        return {
            'categories': categories,
            'risk_matrix': risk_matrix,
            'mitigation_actions': mitigation_actions
        }
    
    def _get_model_performance_data(self) -> pd.DataFrame:
        """Generate model performance data."""
        return pd.DataFrame({
            'model_name': [
                'Quality Predictor',
                'Defect Classifier', 
                'Failure Predictor',
                'Demand Forecaster',
                'Process Optimizer'
            ],
            'accuracy': [87.3, 84.1, 89.7, 82.5, 85.9],
            'precision': [88.1, 86.3, 91.2, 80.7, 84.5],
            'recall': [85.9, 82.8, 87.4, 84.1, 87.2],
            'f1_score': [87.0, 84.5, 89.3, 82.4, 85.8],
            'training_time': [145.3, 89.7, 203.1, 167.8, 134.2],
            'last_updated': [
                '2024-09-15 14:30:00',
                '2024-09-14 09:15:00',
                '2024-09-15 11:45:00',
                '2024-09-13 16:20:00',
                '2024-09-15 08:30:00'
            ]
        })

def render_predictive_insights_page():
    """Render the Predictive Insights page."""
    page = PredictiveInsightsPage()
    page.render()