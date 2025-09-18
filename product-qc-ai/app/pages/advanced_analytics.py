"""Advanced Analytics page for deep dive analytics and insights."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import scipy.stats as stats
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans

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

class AdvancedAnalyticsPage:
    """Advanced Analytics page class."""
    
    def __init__(self):
        """Initialize Advanced Analytics page."""
        self.analytics_data = None
        self.statistical_models = {}
    
    def render(self):
        """Render the Advanced Analytics page."""
        render_header_section(
            title="Advanced Analytics",
            description="Deep dive analytics, statistical analysis, and AI-powered insights",
            icon="🔬"
        )
        
        # Time range selector
        start_date, end_date = render_time_range_selector("analytics")
        
        # Main analytics sections
        self._render_analytics_overview()
        self._render_statistical_analysis()
        self._render_correlation_analysis()
        self._render_trend_decomposition()
        self._render_anomaly_detection()
        self._render_predictive_modeling()
        self._render_clustering_analysis()
        self._render_advanced_insights()
    
    def _render_analytics_overview(self):
        """Render analytics overview section."""
        st.subheader("📊 Analytics Overview")
        
        # Key analytics metrics
        analytics_summary = self._get_analytics_summary()
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            create_metric_card(
                title="Data Points Analyzed",
                value=format_large_number(analytics_summary.get("total_data_points", 0)),
                help_text="Total number of data points in analysis"
            )
        
        with col2:
            create_metric_card(
                title="Statistical Confidence",
                value=f"{analytics_summary.get('confidence_level', 0):.1f}%",
                help_text="Statistical confidence level of analysis"
            )
        
        with col3:
            correlation_strength = analytics_summary.get('avg_correlation', 0)
            create_metric_card(
                title="Avg Correlation Strength",
                value=f"{correlation_strength:.3f}",
                delta_color="normal" if abs(correlation_strength) > 0.5 else "inverse",
                help_text="Average correlation strength across variables"
            )
        
        with col4:
            create_metric_card(
                title="Anomalies Detected",
                value=analytics_summary.get("anomalies_count", 0),
                delta_color="inverse" if analytics_summary.get("anomalies_count", 0) > 10 else "normal",
                help_text="Number of statistical anomalies detected"
            )
        
        with col5:
            create_metric_card(
                title="Model Accuracy",
                value=f"{analytics_summary.get('model_accuracy', 0):.2f}%",
                help_text="Accuracy of predictive models"
            )
        
        st.markdown("---")
    
    def _render_statistical_analysis(self):
        """Render statistical analysis section."""
        st.subheader("📈 Statistical Analysis")
        
        # Create tabs for different statistical analyses
        tab1, tab2, tab3, tab4 = st.tabs([
            "📊 Descriptive Stats", 
            "🔍 Distribution Analysis", 
            "📐 Hypothesis Testing",
            "🎯 Confidence Intervals"
        ])
        
        with tab1:
            self._render_descriptive_statistics()
        
        with tab2:
            self._render_distribution_analysis()
        
        with tab3:
            self._render_hypothesis_testing()
        
        with tab4:
            self._render_confidence_intervals()
        
        st.markdown("---")
    
    def _render_descriptive_statistics(self):
        """Render descriptive statistics."""
        st.markdown("### 📊 Descriptive Statistics")
        
        # Generate sample data for demonstration
        quality_data = self._get_quality_data_sample()
        
        if quality_data.empty:
            render_empty_state("No data available for statistical analysis")
            return
        
        # Calculate descriptive statistics
        desc_stats = quality_data.describe()
        
        # Display statistics table
        st.dataframe(
            desc_stats.style.format(precision=3),
            use_container_width=True
        )
        
        # Create distribution plots
        col1, col2 = st.columns(2)
        
        with col1:
            # Histogram
            fig = px.histogram(
                quality_data, 
                x='quality_score',
                nbins=30,
                title='Quality Score Distribution',
                color_discrete_sequence=[COLOR_SCHEMES["primary"]]
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Box plot
            fig = px.box(
                quality_data,
                y='quality_score',
                title='Quality Score Box Plot',
                color_discrete_sequence=[COLOR_SCHEMES["primary"]]
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    def _render_distribution_analysis(self):
        """Render distribution analysis."""
        st.markdown("### 🔍 Distribution Analysis")
        
        quality_data = self._get_quality_data_sample()
        
        if quality_data.empty:
            render_empty_state("No data available for distribution analysis")
            return
        
        # Distribution fitting
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Distribution Fitting Results:**")
            
            # Fit different distributions
            data = quality_data['quality_score'].dropna()
            
            distributions = ['norm', 'gamma', 'beta', 'lognorm']
            fit_results = {}
            
            for dist_name in distributions:
                try:
                    dist = getattr(stats, dist_name)
                    params = dist.fit(data)
                    
                    # Calculate goodness of fit (Kolmogorov-Smirnov test)
                    D, p_value = stats.kstest(data, dist.cdf, args=params)
                    
                    fit_results[dist_name] = {
                        'params': params,
                        'p_value': p_value,
                        'D_statistic': D
                    }
                except:
                    continue
            
            # Display results
            for dist_name, results in fit_results.items():
                st.write(f"**{dist_name.title()}**: p-value = {results['p_value']:.4f}")
        
        with col2:
            # Q-Q plot
            fig = go.Figure()
            
            # Generate Q-Q plot data
            sorted_data = np.sort(data)
            theoretical_quantiles = stats.norm.ppf(np.linspace(0.01, 0.99, len(sorted_data)))
            
            fig.add_trace(go.Scatter(
                x=theoretical_quantiles,
                y=sorted_data,
                mode='markers',
                name='Data Points',
                marker=dict(color=COLOR_SCHEMES["primary"])
            ))
            
            # Add reference line
            fig.add_trace(go.Scatter(
                x=theoretical_quantiles,
                y=theoretical_quantiles * np.std(sorted_data) + np.mean(sorted_data),
                mode='lines',
                name='Reference Line',
                line=dict(color=COLOR_SCHEMES["danger"], dash='dash')
            ))
            
            fig.update_layout(
                title='Q-Q Plot (Normal Distribution)',
                xaxis_title='Theoretical Quantiles',
                yaxis_title='Sample Quantiles',
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    def _render_hypothesis_testing(self):
        """Render hypothesis testing results."""
        st.markdown("### 📐 Hypothesis Testing")
        
        # Sample hypothesis tests
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Quality Score vs Target (μ = 0.85)**")
            
            # One-sample t-test
            quality_data = self._get_quality_data_sample()
            if not quality_data.empty:
                target_mean = 0.85
                sample_data = quality_data['quality_score'].dropna()
                
                t_stat, p_value = stats.ttest_1samp(sample_data, target_mean)
                
                st.write(f"Sample Mean: {sample_data.mean():.4f}")
                st.write(f"Target Mean: {target_mean}")
                st.write(f"t-statistic: {t_stat:.4f}")
                st.write(f"p-value: {p_value:.4f}")
                
                if p_value < 0.05:
                    st.error("❌ Reject null hypothesis - Significant difference from target")
                else:
                    st.success("✅ Fail to reject null hypothesis - No significant difference")
        
        with col2:
            st.markdown("**Variance Test (σ² = 0.01)**")
            
            if not quality_data.empty:
                target_var = 0.01
                sample_var = sample_data.var()
                
                # Chi-square test for variance
                chi2_stat = (len(sample_data) - 1) * sample_var / target_var
                p_value_var = 1 - stats.chi2.cdf(chi2_stat, len(sample_data) - 1)
                
                st.write(f"Sample Variance: {sample_var:.6f}")
                st.write(f"Target Variance: {target_var}")
                st.write(f"χ² statistic: {chi2_stat:.4f}")
                st.write(f"p-value: {p_value_var:.4f}")
                
                if p_value_var < 0.05:
                    st.error("❌ Reject null hypothesis - Variance significantly different")
                else:
                    st.success("✅ Fail to reject null hypothesis - Variance within expected range")
    
    def _render_confidence_intervals(self):
        """Render confidence intervals."""
        st.markdown("### 🎯 Confidence Intervals")
        
        quality_data = self._get_quality_data_sample()
        
        if quality_data.empty:
            render_empty_state("No data available for confidence interval analysis")
            return
        
        sample_data = quality_data['quality_score'].dropna()
        
        # Calculate confidence intervals for different confidence levels
        confidence_levels = [0.90, 0.95, 0.99]
        
        ci_results = []
        for conf_level in confidence_levels:
            alpha = 1 - conf_level
            mean = sample_data.mean()
            std_err = stats.sem(sample_data)
            
            # t-distribution critical value
            df = len(sample_data) - 1
            t_critical = stats.t.ppf(1 - alpha/2, df)
            
            margin_error = t_critical * std_err
            ci_lower = mean - margin_error
            ci_upper = mean + margin_error
            
            ci_results.append({
                'Confidence Level': f"{conf_level*100:.0f}%",
                'Lower Bound': f"{ci_lower:.4f}",
                'Mean': f"{mean:.4f}",
                'Upper Bound': f"{ci_upper:.4f}",
                'Margin of Error': f"{margin_error:.4f}"
            })
        
        # Display confidence intervals table
        ci_df = pd.DataFrame(ci_results)
        st.dataframe(ci_df, use_container_width=True)
        
        # Visualization of confidence intervals
        fig = go.Figure()
        
        for i, result in enumerate(ci_results):
            conf_level = confidence_levels[i]
            lower = float(result['Lower Bound'])
            upper = float(result['Upper Bound'])
            mean = float(result['Mean'])
            
            fig.add_trace(go.Scatter(
                x=[lower, upper],
                y=[conf_level*100, conf_level*100],
                mode='lines+markers',
                name=f"{conf_level*100:.0f}% CI",
                line=dict(width=4),
                marker=dict(size=8)
            ))
            
            # Add mean point
            fig.add_trace(go.Scatter(
                x=[mean],
                y=[conf_level*100],
                mode='markers',
                name=f"Mean ({conf_level*100:.0f}%)",
                marker=dict(symbol='diamond', size=10, color='red'),
                showlegend=False
            ))
        
        fig.update_layout(
            title='Confidence Intervals for Quality Score Mean',
            xaxis_title='Quality Score',
            yaxis_title='Confidence Level (%)',
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_correlation_analysis(self):
        """Render correlation analysis section."""
        st.subheader("🔗 Correlation Analysis")
        
        # Generate correlation data
        correlation_data = self._get_correlation_data()
        
        if correlation_data.empty:
            render_empty_state("No data available for correlation analysis")
            return
        
        # Calculate correlation matrix
        correlation_matrix = correlation_data.corr()
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Correlation heatmap
            fig = px.imshow(
                correlation_matrix,
                text_auto=True,
                aspect="auto",
                title="Correlation Matrix Heatmap",
                color_continuous_scale="RdBu"
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Strongest correlations
            st.markdown("**Strongest Correlations:**")
            
            # Get upper triangle of correlation matrix
            mask = np.triu(np.ones_like(correlation_matrix, dtype=bool), k=1)
            correlation_pairs = correlation_matrix.where(mask).stack().reset_index()
            correlation_pairs.columns = ['Variable 1', 'Variable 2', 'Correlation']
            correlation_pairs = correlation_pairs.sort_values('Correlation', key=abs, ascending=False)
            
            # Display top correlations
            for _, row in correlation_pairs.head(8).iterrows():
                corr_value = row['Correlation']
                color = "🔴" if abs(corr_value) > 0.7 else "🟡" if abs(corr_value) > 0.5 else "🟢"
                st.write(f"{color} **{row['Variable 1']}** ↔ **{row['Variable 2']}**: {corr_value:.3f}")
        
        st.markdown("---")
    
    def _render_trend_decomposition(self):
        """Render trend decomposition analysis."""
        st.subheader("📈 Trend Decomposition")
        
        # Generate time series data
        ts_data = self._get_time_series_data()
        
        if ts_data.empty:
            render_empty_state("No time series data available for decomposition")
            return
        
        # Perform trend decomposition (simplified version)
        from scipy import signal
        
        # Extract trend using Savitzky-Golay filter
        trend = signal.savgol_filter(ts_data['value'], window_length=31, polyorder=3)
        
        # Calculate seasonal component (simplified monthly pattern)
        seasonal = np.sin(2 * np.pi * np.arange(len(ts_data)) / 30) * 0.1
        
        # Calculate residual
        residual = ts_data['value'] - trend - seasonal
        
        # Create subplots
        fig = make_subplots(
            rows=4, cols=1,
            subplot_titles=['Original', 'Trend', 'Seasonal', 'Residual'],
            vertical_spacing=0.08
        )
        
        # Original data
        fig.add_trace(
            go.Scatter(x=ts_data['date'], y=ts_data['value'], name='Original', line=dict(color='blue')),
            row=1, col=1
        )
        
        # Trend
        fig.add_trace(
            go.Scatter(x=ts_data['date'], y=trend, name='Trend', line=dict(color='red')),
            row=2, col=1
        )
        
        # Seasonal
        fig.add_trace(
            go.Scatter(x=ts_data['date'], y=seasonal, name='Seasonal', line=dict(color='green')),
            row=3, col=1
        )
        
        # Residual
        fig.add_trace(
            go.Scatter(x=ts_data['date'], y=residual, name='Residual', line=dict(color='orange')),
            row=4, col=1
        )
        
        fig.update_layout(height=800, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
    
    def _render_anomaly_detection(self):
        """Render anomaly detection section."""
        st.subheader("🚨 Anomaly Detection")
        
        # Generate data with anomalies
        anomaly_data = self._get_anomaly_data()
        
        if anomaly_data.empty:
            render_empty_state("No data available for anomaly detection")
            return
        
        # Statistical anomaly detection using Z-score
        z_scores = np.abs(stats.zscore(anomaly_data['value']))
        threshold = st.slider("Z-Score Threshold", 1.0, 4.0, 2.5, 0.1)
        anomalies = z_scores > threshold
        
        # Create anomaly detection plot
        fig = go.Figure()
        
        # Normal points
        fig.add_trace(go.Scatter(
            x=anomaly_data.loc[~anomalies, 'timestamp'],
            y=anomaly_data.loc[~anomalies, 'value'],
            mode='markers',
            name='Normal Points',
            marker=dict(color=COLOR_SCHEMES["primary"], size=6)
        ))
        
        # Anomalous points
        if anomalies.any():
            fig.add_trace(go.Scatter(
                x=anomaly_data.loc[anomalies, 'timestamp'],
                y=anomaly_data.loc[anomalies, 'value'],
                mode='markers',
                name='Anomalies',
                marker=dict(color=COLOR_SCHEMES["danger"], size=10, symbol='x')
            ))
        
        # Add control limits
        mean_val = anomaly_data['value'].mean()
        std_val = anomaly_data['value'].std()
        
        fig.add_hline(y=mean_val + threshold * std_val, line_dash="dash", 
                     line_color="red", annotation_text="Upper Control Limit")
        fig.add_hline(y=mean_val - threshold * std_val, line_dash="dash", 
                     line_color="red", annotation_text="Lower Control Limit")
        fig.add_hline(y=mean_val, line_dash="dot", line_color="gray", 
                     annotation_text="Mean")
        
        fig.update_layout(
            title=f'Anomaly Detection (Z-Score > {threshold})',
            xaxis_title='Time',
            yaxis_title='Value',
            height=500
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Anomaly summary
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Anomalies", anomalies.sum())
        
        with col2:
            st.metric("Anomaly Rate", f"{(anomalies.sum() / len(anomaly_data) * 100):.2f}%")
        
        with col3:
            if anomalies.any():
                latest_anomaly = anomaly_data.loc[anomalies, 'timestamp'].max()
                st.metric("Latest Anomaly", latest_anomaly.strftime("%Y-%m-%d %H:%M"))
        
        st.markdown("---")
    
    def _render_predictive_modeling(self):
        """Render predictive modeling section."""
        st.subheader("🔮 Predictive Modeling")
        
        st.info("🚧 Advanced predictive models are being enhanced. Current implementation shows regression analysis.")
        
        # Generate sample data for modeling
        modeling_data = self._get_modeling_data()
        
        if modeling_data.empty:
            render_empty_state("No data available for predictive modeling")
            return
        
        # Simple linear regression example
        from sklearn.linear_model import LinearRegression
        from sklearn.metrics import r2_score, mean_squared_error
        
        X = modeling_data[['feature_1', 'feature_2']].values
        y = modeling_data['target'].values
        
        # Fit model
        model = LinearRegression()
        model.fit(X, y)
        y_pred = model.predict(X)
        
        # Model performance
        r2 = r2_score(y, y_pred)
        rmse = np.sqrt(mean_squared_error(y, y_pred))
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Model Performance:**")
            st.write(f"R² Score: {r2:.4f}")
            st.write(f"RMSE: {rmse:.4f}")
            st.write(f"Intercept: {model.intercept_:.4f}")
            st.write(f"Coefficients: {model.coef_}")
        
        with col2:
            # Prediction vs Actual plot
            fig = px.scatter(
                x=y, y=y_pred,
                title='Predicted vs Actual Values',
                labels={'x': 'Actual', 'y': 'Predicted'}
            )
            
            # Add perfect prediction line
            min_val, max_val = min(y.min(), y_pred.min()), max(y.max(), y_pred.max())
            fig.add_trace(go.Scatter(
                x=[min_val, max_val],
                y=[min_val, max_val],
                mode='lines',
                name='Perfect Prediction',
                line=dict(color='red', dash='dash')
            ))
            
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
    
    def _render_clustering_analysis(self):
        """Render clustering analysis section."""
        st.subheader("🎯 Clustering Analysis")
        
        # Generate clustering data
        cluster_data = self._get_clustering_data()
        
        if cluster_data.empty:
            render_empty_state("No data available for clustering analysis")
            return
        
        # Perform K-means clustering
        n_clusters = st.slider("Number of Clusters", 2, 8, 4)
        
        features = ['feature_1', 'feature_2', 'feature_3']
        X = cluster_data[features].values
        
        # Standardize features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # K-means clustering
        kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        cluster_labels = kmeans.fit_predict(X_scaled)
        
        # PCA for visualization
        pca = PCA(n_components=2)
        X_pca = pca.fit_transform(X_scaled)
        
        # Create clustering plot
        fig = px.scatter(
            x=X_pca[:, 0], y=X_pca[:, 1],
            color=cluster_labels,
            title=f'K-Means Clustering (k={n_clusters})',
            labels={'x': f'PC1 ({pca.explained_variance_ratio_[0]:.2%} variance)', 
                   'y': f'PC2 ({pca.explained_variance_ratio_[1]:.2%} variance)'}
        )
        
        # Add cluster centers
        centers_pca = pca.transform(kmeans.cluster_centers_)
        fig.add_trace(go.Scatter(
            x=centers_pca[:, 0],
            y=centers_pca[:, 1],
            mode='markers',
            marker=dict(symbol='x', size=15, color='black', line=dict(width=2)),
            name='Centroids'
        ))
        
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        
        # Cluster summary
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Cluster Summary:**")
            cluster_summary = pd.DataFrame({
                'Cluster': range(n_clusters),
                'Size': [np.sum(cluster_labels == i) for i in range(n_clusters)],
                'Percentage': [np.sum(cluster_labels == i) / len(cluster_labels) * 100 
                              for i in range(n_clusters)]
            })
            st.dataframe(cluster_summary, use_container_width=True)
        
        with col2:
            # Elbow method for optimal k
            inertias = []
            k_range = range(1, 11)
            
            for k in k_range:
                kmeans_temp = KMeans(n_clusters=k, random_state=42)
                kmeans_temp.fit(X_scaled)
                inertias.append(kmeans_temp.inertia_)
            
            fig_elbow = px.line(
                x=k_range, y=inertias,
                title='Elbow Method for Optimal k',
                labels={'x': 'Number of Clusters', 'y': 'Inertia'}
            )
            fig_elbow.add_vline(x=n_clusters, line_dash="dash", line_color="red")
            fig_elbow.update_layout(height=300)
            st.plotly_chart(fig_elbow, use_container_width=True)
        
        st.markdown("---")
    
    def _render_advanced_insights(self):
        """Render advanced insights section."""
        st.subheader("💡 Advanced Insights")
        
        # Key insights from analysis
        insights = [
            {
                "title": "Quality Score Distribution",
                "insight": "Quality scores follow a near-normal distribution with slight positive skew, indicating consistent high-quality output with occasional exceptional performance.",
                "recommendation": "Investigate factors contributing to exceptional performance to replicate across all production lines.",
                "impact": "High"
            },
            {
                "title": "Seasonal Patterns",
                "insight": "Quality metrics show cyclical patterns correlating with production schedules and maintenance cycles.",
                "recommendation": "Optimize maintenance scheduling to minimize impact on quality during peak production periods.",
                "impact": "Medium"
            },
            {
                "title": "Process Correlation",
                "insight": "Strong positive correlation (r=0.78) between temperature control and final quality scores.",
                "recommendation": "Implement enhanced temperature monitoring and control systems for quality improvement.",
                "impact": "High"
            },
            {
                "title": "Anomaly Clustering",
                "insight": "Detected anomalies cluster around specific production lines and time periods, suggesting systematic issues.",
                "recommendation": "Conduct detailed investigation of Line C during shift changes (3-4 PM window).",
                "impact": "Critical"
            }
        ]
        
        for i, insight in enumerate(insights):
            with st.expander(f"💡 Insight {i+1}: {insight['title']}", expanded=i==0):
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.markdown(f"**Analysis:** {insight['insight']}")
                    st.markdown(f"**Recommendation:** {insight['recommendation']}")
                
                with col2:
                    impact_color = {
                        "Critical": "🔴",
                        "High": "🟡", 
                        "Medium": "🟢",
                        "Low": "⚪"
                    }
                    st.markdown(f"**Impact Level**  \n{impact_color.get(insight['impact'], '⚪')} {insight['impact']}")
        
        # Export options
        st.markdown("---")
        analytics_summary = self._get_analytics_summary()
        render_export_options(
            data=analytics_summary,
            filename_prefix="advanced_analytics"
        )
    
    # Helper methods for data generation
    def _get_analytics_summary(self) -> Dict[str, Any]:
        """Get analytics summary data."""
        return {
            "total_data_points": 125000,
            "confidence_level": 95.5,
            "avg_correlation": 0.342,
            "anomalies_count": 23,
            "model_accuracy": 87.6
        }
    
    def _get_quality_data_sample(self) -> pd.DataFrame:
        """Generate sample quality data."""
        np.random.seed(42)
        n_samples = 1000
        
        return pd.DataFrame({
            'quality_score': np.random.beta(2, 0.5, n_samples) * 0.4 + 0.6,
            'temperature': np.random.normal(75, 5, n_samples),
            'pressure': np.random.normal(1013, 15, n_samples),
            'humidity': np.random.normal(45, 8, n_samples)
        })
    
    def _get_correlation_data(self) -> pd.DataFrame:
        """Generate correlation analysis data."""
        np.random.seed(42)
        n_samples = 500
        
        # Create correlated variables
        base = np.random.normal(0, 1, n_samples)
        
        return pd.DataFrame({
            'Quality_Score': base * 0.8 + np.random.normal(0, 0.6, n_samples),
            'Temperature': base * 0.7 + np.random.normal(0, 0.7, n_samples),
            'Pressure': base * 0.5 + np.random.normal(0, 0.9, n_samples),
            'Humidity': base * -0.4 + np.random.normal(0, 0.9, n_samples),
            'Processing_Speed': base * 0.6 + np.random.normal(0, 0.8, n_samples),
            'Defect_Rate': base * -0.75 + np.random.normal(0, 0.7, n_samples)
        })
    
    def _get_time_series_data(self) -> pd.DataFrame:
        """Generate time series data for decomposition."""
        dates = pd.date_range('2023-01-01', periods=365, freq='D')
        
        # Create time series with trend, seasonality, and noise
        trend = np.linspace(0.7, 0.9, len(dates))
        seasonal = 0.1 * np.sin(2 * np.pi * np.arange(len(dates)) / 30)
        noise = np.random.normal(0, 0.05, len(dates))
        
        return pd.DataFrame({
            'date': dates,
            'value': trend + seasonal + noise
        })
    
    def _get_anomaly_data(self) -> pd.DataFrame:
        """Generate data with anomalies."""
        np.random.seed(42)
        n_samples = 1000
        
        # Normal data
        normal_data = np.random.normal(0.85, 0.1, n_samples)
        
        # Inject anomalies
        anomaly_indices = np.random.choice(n_samples, size=25, replace=False)
        normal_data[anomaly_indices] = np.random.choice([0.3, 1.2], size=25)
        
        return pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=n_samples, freq='H'),
            'value': normal_data
        })
    
    def _get_modeling_data(self) -> pd.DataFrame:
        """Generate data for predictive modeling."""
        np.random.seed(42)
        n_samples = 300
        
        feature_1 = np.random.normal(0, 1, n_samples)
        feature_2 = np.random.normal(0, 1, n_samples)
        
        # Create target with some relationship to features
        target = 2 * feature_1 + 1.5 * feature_2 + np.random.normal(0, 0.5, n_samples)
        
        return pd.DataFrame({
            'feature_1': feature_1,
            'feature_2': feature_2,
            'target': target
        })
    
    def _get_clustering_data(self) -> pd.DataFrame:
        """Generate data for clustering analysis."""
        np.random.seed(42)
        n_samples = 300
        
        return pd.DataFrame({
            'feature_1': np.random.normal(0, 1, n_samples),
            'feature_2': np.random.normal(0, 1, n_samples),
            'feature_3': np.random.normal(0, 1, n_samples),
            'quality_metric': np.random.beta(2, 2, n_samples)
        })

def render_advanced_analytics_page():
    """Render the Advanced Analytics page."""
    page = AdvancedAnalyticsPage()
    page.render()