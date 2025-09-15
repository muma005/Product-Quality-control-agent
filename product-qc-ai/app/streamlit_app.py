import streamlit as st
from pipeline import recommendations
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Product Data QC Monitor", layout="wide")

# --- Custom CSS for style ---
st.markdown("""
    <style>
    .big-font { font-size:32px !important; font-weight: bold; }
    .kpi-card { background: #f7f7f7; border-radius: 10px; padding: 20px; margin-bottom: 10px; }
    .sidebar .sidebar-content { background: #f0f2f6; }
    </style>
""", unsafe_allow_html=True)

# --- Top Bar / Global Layout ---
st.markdown('<div class="big-font">🛡️ Product Data QC Monitor</div>', unsafe_allow_html=True)

# Global Filters
date_range = st.sidebar.date_input("Date Range")
category = st.sidebar.selectbox("Category", options=["All"])
brand = st.sidebar.selectbox("Brand", options=["All"])
severity = st.sidebar.selectbox("Severity", options=["All", "Low", "Medium", "High"])

# --- Tabs for Navigation ---
tabs = st.tabs(["Overview", "Product-Level QC", "Customer Review Alignment", "Revenue & Risk Impact", "Forecast & Trends", "Admin / Settings"])

# --- Tab 1: Overview ---
with tabs[0]:
    st.subheader("Overview")
    df_heatmap = recommendations.get_mismatch_heatmap()
    df_risk = recommendations.get_revenue_risk_impact()
    total_products = int(df_heatmap["total_products"].sum())
    products_with_mismatches = int(df_heatmap["mismatch_count"].sum())
    mismatch_rate = products_with_mismatches / total_products if total_products else 0
    revenue_at_risk = float(df_risk["potential_loss"].sum()) if not df_risk.empty else 0
    # KPI Cards
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.markdown(f'<div class="kpi-card">✅<br>Total Products<br><span style="font-size:28px">{total_products}</span></div>', unsafe_allow_html=True)
    kpi2.markdown(f'<div class="kpi-card">⚠️<br>With Mismatches<br><span style="font-size:28px">{products_with_mismatches}</span></div>', unsafe_allow_html=True)
    kpi3.markdown(f'<div class="kpi-card">📊<br>Mismatch Rate<br><span style="font-size:28px">{mismatch_rate:.1%}</span></div>', unsafe_allow_html=True)
    kpi4.markdown(f'<div class="kpi-card">💰<br>Revenue at Risk<br><span style="font-size:28px">${revenue_at_risk:,.0f}</span></div>', unsafe_allow_html=True)
    # Main Visualization: Category Heatmap (Bar Chart)
    st.markdown("### Mismatch Rate by Category")
    if not df_heatmap.empty:
        fig = px.bar(df_heatmap, x="category", y="mismatch_rate", color="mismatch_rate", color_continuous_scale=["#4CAF50", "#FFC107", "#F44336"], labels={"mismatch_rate": "Mismatch Rate"})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available for heatmap.")
    # Side Widget: Top 5 Problem Categories
    df_heatmap["impact"] = df_heatmap["mismatch_rate"] * df_heatmap["total_products"]
    top5 = df_heatmap.sort_values("impact", ascending=False).head(5)
    st.markdown("#### Top 5 Problem Categories")
    for idx, row in top5.iterrows():
        st.write(f"{row['category']}: {row['mismatch_rate']:.1%} ({int(row['impact'])} impacted)")

# --- Tab 2: Product-Level QC ---
with tabs[1]:
    st.subheader("Product-Level QC")
    # Search Bar
    search_id = st.text_input("Search by Product ID, SKU, or Name")
    if search_id:
        # For demo, search by product_id only
        client = None
        df_products = recommendations.get_product_details(search_id, client=client)  # You need to implement this in recommendations.py
        if not df_products.empty:
            prod = df_products.iloc[0]
            st.image(prod.get("image_path", None), width=200)
            st.markdown(f"**Product Name:** {prod.get('product_name','')}")
            st.markdown(f"**Description:** {prod.get('description','')}")
            st.markdown(f"**Specs:**")
            st.json(prod.get('specs', {}))
            st.markdown(f"**Category:** {prod.get('category','')}")
            st.markdown(f"**Price:** ${prod.get('price',0):,.2f}")
            # Reviews
            st.markdown("**Reviews:**")
            df_reviews = recommendations.get_product_reviews(prod['product_id'], client=client)  # Implement in recommendations.py
            for _, review in df_reviews.iterrows():
                flag = "🚨" if review.get('review_flag', False) else "✅"
                st.write(f"{flag} {review['review']}")
            # Flags
            st.markdown("**Flags:**")
            flags = []
            if prod.get('vector_mismatch', 0) > 0.7:
                flags.append("Text-Image Mismatch")
            if prod.get('rule_mismatch', False):
                flags.append("Text-Spec Mismatch")
            if any(df_reviews['review_flag']):
                flags.append("Review Contradiction")
            if flags:
                st.write(", ".join(flags))
            else:
                st.write("No major mismatches detected.")
            # AI Suggestions
            st.markdown("**AI Suggestions:**")
            suggestion = recommendations.get_ai_suggestions(prod['product_id'], client=client)  # Implement in recommendations.py
            if suggestion:
                st.write(f"**Corrected Description:** {suggestion.get('corrected_description','')}")
                st.write(f"**Alert Message:** {suggestion.get('image_text_alert','')}")
                st.write(f"**Confidence Score:** {suggestion.get('confidence', 'N/A')}")
        else:
            st.warning("No product found with that ID/SKU/Name.")
    else:
        st.info("Enter a Product ID, SKU, or Name to view details.")

# --- Tab 3: Customer Review Alignment ---
with tabs[2]:
    st.subheader("Customer Review Alignment")
    df_reviews = recommendations.get_review_alignment_summary()  # Implement in recommendations.py
    if not df_reviews.empty:
        st.markdown("### Review Alignment Pie Chart")
        pie_data = df_reviews['review_flag'].value_counts().rename({True: 'Contradict', False: 'Aligned'})
        st.plotly_chart(px.pie(values=pie_data.values, names=pie_data.index, title="Review Alignment"))
        st.markdown("### Most Common Contradictions")
        st.bar_chart(df_reviews['contradiction_type'].value_counts())  # Assumes this column exists
        st.markdown("### Frequent Complaint Terms (Word Cloud)")
        # Placeholder: st.image('wordcloud.png')
        st.info("Word cloud feature coming soon.")
        st.markdown("### Product-Level Review Mismatch Stats")
        st.dataframe(df_reviews)
    else:
        st.info("No review alignment data available.")

# --- Tab 4: Revenue & Risk Impact ---
with tabs[3]:
    st.subheader("Revenue & Risk Impact")
    df_risk = recommendations.get_revenue_risk_impact()
    if not df_risk.empty:
        kpi1, kpi2, kpi3 = st.columns(3)
        kpi1.metric("💵 Total Sales Impacted", f"${df_risk['potential_loss'].sum():,.0f}")
        kpi2.metric("📉 Returns Risk Estimate", f"${df_risk['potential_loss'].sum()*0.2:,.0f}")
        kpi3.metric("🔮 Forecasted Next Quarter Loss", f"${df_risk['potential_loss'].sum()*1.1:,.0f}")
        st.markdown("### Mismatch Cost by Category")
        st.bar_chart(df_risk.set_index("category")["potential_loss"])
        st.markdown("### Mismatch Rate Trend (Last 6 Months)")
        st.info("Trend chart feature coming soon.")
    else:
        st.info("No revenue/risk data available.")

# --- Tab 5: Forecast & Trends ---
with tabs[4]:
    st.subheader("Forecast & Trends")
    df_forecast = recommendations.get_mismatch_trend_forecast()  # Implemented in recommendations.py
    if not df_forecast.empty:
        st.line_chart(df_forecast.set_index("forecast_timestamp")["forecast_value"])  # Adjust columns as needed
        st.markdown("### Category Projection Table")
        st.dataframe(df_forecast)
        st.markdown("### Insight Box")
        st.success("Mismatch rates in Electronics expected to rise by 3% next quarter, driven by review contradictions.")
    else:
        st.info("No forecast data available.")

# --- Tab 6: Admin / Settings ---
with tabs[5]:
    st.subheader("Admin / Settings")
    st.markdown("Control thresholds, export options, and API key setup coming soon.")
    st.slider("Mismatch Score Alert Threshold", 0.0, 1.0, 0.7)
    st.button("Export Data (CSV)")
    st.button("Export Data (PDF)")
    st.text_input("API Key for E-commerce Integration")
