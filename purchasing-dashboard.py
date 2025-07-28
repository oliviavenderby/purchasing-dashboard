import streamlit as st
import requests
from requests_oauthlib import OAuth1

# -----------------------------
# 🔐 Sidebar: API Credentials
# -----------------------------
st.sidebar.header("🔑 BrickLink API Credentials")
consumer_key = st.sidebar.text_input("Consumer Key", type="password")
consumer_secret = st.sidebar.text_input("Consumer Secret", type="password")
token = st.sidebar.text_input("Token", type="password")
token_secret = st.sidebar.text_input("Token Secret", type="password")

# -----------------------------
# 🧱 Main App Interface
# -----------------------------
st.title("🧱 LEGO Set Price Lookup")
set_number = st.text_input("Enter LEGO Set Number (e.g., 10276):")

# -----------------------------
# 🔁 Safe display helper
# -----------------------------
def safe_metric(label, value, suffix="$"):
    try:
        if value is not None:
            val = float(value)
            st.metric(label, f"{suffix}{val:,.2f}")
        else:
            st.warning(f"{label}: No data available.")
    except (ValueError, TypeError):
        st.warning(f"{label}: Invalid format.")

# -----------------------------
# 🚀 Button & API Logic
# -----------------------------
if st.button("Fetch Price Data"):
    if all([consumer_key, consumer_secret, token, token_secret, set_number]):
        auth = OAuth1(consumer_key, consumer_secret, token, token_secret)

        # Define base URL
        url_base = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}/price"

        # Fetch NEW data
        with st.spinner("Fetching NEW price data..."):
            response_new = requests.get(url_base + "?new_or_used=N", auth=auth)

        # Fetch USED data
        with st.spinner("Fetching USED price data..."):
            response_used = requests.get(url_base + "?new_or_used=U", auth=auth)

        # Handle both responses
        if response_new.status_code == 200 and response_used.status_code == 200:
            new_data = response_new.json().get("data", {})
            used_data = response_used.json().get("data", {})

            st.subheader(f"💸 Price Guide for Set {set_number}")
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**🟢 New Condition**")
                safe_metric("Avg Price (New)", new_data.get("avg_price"))
                safe_metric("Total Qty Sold (New)", new_data.get("total_quantity"), suffix="")
                safe_metric("Total Sales (New)", new_data.get("unit_quantity"), suffix="")

            with col2:
                st.markdown("**🟠 Used Condition**")
                safe_metric("Avg Price (Used)", used_data.get("avg_price"))
                safe_metric("Total Qty Sold (Used)", used_data.get("total_quantity"), suffix="")
                safe_metric("Total Sales (Used)", used_data.get("unit_quantity"), suffix="")

            with st.expander("🔍 API Debug Info"):
                st.write("Status (NEW):", response_new.status_code)
                st.write("Status (USED):", response_used.status_code)
                st.json({"new": new_data, "used": used_data})
        else:
            st.error("Failed to fetch one or both data types from BrickLink API.")
            st.write("New status code:", response_new.status_code)
            st.write("Used status code:", response_used.status_code)
    else:
        st.warning("Please fill in all API credentials and a valid set number.")

# -----------------------------
# 🧾 Footer
# -----------------------------
st.markdown("---")
st.caption("Powered by BrickLink API • Created by ReUseBricks")
