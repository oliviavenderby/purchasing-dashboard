import streamlit as st
import requests
from requests_oauthlib import OAuth1
import pandas as pd

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
# 🚀 Button & API Logic
# -----------------------------
if st.button("Fetch Price Data"):
    if all([consumer_key, consumer_secret, token, token_secret, set_number]):
        auth = OAuth1(consumer_key, consumer_secret, token, token_secret)
        url_base = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}/price"

        # Fetch NEW and USED price data
        with st.spinner("Fetching NEW price data..."):
            response_new = requests.get(url_base + "?new_or_used=N", auth=auth)
        with st.spinner("Fetching USED price data..."):
            response_used = requests.get(url_base + "?new_or_used=U", auth=auth)

        # If both successful
        if response_new.status_code == 200 and response_used.status_code == 200:
            new_data = response_new.json().get("data", {})
            used_data = response_used.json().get("data", {})

            st.subheader(f"💸 Price Guide for Set {set_number}")

            # Build a DataFrame to compare NEW and USED data
            summary_data = pd.DataFrame({
                "Metric": [
                    "BL recent Avg Price",
                    "BL Number Sales",
                    "BL Total Qty Sold"
                ],
                "New": [
                    f"${new_data.get('avg_price', 'N/A')}",
                    new_data.get("unit_quantity", "N/A"),
                    new_data.get("total_quantity", "N/A")
                ],
                "Used": [
                    f"${used_data.get('avg_price', 'N/A')}",
                    used_data.get("unit_quantity", "N/A"),
                    used_data.get("total_quantity", "N/A")
                ]
            })

            st.dataframe(summary_data.set_index("Metric"))

            # Optionally expand raw data for debugging
            with st.expander("🔍 Raw API Data"):
                st.json({
                    "new_data": new_data,
                    "used_data": used_data
                })

        else:
            st.error("Failed to fetch one or both data types from BrickLink API.")
            st.write("New response code:", response_new.status_code)
            st.write("Used response code:", response_used.status_code)
    else:
        st.warning("Please fill in all API credentials and a valid set number.")

# -----------------------------
# 🧾 Footer
# -----------------------------
st.markdown("---")
st.caption("Powered by BrickLink API • Created by ReUseBricks")
