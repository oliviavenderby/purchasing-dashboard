import streamlit as st
import requests
from requests_oauthlib import OAuth1

# --- Sidebar: BrickLink Credentials ---
st.sidebar.header("🔑 BrickLink API Credentials")
consumer_key = st.sidebar.text_input("Consumer Key", type="password")
consumer_secret = st.sidebar.text_input("Consumer Secret", type="password")
token = st.sidebar.text_input("Token", type="password")
token_secret = st.sidebar.text_input("Token Secret", type="password")

# --- Main App ---
st.title("🧱 LEGO Set Price Lookup")
set_number = st.text_input("Enter LEGO Set Number (e.g., 10276):")

# --- Button with conditional logic ---
fetch_clicked = st.button("Fetch Price Data")

if fetch_clicked:
    if all([consumer_key, consumer_secret, token, token_secret, set_number]):
        auth = OAuth1(consumer_key, consumer_secret, token, token_secret)
        url = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}/price"

        with st.spinner("Fetching data from BrickLink..."):
            response = requests.get(url, auth=auth)

        if response.status_code == 200:
            data = response.json()["data"]

            st.subheader(f"💸 Price Guide for Set {set_number}")
            col1, col2 = st.columns(2)
            
            if "new" in data:
                with col1:
                    st.markdown("**🟢 New Condition**")
                    st.metric("Avg Price (New)", f"${data['new']['avg_price']:.2f}")
                    st.metric("Total Qty Sold (New)", data['new']['total_quantity'])
                    st.metric("Total Sales (New)", data['new']['unit_quantity'])
            else:
                with col1:
                    st.warning("No new condition data available.")
            
            if "used" in data:
                with col2:
                    st.markdown("**🟠 Used Condition**")
                    st.metric("Avg Price (Used)", f"${data['used']['avg_price']:.2f}")
                    st.metric("Total Qty Sold (Used)", data['used']['total_quantity'])
                    st.metric("Total Sales (Used)", data['used']['unit_quantity'])
            else:
                with col2:
                    st.warning("No used condition data available.")
        else:
            st.error(f"Failed to fetch data (status code: {response.status_code})")
    else:
        st.warning("Please fill in all API credentials and a set number.")

with st.expander("🔍 API Debug Info"):
    st.write("Status code:", response.status_code)
    try:
        st.json(response.json())
    except Exception as e:
        st.write("Failed to parse JSON:", e)


# Footer
st.markdown("---")
st.caption("Powered by BrickLink API • Streamlit App by ReUseBricks")

