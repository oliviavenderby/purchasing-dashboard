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

        # Make two calls: one for NEW, one for USED
        url_base = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}/price"

        with st.spinner("Fetching NEW price data..."):
            response_new = requests.get(url_base + "?new_or_used=N", auth=auth)
        with st.spinner("Fetching USED price data..."):
            response_used = requests.get(url_base + "?new_or_used=U", auth=auth)

        if response_new.status_code == 200 and response_used.status_code == 200:
            new_data = response_new.json()["data"]
            used_data = response_used.json()["data"]

            st.subheader(f"💸 Price Guide for Set {set_number}")
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**🟢 New Condition**")
                avg_price_new = new_data.get("avg_price")
                if avg_price_new is not None:
                    st.metric("Avg Price (New)", f"${float(avg_price_new):.2f}")
                else:
                    st.warning("No average price (new) available.")          
                st.metric("Total Qty Sold (New)", new_data['total_quantity'])
                st.metric("Total Sales (New)", new_data['unit_quantity'])

            with col2:
                st.markdown("**🟠 Used Condition**")
                if avg_price_used is not None:
                    st.metric("Avg Price (Used)", f"${float['avg_price_used']:.2f}")
                else:
                    st.warning("No average price (used) available.")
                st.metric("Total Qty Sold (Used)", used_data['total_quantity'])
                st.metric("Total Sales (Used)", used_data['unit_quantity'])

        else:
            st.error("Failed to fetch data from BrickLink API.")
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

