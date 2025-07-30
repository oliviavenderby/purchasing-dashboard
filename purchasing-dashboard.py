import streamlit as st
import requests
from requests_oauthlib import OAuth1
import pandas as pd

# -----------------------------
# Sidebar: API Credentials
# -----------------------------
st.sidebar.header("BrickLink API Credentials")
consumer_key = st.sidebar.text_input("Consumer Key", type="password")
consumer_secret = st.sidebar.text_input("Consumer Secret", type="password")
token = st.sidebar.text_input("Token", type="password")
token_secret = st.sidebar.text_input("Token Secret", type="password")

with st.sidebar.expander("Show Current IP Address"):
    try:
        ip = requests.get("https://api.ipify.org").text
        st.code(ip, language="text")
        st.caption("Use this IP address to register your BrickLink API access.")
    except:
        st.error("Unable to fetch IP address.")

# -----------------------------
# CSS: Align content to left

st.markdown("""
    <style>
        .block-container {
            padding-top: 2rem;
            padding-left: 2rem;
            padding-right: 2rem;
            max-width: 100%;
        }
    </style>
""", unsafe_allow_html=True)


# -----------------------------
# Main App Interface
# -----------------------------
st.title("LEGO Set Price Summary (BrickLink API)")
set_input = st.text_input("Enter LEGO Set Numbers (comma-separated):", placeholder="e.g., 10276, 75192, 21309")

# -----------------------------
# 🔧 Normalize Set Number
# -----------------------------
def normalize_set_number(s):
    s = s.strip()
    return s if "-" in s else f"{s}-1"

# -----------------------------
# 🧩 Fetch Set Metadata
# -----------------------------
def fetch_set_metadata(set_number, auth):
    url = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}"
    try:
        resp = requests.get(url, auth=auth)
        if resp.status_code == 200:
            data = resp.json().get("data", {})
            return {
                "Set Name": data.get("name", "N/A"),
                "Category ID": data.get("category_id", "N/A")
            }
    except Exception:
        pass
    return {
        "Set Name": "Error",
        "Category ID": "Error"
    }

# -----------------------------
# 🖼️ Fetch Image URL
# -----------------------------
def fetch_image_url(set_number, auth):
    url = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}/images/0"
    try:
        resp = requests.get(url, auth=auth)
        if resp.status_code == 200:
            return resp.json().get("data", {}).get("thumbnail_url", "")
    except:
        pass
    return ""

# -----------------------------
# 📦 Fetch Price Data (Current and Sold)
# -----------------------------
def fetch_price_data(set_number, auth, guide_type, new_or_used):
    url = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}/price"
    params = {
        "guide_type": guide_type,
        "new_or_used": new_or_used
    }
    try:
        resp = requests.get(url, params=params, auth=auth)
        if resp.status_code == 200:
            return resp.json().get("data", {})
    except Exception:
        pass
    return {}

# -----------------------------
# 📊 Fetch All Set Data
# -----------------------------
def fetch_set_data(set_number, auth):
    set_number = normalize_set_number(set_number)
    metadata = fetch_set_metadata(set_number, auth)
    image_url = fetch_image_url(set_number, auth)

    current_new = fetch_price_data(set_number, auth, "stock", "N")
    current_used = fetch_price_data(set_number, auth, "stock", "U")
    sold_new = fetch_price_data(set_number, auth, "sold", "N")
    sold_used = fetch_price_data(set_number, auth, "sold", "U")

    set_name = metadata.get("Set Name", "N/A")
    link = f"https://www.bricklink.com/v2/catalog/catalogitem.page?S={set_number}#T=P"

    return {
        "Set Image": f'<img src="{image_url}" width="200"/>',
        "Set Number": set_number,
        "Set Name": f'<a href="{link}" target="_blank">{set_name}</a>',
        "Category ID": metadata.get("Category ID", "N/A"),

        # Current New
        "Current Avg Price (New)": f"${float(current_new.get('avg_price', 0)):.2f}" if current_new.get("avg_price") else "N/A",
        "Qty (New)": current_new.get("total_quantity", "N/A"),
        "Lots (New)": current_new.get("unit_quantity", "N/A"),

        # Current Used
        "Current Avg Price (Used)": f"${float(current_used.get('avg_price', 0)):.2f}" if current_used.get("avg_price") else "N/A",
        "Qty (Used)": current_used.get("total_quantity", "N/A"),
        "Lots (Used)": current_used.get("unit_quantity", "N/A"),

        # Last 6 months Sales New
        "Last 6 Months Sales - Avg Price (New)": f"${float(sold_new.get('avg_price', 0)):.2f}" if sold_new.get("avg_price") else "N/A",
        "Sold Qty (New)": sold_new.get("total_quantity", "N/A"),
        "Times Sold (New)": sold_new.get("unit_quantity", "N/A"),

        # Sold Used
        "Last 6 Months Sales - Avg Price (Used)": f"${float(sold_used.get('avg_price', 0)):.2f}" if sold_used.get("avg_price") else "N/A",
        "Sold Qty (Used)": sold_used.get("total_quantity", "N/A"),
        "Times Sold (Used)": sold_used.get("unit_quantity", "N/A"),
    }

# -----------------------------
# 🚀 Fetch and Display
# -----------------------------
if st.button("Fetch Data for Sets"):
    st.markdown("*Please note, data excludes incomplete sets.*")
    
    if all([consumer_key, consumer_secret, token, token_secret, set_input]):
        auth = OAuth1(consumer_key, consumer_secret, token, token_secret)

        set_raw_list = [s.strip() for s in set_input.split(",") if s.strip()]
        set_numbers = [normalize_set_number(s) for s in set_raw_list]
        results = []

        with st.spinner("Fetching BrickLink data..."):
            for set_number in set_numbers:
                data = fetch_set_data(set_number, auth)
                if data:
                    results.append(data)

        if results:
            df = pd.DataFrame(results)
            st.success("Data loaded successfully")
            st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)

            # Prepare CSV
            df_csv = df.copy()
            df_csv["Set Name"] = df_csv["Set Name"].str.extract(r'">(.*?)</a>')
            df_csv["Set Image"] = df_csv["Set Image"].str.extract(r'src="(.*?)"')
            csv = df_csv.to_csv(index=False).encode("utf-8")

            st.download_button(
                label="Download as CSV",
                data=csv,
                file_name="bricklink_set_prices.csv",
                mime="text/csv"
            )
        else:
            st.warning("No valid results found.")
    else:
        st.warning("Please enter your BrickLink credentials and at least one set number.")

# -----------------------------
# Footer
# -----------------------------
st.markdown("---")
st.caption("Powered by BrickLink API, Brickset API, and BrickEconomy API • Created by ReUseBricks")
