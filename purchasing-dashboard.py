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
st.title("🧱 LEGO Set Price Summary (BrickLink API)")
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
        return {
            "Set Name": "Error",
            "Category ID": "Error"
        }
    return None

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

    # Current for sale
    current_new = fetch_price_data(set_number, auth, "stock", "N")
    current_used = fetch_price_data(set_number, auth, "stock", "U")

    # Last 6 months sold
    sold_new = fetch_price_data(set_number, auth, "sold", "N")
    sold_used = fetch_price_data(set_number, auth, "sold", "U")

    set_name = metadata.get("Set Name", "N/A")
    link = f"https://www.bricklink.com/v2/catalog/catalogitem.page?S={set_number}#T=P"

    return {
        "Set Number": set_number,
        "Set Name": f'<a href="{link}" target="_blank">{set_name}</a>',
        "Category ID": metadata.get("Category ID", "N/A"),

        # Current New
        "Current Avg (New)": f"${float(current_new.get('avg_price', 0)):.2f}" if current_new.get("avg_price") else "N/A",
        "Qty (New)": current_new.get("total_quantity", "N/A"),
        "Lots (New)": current_new.get("unit_quantity", "N/A"),

        # Current Used
        "Current Avg (Used)": f"${float(current_used.get('avg_price', 0)):.2f}" if current_used.get("avg_price") else "N/A",
        "Qty (Used)": current_used.get("total_quantity", "N/A"),
        "Lots (Used)": current_used.get("unit_quantity", "N/A"),

        # Sold New
        "Sold Avg (New)": f"${float(sold_new.get('avg_price', 0)):.2f}" if sold_new.get("avg_price") else "N/A",
        "Sold Qty (New)": sold_new.get("total_quantity", "N/A"),
        "Times Sold (New)": sold_new.get("unit_quantity", "N/A"),

        # Sold Used
        "Sold Avg (Used)": f"${float(sold_used.get('avg_price', 0)):.2f}" if sold_used.get("avg_price") else "N/A",
        "Sold Qty (Used)": sold_used.get("total_quantity", "N/A"),
        "Times Sold (Used)": sold_used.get("unit_quantity", "N/A"),
    }

# -----------------------------
# 🚀 Fetch and Display
# -----------------------------
if st.button("Fetch Data for Sets"):
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
            st.success("✅ Data loaded successfully")
            st.markdown("📎 Click a set name to view on BrickLink:")
            st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)
        else:
            st.warning("No valid results found.")
    else:
        st.warning("Please enter your BrickLink credentials and at least one set number.")

# -----------------------------
# 🧾 Footer
# -----------------------------
st.markdown("---")
st.caption("Powered by BrickLink API • Created by ReUseBricks")


