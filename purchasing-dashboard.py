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
# 📦 Fetch Price Guide Data
# -----------------------------
def fetch_set_data(set_number, auth):
    set_number = normalize_set_number(set_number)
    base_url = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}/price"
    try:
        new_resp = requests.get(base_url + "?new_or_used=N", auth=auth)
        used_resp = requests.get(base_url + "?new_or_used=U", auth=auth)
        metadata = fetch_set_metadata(set_number, auth)

        if new_resp.status_code == 200 and used_resp.status_code == 200:
            new_data = new_resp.json().get("data", {})
            used_data = used_resp.json().get("data", {})

            set_name = metadata.get("Set Name", "N/A")
            link = f"https://www.bricklink.com/v2/catalog/catalogitem.page?S={set_number}#T=P"

            return {
                "Set Number": set_number,
                "Set Name": f'<a href="{link}" target="_blank">{set_name}</a>',
                "Category ID": metadata.get("Category ID", "N/A"),
                "Avg Price (New)": f"${float(new_data.get('avg_price', 0)):.2f}" if new_data.get("avg_price") else "N/A",
                "Qty (New)": new_data.get("total_quantity", "N/A"),
                "Lots (New)": new_data.get("unit_quantity", "N/A"),
                "Avg Price (Used)": f"${float(used_data.get('avg_price', 0)):.2f}" if used_data.get("avg_price") else "N/A",
                "Qty (Used)": used_data.get("total_quantity", "N/A"),
                "Lots (Used)": used_data.get("unit_quantity", "N/A"),
            }
    except Exception:
        return {
            "Set Number": set_number,
            "Set Name": "Error",
            "Category ID": "Error",
            "Avg Price (New)": "Error",
            "Qty (New)": "Error",
            "Lots (New)": "Error",
            "Avg Price (Used)": "Error",
            "Qty (Used)": "Error",
            "Lots (Used)": "Error"
        }

    return None

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

