import json
import pandas as pd
import requests
import streamlit as st
from requests_oauthlib import OAuth1


###############################################################################
# Utility functions
###############################################################################

def normalize_set_number(s: str) -> str:
    """Normalize a LEGO set number to include a variant (e.g., 10276 -> 10276-1)."""
    s = s.strip()
    return s if "-" in s else f"{s}-1"


def _get_json(url, *, params=None, auth=None, timeout=20):
    """GET JSON with clear errors (no silent failures)."""
    resp = requests.get(
        url,
        params=params,
        auth=auth,
        timeout=timeout,
        headers={"Accept": "application/json"},
    )
    try:
        data = resp.json()
    except Exception:
        data = None
    if resp.status_code != 200 or data is None:
        # Raise so caller can surface in UI
        raise RuntimeError(
            f"BrickLink API {url} failed: HTTP {resp.status_code} "
            f"{resp.text[:300]}"
        )
    return data


###############################################################################
# BrickLink API helpers
###############################################################################

def fetch_set_metadata(set_number: str, auth: OAuth1) -> dict:
    url = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}"
    try:
        data = _get_json(url, auth=auth).get("data", {})
        return {"Set Name": data.get("name"), "Category ID": data.get("category_id")}
    except Exception as e:
        return {"Set Name": None, "Category ID": None, "_error": str(e)}


def fetch_image_url(set_number: str, auth: OAuth1) -> str:
    url = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}/images/0"
    try:
        return _get_json(url, auth=auth).get("data", {}).get("thumbnail_url") or ""
    except Exception:
        return ""


def fetch_price_data(set_number: str, auth: OAuth1, guide_type: str, new_or_used: str) -> dict:
    """guide_type: 'stock' (current) or 'sold' (last 6 months). new_or_used: 'N' or 'U'."""
    url = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}/price"
    params = {"guide_type": guide_type, "new_or_used": new_or_used}
    try:
        return _get_json(url, params=params, auth=auth).get("data", {}) or {}
    except Exception as e:
        return {"_error": str(e)}


def _fmt_money(v):
    return f"${float(v):.2f}" if v is not None else "N/A"


def fetch_set_data(set_number: str, auth: OAuth1) -> dict:
    """Aggregate BrickLink data for a single set number into a display row."""
    set_number = normalize_set_number(set_number)
    metadata = fetch_set_metadata(set_number, auth)
    image_url = fetch_image_url(set_number, auth)

    current_new = fetch_price_data(set_number, auth, "stock", "N")
    current_used = fetch_price_data(set_number, auth, "stock", "U")
    sold_new    = fetch_price_data(set_number, auth, "sold",  "N")
    sold_used   = fetch_price_data(set_number, auth, "sold",  "U")

    cn_avg = current_new.get("avg_price", None)  # allow 0.0
    cu_avg = current_used.get("avg_price", None)
    sn_avg = sold_new.get("avg_price", None)
    su_avg = sold_used.get("avg_price", None)

    link = f"https://www.bricklink.com/v2/catalog/catalogitem.page?S={set_number}#T=P"
    set_name_text = metadata.get("Set Name") or "N/A"

    row = {
        "Set Image": f'<img src="{image_url}" width="200"/>',
        "Set Number": set_number,
        "Set Name": f'<a href="{link}" target="_blank">{set_name_text}</a>',
        "Category ID": metadata.get("Category ID") if metadata.get("Category ID") is not None else "N/A",

        # Current (stock)
        "Current Avg Price (New)":  _fmt_money(cn_avg) if cn_avg is not None else "N/A",
        "Qty (New)":                 current_new.get("total_quantity", "N/A"),
        "Lots (New)":                current_new.get("total_lots", "N/A"),

        "Current Avg Price (Used)": _fmt_money(cu_avg) if cu_avg is not None else "N/A",
        "Qty (Used)":                current_used.get("total_quantity", "N/A"),
        "Lots (Used)":               current_used.get("total_lots", "N/A"),

        # Sold (last 6 months)
        "Last 6 Months Sales - Avg Price (New)":  _fmt_money(sn_avg) if sn_avg is not None else "N/A",
        "Sold Qty (New)":                          sold_new.get("total_quantity", "N/A"),
        "Times Sold (New)":                        sold_new.get("total_lots", "N/A"),  # or len(price_detail)

        "Last 6 Months Sales - Avg Price (Used)": _fmt_money(su_avg) if su_avg is not None else "N/A",
        "Sold Qty (Used)":                         sold_used.get("total_quantity", "N/A"),
        "Times Sold (Used)":                       sold_used.get("total_lots", "N/A"),
    }

    # Bubble up any errors so the UI can show them
    for d in (metadata, current_new, current_used, sold_new, sold_used):
        if isinstance(d.get("_error"), str):
            row.setdefault("_errors", []).append(d["_error"])
    return row


###############################################################################
# BrickSet API
###############################################################################

def fetch_brickset_details(set_number: str, api_key: str) -> dict:
    """Fetch official metadata for a set from the BrickSet API."""
    url = "https://brickset.com/api/v3.asmx/getSets"
    params = {"setNumber": set_number, "extendedData": 1}
    form_data = {"apiKey": api_key, "userHash": "", "params": json.dumps(params)}
    try:
        resp = requests.post(url, data=form_data, timeout=20)
        data = resp.json()
        if data.get("status") == "success" and data.get("matches", 0) > 0:
            set_info = data["sets"][0]
            collections = set_info.get("collections", {}) or {}
            return {
                "Set Name (BrickSet)": set_info.get("name", "N/A"),
                "Pieces": set_info.get("pieces", "N/A"),
                "Minifigs": set_info.get("minifigs", "N/A"),
                "BrickSet Theme": set_info.get("theme", "N/A"),
                "BrickSet Year": set_info.get("year", "N/A"),
                "BrickSet Rating": set_info.get("rating", "N/A"),
                "Users Owned": collections.get("ownedBy", "N/A"),
                "Users Wanted": collections.get("wantedBy", "N/A"),
            }
    except Exception:
        pass
    return {
        "Set Name (BrickSet)": "N/A",
        "Pieces": "N/A",
        "Minifigs": "N/A",
        "BrickSet Theme": "N/A",
        "BrickSet Year": "N/A",
        "BrickSet Rating": "N/A",
        "Users Owned": "N/A",
        "Users Wanted": "N/A",
    }


###############################################################################
# BrickEconomy API
###############################################################################

def fetch_brickeconomy_details(set_number: str, api_key: str, currency: str = "USD") -> dict:
    set_number = set_number.strip()
    if "-" not in set_number:
        set_number = f"{set_number}-1"

    base_url = "https://www.brickeconomy.com/api/v1/set/"
    url = f"{base_url}{set_number}"
    params = {"currency": currency} if currency else {}
    headers = {
        "accept": "application/json",
        "x-apikey": api_key,
        "User-Agent": "ReUseBricks-Streamlit-App/1.0",
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=20)
        data = resp.json().get("data") if resp.status_code == 200 else None
        if data:
            current_value_new = data.get("current_value_new")
            forecast_2y = data.get("forecast_value_new_2_years")
            forecast_5y = data.get("forecast_value_new_5_years")

            try:
                growth_2y_pct = (
                    ((forecast_2y - current_value_new) / current_value_new) * 100
                    if current_value_new and forecast_2y
                    else "N/A"
                )
            except (TypeError, ZeroDivisionError):
                growth_2y_pct = "N/A"

            try:
                growth_5y_pct = (
                    ((forecast_5y - current_value_new) / current_value_new) * 100
                    if current_value_new and forecast_5y
                    else "N/A"
                )
            except (TypeError, ZeroDivisionError):
                growth_5y_pct = "N/A"

            return {
                "Set Name": data.get("name", "N/A"),
                "Theme": data.get("theme", "N/A"),
                "Subtheme": data.get("subtheme", "N/A"),
                "Year": data.get("year", "N/A"),
                "Pieces": data.get("pieces_count", "N/A"),
                "Minifigs": data.get("minifigs_count", "N/A"),
                "Retail Price (US)": data.get("retail_price_us", "N/A"),
                "Current Value New": current_value_new if current_value_new else "N/A",
                "Current Value Used": data.get("current_value_used", "N/A"),
                "Forecast New 2y": forecast_2y if forecast_2y else "N/A",
                "Forecast New 5y": forecast_5y if forecast_5y else "N/A",
                "Forecast Growth New 2y %": round(growth_2y_pct, 2)
                if isinstance(growth_2y_pct, (float, int))
                else "N/A",
                "Forecast Growth New 5y %": round(growth_5y_pct, 2)
                if isinstance(growth_5y_pct, (float, int))
                else "N/A",
                "Growth Last Year %": data.get("rolling_growth_lastyear", "N/A"),
                "Growth 12 Months %": data.get("rolling_growth_12months", "N/A"),
            }
    except Exception:
        pass

    return {
        "Set Name": "N/A",
        "Theme": "N/A",
        "Subtheme": "N/A",
        "Year": "N/A",
        "Pieces": "N/A",
        "Minifigs": "N/A",
        "Retail Price (US)": "N/A",
        "Current Value New": "N/A",
        "Current Value Used": "N/A",
        "Forecast New 2y": "N/A",
        "Forecast New 5y": "N/A",
        "Forecast Growth New 2y %": "N/A",
        "Forecast Growth New 5y %": "N/A",
        "Growth Last Year %": "N/A",
        "Growth 12 Months %": "N/A",
    }


###############################################################################
# Streamlit application layout
###############################################################################

st.set_page_config(page_title="LEGO Set Dashboard", layout="wide")

# Sidebar credentials
st.sidebar.header("API Credentials")

st.sidebar.subheader("BrickLink")
consumer_key = st.sidebar.text_input("Consumer Key", type="password")
consumer_secret = st.sidebar.text_input("Consumer Secret", type="password")
token = st.sidebar.text_input("Token", type="password")
token_secret = st.sidebar.text_input("Token Secret", type="password")

st.sidebar.subheader("BrickSet")
brickset_key = st.sidebar.text_input("BrickSet API", type="password")

st.sidebar.subheader("BrickEconomy")
brickeconomy_key = st.sidebar.text_input("BrickEconomy API", type="password")

with st.sidebar.expander("Show Current IP Address"):
    try:
        ip = requests.get("https://api.ipify.org").text
        st.code(ip, language="text")
        st.caption("Use this IP address to register your BrickLink API access.")
    except Exception:
        st.error("Unable to fetch IP address.")

# Custom CSS
st.markdown(
    """
    <style>
        .block-container { padding-top: 2rem; padding-left: 2rem; padding-right: 2rem; max-width: 100%; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("LEGO Set Price & Metadata Dashboard")

tab_bricklink, tab_brickset, tab_brickeconomy, tab_scoring = st.tabs(
    ["BrickLink", "BrickSet", "BrickEconomy", "Scoring"]
)

# -----------------------------------------------------------------------------
# BrickLink Tab
# -----------------------------------------------------------------------------
with tab_bricklink:
    st.header("BrickLink Data")

    set_input = st.text_input(
        "Enter LEGO Set Numbers (comma-separated) for BrickLink:",
        placeholder="e.g., 10276, 75192, 21309",
        key="bricklink_set_input",
    )
    if st.button("Fetch BrickLink Data"):
        st.markdown("*Please note, data excludes incomplete sets.*")
        if all([consumer_key, consumer_secret, token, token_secret, set_input]):
            auth = OAuth1(consumer_key, consumer_secret, token, token_secret)
            set_raw_list = [s.strip() for s in set_input.split(",") if s.strip()]
            results = []
            errors = []
            with st.spinner("Fetching BrickLink data..."):
                for s in set_raw_list:
                    row = fetch_set_data(s, auth)
                    if row:
                        if "_errors" in row:
                            for e in row["_errors"]:
                                errors.append(f"{normalize_set_number(s)}: {e}")
                            row.pop("_errors", None)
                        results.append(row)
            if errors:
                st.warning("Some BrickLink requests failed:\n\n" + "\n".join(f"- {e}" for e in errors))
            if results:
                df = pd.DataFrame(results)
                st.success("BrickLink data loaded successfully")
                # For display: strip anchor text, drop image column
                df_display = df.copy()
                df_display["Set Name"] = df_display["Set Name"].str.extract(r'">(.*?)</a>')
                if "Set Image" in df_display.columns:
                    df_display = df_display.drop(columns=["Set Image"])
                st.dataframe(df_display)
                csv = df_display.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="Download BrickLink Data as CSV",
                    data=csv,
                    file_name="bricklink_set_prices.csv",
                    mime="text/csv",
                )
            else:
                st.warning("No valid BrickLink results found.")
        else:
            st.warning("Please enter your BrickLink credentials and at least one set number.")

# -----------------------------------------------------------------------------
# BrickSet Tab
# -----------------------------------------------------------------------------
with tab_brickset:
    st.header("BrickSet Data")
    bs_set_input = st.text_input(
        "Enter LEGO Set Numbers (comma-separated) for BrickSet:",
        placeholder="e.g., 10276, 75192, 21309",
        key="brickset_set_input",
    )
    if st.button("Fetch BrickSet Data"):
        if brickset_key and bs_set_input:
            set_raw_list = [s.strip() for s in bs_set_input.split(",") if s.strip()]
            results = []
            with st.spinner("Fetching BrickSet data..."):
                for s in set_raw_list:
                    s_norm = normalize_set_number(s)
                    data = fetch_brickset_details(s_norm, brickset_key)
                    data.update({"Set Number": s_norm})
                    results.append(data)
            if results:
                df = pd.DataFrame(results)
                st.success("BrickSet data loaded successfully")
                st.dataframe(df)
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="Download BrickSet Data as CSV",
                    data=csv,
                    file_name="brickset_metadata.csv",
                    mime="text/csv",
                )
            else:
                st.warning("No valid BrickSet results found.")
        else:
            st.warning("Please enter your BrickSet API key (in the sidebar) and at least one set number.")

# -----------------------------------------------------------------------------
# BrickEconomy Tab
# -----------------------------------------------------------------------------
with tab_brickeconomy:
    st.header("BrickEconomy Data")
    be_set_input = st.text_input(
        "Enter LEGO Set Numbers (comma-separated) for BrickEconomy:",
        placeholder="e.g., 10276, 75192, 21309",
        key="brickeconomy_set_input",
    )
    currency = st.selectbox(
        "Select currency for valuations:",
        options=["USD", "GBP", "CAD", "AUD", "CNY", "KRW", "EUR", "JPY"],
        index=0,
    )
    if st.button("Fetch BrickEconomy Data"):
        if brickeconomy_key and be_set_input:
            set_raw_list = [s.strip() for s in be_set_input.split(",") if s.strip()]
            results = []
            with st.spinner("Fetching BrickEconomy data..."):
                for s in set_raw_list:
                    data = fetch_brickeconomy_details(s, brickeconomy_key, currency)
                    data.update({"Set Number": normalize_set_number(s)})
                    results.append(data)
            if results:
                df = pd.DataFrame(results)
                st.success("BrickEconomy data loaded successfully")
                st.dataframe(df)
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="Download BrickEconomy Data as CSV",
                    data=csv,
                    file_name="brickeconomy_data.csv",
                    mime="text/csv",
                )
            else:
                st.warning("No valid BrickEconomy results found.")
        else:
            st.warning("Please enter your BrickEconomy API key (in the sidebar) and at least one set number.")

# -----------------------------------------------------------------------------
# Scoring Tab
# -----------------------------------------------------------------------------
with tab_scoring:
    st.header("LEGO Set Scoring Metrics")
    scoring_input = st.text_input(
        "Enter LEGO Set Numbers (comma-separated) for Scoring:",
        placeholder="e.g., 10276, 75192, 21309",
        key="scoring_set_input",
    )
    if st.button("Calculate Scores"):
        if brickset_key and scoring_input:
            set_raw_list = [s.strip() for s in scoring_input.split(",") if s.strip()]
            results = []
            with st.spinner("Fetching data and calculating scores..."):
                for s in set_raw_list:
                    s_norm = normalize_set_number(s)
                    bset_data = fetch_brickset_details(s_norm, brickset_key)
                    try:
                        owned_raw = bset_data.get("Users Owned")
                        wanted_raw = bset_data.get("Users Wanted")
                        try:
                            owned = float(owned_raw)
                            wanted = float(wanted_raw)
                            demand_ratio = wanted / owned if owned else 0
                            demand_percent = ((owned + wanted) / 357478) * 100
                        except (TypeError, ValueError):
                            owned = wanted = demand_ratio = demand_percent = "N/A"
                    except Exception:
                        owned = wanted = demand_ratio = demand_percent = "N/A"
                    results.append({
                        "Set Number": s_norm,
                        "Brickset Owned": owned,
                        "Brickset Wanted": wanted,
                        "Demand Ratio": round(demand_ratio, 3) if isinstance(demand_ratio, (float, int)) else "N/A",
                        "Demand %": round(demand_percent, 2) if isinstance(demand_percent, (float, int)) else "N/A",
                    })
            if results:
                df = pd.DataFrame(results)
                st.success("Scoring complete")
                st.dataframe(df)
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="Download Scoring Data as CSV",
                    data=csv,
                    file_name="lego_set_scoring.csv",
                    mime="text/csv",
                )
            else:
                st.warning("No results computed.")
        else:
            st.warning("Please enter your BrickSet API key and at least one set number.")

# Footer
st.markdown("---")
st.caption("Powered by BrickLink API and BrickSet API • Created by ReUseBricks")
