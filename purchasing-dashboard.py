import json
import pandas as pd
import requests
import streamlit as st
from requests_oauthlib import OAuth1


###############################################################################
# Utility functions
###############################################################################

def normalize_set_number(s: str) -> str:
    """Normalize a LEGO set number to include a variant (e.g., 10276 -> 10276-1).

    BrickLink's API expects set numbers in the form ``number-variant``.
    If the user does not supply a variant, ``-1`` is appended.

    Args:
        s: Raw set number entered by the user.

    Returns:
        Normalized set number.
    """
    s = s.strip()
    return s if "-" in s else f"{s}-1"


def fetch_set_metadata(set_number: str, auth: OAuth1) -> dict:
    """Fetch basic metadata for a set from the BrickLink API.

    Args:
        set_number: Normalized set number (e.g., "10276-1").
        auth: OAuth1 authentication object.

    Returns:
        A dictionary containing the set name and category ID, or placeholders
        if the request fails.
    """
    url = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}"
    try:
        resp = requests.get(url, auth=auth)
        if resp.status_code == 200:
            data = resp.json().get("data", {})
            return {
                "Set Name": data.get("name", "N/A"),
                "Category ID": data.get("category_id", "N/A"),
            }
    except Exception:
        pass
    return {
        "Set Name": "Error",
        "Category ID": "Error",
    }


def fetch_image_url(set_number: str, auth: OAuth1) -> str:
    """Retrieve the first thumbnail image URL for a set from BrickLink.

    Args:
        set_number: Normalized set number.
        auth: OAuth1 authentication object.

    Returns:
        A URL string to a thumbnail image, or an empty string on failure.
    """
    url = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}/images/0"
    try:
        resp = requests.get(url, auth=auth)
        if resp.status_code == 200:
            return resp.json().get("data", {}).get("thumbnail_url", "")
    except Exception:
        pass
    return ""


def fetch_price_data(
    set_number: str,
    auth: OAuth1,
    guide_type: str,
    new_or_used: str,
) -> dict:
    """Fetch price information for a set from the BrickLink API.

    The BrickLink API allows querying both current stock and sold prices,
    split by new/used condition. This helper encapsulates those calls.

    Args:
        set_number: Normalized set number.
        auth: OAuth1 authentication object.
        guide_type: Either ``"stock"`` for current prices or ``"sold"``
            for last six months sales data.
        new_or_used: ``"N"`` for new condition or ``"U"`` for used.

    Returns:
        A dictionary of pricing information, or an empty dictionary on failure.
    """
    url = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}/price"
    params = {
        "guide_type": guide_type,
        "new_or_used": new_or_used,
    }
    try:
        resp = requests.get(url, params=params, auth=auth)
        if resp.status_code == 200:
            return resp.json().get("data", {})
    except Exception:
        pass
    return {}


def fetch_set_data(set_number: str, auth: OAuth1) -> dict:
    """Aggregate BrickLink data for a single set number.

    Combines metadata, image URL, current price (new/used) and sold price
    (new/used) into a single dictionary with human-friendly keys.  The
    "Set Name" value is wrapped in an anchor tag for linking back to
    BrickLink's catalog page.

    Args:
        set_number: Raw set number entered by the user (may omit variant).
        auth: OAuth1 authentication object.

    Returns:
        A dictionary with the aggregated data.
    """
    set_number = normalize_set_number(set_number)
    metadata = fetch_set_metadata(set_number, auth)
    image_url = fetch_image_url(set_number, auth)

    current_new = fetch_price_data(set_number, auth, "stock", "N")
    current_used = fetch_price_data(set_number, auth, "stock", "U")
    sold_new = fetch_price_data(set_number, auth, "sold", "N")
    sold_used = fetch_price_data(set_number, auth, "sold", "U")

    set_name = metadata.get("Set Name", "N/A")
    link = (
        f"https://www.bricklink.com/v2/catalog/catalogitem.page?S={set_number}#T=P"
    )

    return {
        "Set Image": f'<img src="{image_url}" width="200"/>',
        "Set Number": set_number,
        "Set Name": f'<a href="{link}" target="_blank">{set_name}</a>',
        "Category ID": metadata.get("Category ID", "N/A"),
        # Current New
        "Current Avg Price (New)": (
            f"${float(current_new.get('avg_price', 0)):.2f}"
            if current_new.get("avg_price")
            else "N/A"
        ),
        "Qty (New)": current_new.get("total_quantity", "N/A"),
        "Lots (New)": current_new.get("unit_quantity", "N/A"),
        # Current Used
        "Current Avg Price (Used)": (
            f"${float(current_used.get('avg_price', 0)):.2f}"
            if current_used.get("avg_price")
            else "N/A"
        ),
        "Qty (Used)": current_used.get("total_quantity", "N/A"),
        "Lots (Used)": current_used.get("unit_quantity", "N/A"),
        # Last 6 months Sales New
        "Last 6 Months Sales - Avg Price (New)": (
            f"${float(sold_new.get('avg_price', 0)):.2f}"
            if sold_new.get("avg_price")
            else "N/A"
        ),
        "Sold Qty (New)": sold_new.get("total_quantity", "N/A"),
        "Times Sold (New)": sold_new.get("unit_quantity", "N/A"),
        # Sold Used
        "Last 6 Months Sales - Avg Price (Used)": (
            f"${float(sold_used.get('avg_price', 0)):.2f}"
            if sold_used.get("avg_price")
            else "N/A"
        ),
        "Sold Qty (Used)": sold_used.get("total_quantity", "N/A"),
        "Times Sold (Used)": sold_used.get("unit_quantity", "N/A"),
    }


def fetch_brickset_details(set_number: str, api_key: str) -> dict:
    """Fetch official metadata for a set from the BrickSet API.

    This function sends a POST request to the BrickSet `getSets` endpoint with
    three parameters: ``apiKey``, ``userHash``, and a JSON-encoded ``params``
    string. Even when not retrieving owned or wanted sets, the API requires
    a ``userHash`` parameter to be present, so an empty string is provided.
    The request asks for ``extendedData`` to include additional fields such
    as ratings, tags and descriptions.

    Args:
        set_number: Normalized set number (e.g. "75192-1").
        api_key: BrickSet API key.

    Returns:
        A dictionary with selected BrickSet fields, or N/A placeholders on
        error.
    """
    url = "https://brickset.com/api/v3.asmx/getSets"
    # Request extended data for a richer response
    params = {"setNumber": set_number, "extendedData": 1}
    # According to the API documentation, userHash is mandatory even if not used
    form_data = {
        "apiKey": api_key,
        "userHash": "",  # empty user hash since we're not logged in
        "params": json.dumps(params),
    }
    try:
        # Use POST instead of GET to avoid URL length issues and satisfy API requirements
        resp = requests.post(url, data=form_data, timeout=20)
        # The response should be JSON; if it's XML, .json() will raise
        data = resp.json()
        if data.get("status") == "success" and data.get("matches", 0) > 0:
            set_info = data["sets"][0]
            # Extract nested collection ownership counts
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
        # Swallow errors and return N/A values if any exception occurs
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

def fetch_brickeconomy_details(
    set_number: str, api_key: str, currency: str = "USD"
) -> dict:
    """Fetch pricing and value information from the BrickEconomy API.

    The BrickEconomy API uses a simple REST endpoint: ``/api/v1/set/<set number>``.
    It requires an `x-apikey` header for authentication and a `User-Agent` header.
    Optionally, a `currency` query parameter can be provided to retrieve values
    in a specific currency.  The endpoint returns a JSON object under the
    ``data`` key containing information such as retail prices, current values,
    forecast values and growth metrics【231053118022290†L274-L457】.

    Args:
        set_number: The LEGO set number, including variant (e.g. "75192-1").
        api_key: The BrickEconomy API key.
        currency: Optional ISO 4217 currency code (e.g. "USD", "EUR").

    Returns:
        A dictionary with selected BrickEconomy metrics, or N/A values on error.
    """
    # Normalize set number to include variant if missing
    set_number = set_number.strip()
    if "-" not in set_number:
        set_number = f"{set_number}-1"

    base_url = "https://www.brickeconomy.com/api/v1/set/"
    url = f"{base_url}{set_number}"
    params = {"currency": currency} if currency else {}
    headers = {
        "accept": "application/json",
        "x-apikey": api_key,
        # Use a simple user-agent string as required by the API
        "User-Agent": "ReUseBricks-Streamlit-App/1.0",
    }
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=20)
        data = resp.json().get("data") if resp.status_code == 200 else None
        if data:
            return {
                "Set Name": data.get("name", "N/A"),
                "Theme": data.get("theme", "N/A"),
                "Subtheme": data.get("subtheme", "N/A"),
                "Year": data.get("year", "N/A"),
                "Pieces": data.get("pieces_count", "N/A"),
                "Minifigs": data.get("minifigs_count", "N/A"),
                "Retail Price (US)": data.get("retail_price_us", "N/A"),
                "Current Value New": data.get("current_value_new", "N/A"),
                "Current Value Used": data.get("current_value_used", "N/A"),
                "Forecast New 2y": data.get("forecast_value_new_2_years", "N/A"),
                "Forecast New 5y": data.get("forecast_value_new_5_years", "N/A"),
                "Forecast Growth New 2y %": data.get("forecast_growth_new_2_years", "N/A"),
                "Forecast Growth New 5y %": data.get("forecast_growth_new_5_years", "N/A"),
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
        "Forecast Growth New 2y %",
        "Forecast Growth New 5y %",
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
# For security, all BrickLink credentials are entered manually in the sidebar.
consumer_key = st.sidebar.text_input("Consumer Key", type="password")
consumer_secret = st.sidebar.text_input("Consumer Secret", type="password")
token = st.sidebar.text_input("Token", type="password")
token_secret = st.sidebar.text_input("Token Secret", type="password")

st.sidebar.subheader("BrickSet")
# BrickSet API key is also entered manually. This app does not persist the key.
brickset_key = st.sidebar.text_input("BrickSet API", type="password")

st.sidebar.subheader("BrickEconomy")
# BrickEconomy API key is provided manually for security. Users should
# paste their BrickEconomy key here. The app does not persist this value.
brickeconomy_key = st.sidebar.text_input("BrickEconomy API", type="password")

with st.sidebar.expander("Show Current IP Address"):
    try:
        ip = requests.get("https://api.ipify.org").text
        st.code(ip, language="text")
        st.caption("Use this IP address to register your BrickLink API access.")
    except Exception:
        st.error("Unable to fetch IP address.")

# Custom CSS to align content and maximize width
st.markdown(
    """
    <style>
        .block-container {
            padding-top: 2rem;
            padding-left: 2rem;
            padding-right: 2rem;
            max-width: 100%;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# Title
st.title("LEGO Set Price & Metadata Dashboard")

# Tabs for BrickLink, BrickSet, BrickEconomy, Scoring
tab_bricklink, tab_brickset, tab_brickeconomy, tab_scoring = st.tabs(["BrickLink", "BrickSet", "BrickEconomy", "Scoring"])

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
    # Button to trigger BrickLink data fetch
    if st.button("Fetch BrickLink Data"):
        st.markdown("*Please note, data excludes incomplete sets.*")
        if all([consumer_key, consumer_secret, token, token_secret, set_input]):
            auth = OAuth1(consumer_key, consumer_secret, token, token_secret)
            set_raw_list = [s.strip() for s in set_input.split(",") if s.strip()]
            results = []
            with st.spinner("Fetching BrickLink data..."):
                for s in set_raw_list:
                    data = fetch_set_data(s, auth)
                    if data:
                        results.append(data)
            if results:
                df = pd.DataFrame(results)
                st.success("BrickLink data loaded successfully")
                # Convert the 'Set Name' HTML link to plain text and drop the image column
                df_display = df.copy()
                # Extract the inner text from the anchor tag for display
                df_display["Set Name"] = df_display["Set Name"].str.extract(r'">(.*?)</a>')
                # Drop the Set Image column so the table formatting matches other tabs
                if "Set Image" in df_display.columns:
                    df_display = df_display.drop(columns=["Set Image"])  # remove image
                # Show the data using Streamlit's built-in dataframe component
                st.dataframe(df_display)
                # Prepare CSV for download using the same cleaned DataFrame
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
            st.warning(
                "Please enter your BrickLink credentials and at least one set number."
            )

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
    # Button to trigger BrickSet data fetch
    if st.button("Fetch BrickSet Data"):
        if brickset_key and bs_set_input:
            set_raw_list = [s.strip() for s in bs_set_input.split(",") if s.strip()]
            results = []
            with st.spinner("Fetching BrickSet data..."):
                for s in set_raw_list:
                    s_norm = normalize_set_number(s)
                    data = fetch_brickset_details(s_norm, brickset_key)
                    # Include the normalized set number for reference
                    data.update({"Set Number": s_norm})
                    results.append(data)
            if results:
                df = pd.DataFrame(results)
                st.success("BrickSet data loaded successfully")
                st.dataframe(df)
                # Offer CSV download
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
            st.warning(
                "Please enter your BrickSet API key (in the sidebar) and at least "
                "one set number."
            )

# -----------------------------------------------------------------------------
# BrickEconomy Tab
# -----------------------------------------------------------------------------
with tab_brickeconomy:
    st.header("BrickEconomy Data")
    
    # Input for set numbers
    be_set_input = st.text_input(
        "Enter LEGO Set Numbers (comma-separated) for BrickEconomy:",
        placeholder="e.g., 10276, 75192, 21309",
        key="brickeconomy_set_input",
    )
    # Allow user to select currency; default to USD
    currency = st.selectbox(
        "Select currency for valuations:",
        options=["USD", "GBP", "CAD", "AUD", "CNY", "KRW", "EUR", "JPY"],
        index=0,
    )
    if st.button("Fetch BrickEconomy Data"):
        # Require both the API key (entered in the sidebar) and set numbers
        if brickeconomy_key and be_set_input:
            set_raw_list = [s.strip() for s in be_set_input.split(",") if s.strip()]
            results = []
            with st.spinner("Fetching BrickEconomy data..."):
                for s in set_raw_list:
                    # Fetch data for each set and append normalized set number
                    data = fetch_brickeconomy_details(s, brickeconomy_key, currency)
                    data.update({"Set Number": normalize_set_number(s)})
                    results.append(data)
            if results:
                df = pd.DataFrame(results)
                st.success("BrickEconomy data loaded successfully")
                st.dataframe(df)
                # Prepare CSV download
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
            st.warning(
                "Please enter your BrickEconomy API key (in the sidebar) and at "
                "least one set number."
            )

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
st.caption(
    "Powered by BrickLink API and BrickSet API • Created by ReUseBricks"
)
