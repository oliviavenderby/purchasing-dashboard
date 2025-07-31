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
# Streamlit application layout
###############################################################################

st.set_page_config(page_title="LEGO Set Dashboard", layout="wide")

# Sidebar credentials
st.sidebar.header("API Credentials")
st.sidebar.subheader("BrickLink API")
consumer_key = st.sidebar.text_input("Consumer Key", type="password")
consumer_secret = st.sidebar.text_input("Consumer Secret", type="password")
token = st.sidebar.text_input("Token", type="password")
token_secret = st.sidebar.text_input("Token Secret", type="password")

st.sidebar.subheader("BrickSet API")
# If the key is stored in secrets, use that. Otherwise allow manual entry.
default_brickset_key = (
    st.secrets["brickset"]["api_key"]
    if "brickset" in st.secrets and "api_key" in st.secrets["brickset"]
    else ""
)
brickset_key = st.sidebar.text_input(
    "BrickSet API Key",
    type="password",
    value=default_brickset_key,
)

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

# Tabs for BrickLink and BrickSet
tab_bricklink, tab_brickset = st.tabs(["BrickLink", "BrickSet"])

# -----------------------------------------------------------------------------
# BrickLink Tab
# -----------------------------------------------------------------------------
with tab_bricklink:
    st.header("BrickLink Data")
    st.caption(
        "Enter your BrickLink API credentials in the sidebar and supply one or more "
        "set numbers. Results include current and sold prices, quantities, and links "
        "back to BrickLink."
    )

    set_input = st.text_input(
        "Enter LEGO Set Numbers (comma-separated):",
        placeholder="e.g., 10276, 75192, 21309",
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
                # Display with HTML to render images and links
                st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)
                # Prepare CSV for download (strip HTML)
                df_csv = df.copy()
                df_csv["Set Name"] = df_csv["Set Name"].str.extract(r'">(.*?)</a>')
                df_csv["Set Image"] = df_csv["Set Image"].str.extract(r'src="(.*?)"')
                csv = df_csv.to_csv(index=False).encode("utf-8")
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
    st.caption(
        "Enter your BrickSet API key in the sidebar. Provide one or more set "
        "numbers to retrieve official metadata such as piece counts, minifig counts, "
        "themes, years, and ratings. You can cache your key in ``secrets.toml``."
    )
    bs_set_input = st.text_input(
        "Enter LEGO Set Numbers (comma-separated) for BrickSet:",
        placeholder="e.g., 10276, 75192, 21309",
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

# Footer
st.markdown("---")
st.caption(
    "Powered by BrickLink API and BrickSet API • Created by ReUseBricks"
)
