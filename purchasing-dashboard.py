# purchasing_dashboard.py
# Streamlit app: LEGO purchasing assistant with 24h API caching and a persistent query log
# - BrickLink (OAuth1)
# - BrickSet (API key)
# - BrickEconomy (API key)
#
# Notes
# * Caches identical requests for 24 hours to avoid repeated API calls.
# * Logs every query (cache hit or live) to a local SQLite DB (search_log.db).
# * Sidebar shows "Search Log (today)" with a clear button.

import os
import json
import sqlite3
import hashlib
from datetime import datetime, timezone, date
from typing import Optional, List, Dict, Any

import requests
import pandas as pd
import streamlit as st
from requests_oauthlib import OAuth1

# ---------------------
# App Config
# ---------------------
st.set_page_config(page_title="LEGO Purchasing Assistant", layout="wide")

# ---------------------
# Utilities: DB (query log)
# ---------------------
DB_PATH = os.environ.get("QUERY_LOG_DB_PATH", "search_log.db")


def _init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS query_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc TEXT NOT NULL,
            source TEXT NOT NULL,
            set_number TEXT NOT NULL,
            params_hash TEXT NOT NULL,
            cache_hit INTEGER NOT NULL,
            summary TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def _hash_params(d: dict) -> str:
    blob = json.dumps(d or {}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def log_query(*, source: str, set_number: str, params: dict, cache_hit: bool, summary: str = ""):
    _init_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO query_log (ts_utc, source, set_number, params_hash, cache_hit, summary) VALUES (?,?,?,?,?,?)",
        (
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
            source,
            set_number,
            _hash_params(params),
            int(bool(cache_hit)),
            (summary or "")[:300],
        ),
    )
    conn.commit()
    conn.close()


def get_todays_log():
    _init_db()
    today = date.today().isoformat()  # YYYY-MM-DD in local server date
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        SELECT ts_utc, source, set_number, params_hash, cache_hit, summary
        FROM query_log
        WHERE substr(ts_utc, 1, 10) = ?
        ORDER BY ts_utc DESC
        """,
        (today,),
    )
    rows = c.fetchall()
    conn.close()
    return rows


def clear_todays_log():
    _init_db()
    today = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM query_log WHERE substr(ts_utc,1,10)=?", (today,))
    conn.commit()
    conn.close()


# ---------------------
# Utilities: misc
# ---------------------

def normalize_set_number(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    return s if "-" in s else f"{s}-1"


def parse_set_input(raw: str) -> List[str]:
    if not raw:
        return []
    parts = [p.strip() for p in raw.replace("
", ",").split(",")]
    parts = [p for p in parts if p]
    return parts


# ---------------------
# Low-level HTTP helpers
# ---------------------

@st.cache_data(ttl=86400, show_spinner=False, hash_funcs={OAuth1: lambda _: "oauth1"})
def _cached_get_json(url: str, params: Optional[dict], oauth: OAuth1, timeout: int = 20) -> Dict[str, Any]:
    resp = requests.get(url, params=params, auth=oauth, timeout=timeout)
    resp.raise_for_status()
    try:
        return resp.json()
    except Exception:
        return {"_raw": resp.text}


@st.cache_data(ttl=86400, show_spinner=False)
def _cached_brickset(payload: dict) -> Dict[str, Any]:
    url = "https://brickset.com/api/v3.asmx/getSets"
    resp = requests.post(url, data=payload, timeout=20)
    try:
        return resp.json()
    except Exception:
        return {"status": "error", "http": resp.status_code, "body": resp.text[:400]}


@st.cache_data(ttl=86400, show_spinner=False)
def _cached_brickeconomy(url: str, headers: dict, params: dict):
    resp = requests.get(url, headers=headers, params=params, timeout=20)
    try:
        return resp.status_code, resp.json()
    except Exception:
        return resp.status_code, None


# ---------------------
# API wrappers (BrickLink)
# ---------------------

def bl_fetch_set_metadata(set_number: str, oauth: OAuth1) -> Dict[str, Any]:
    url = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}"
    try:
        raw = _cached_get_json(url, params=None, oauth=oauth)
        data = raw.get("data", {}) or {}
        out = {"Set Name": data.get("name"), "Category ID": data.get("category_id")}
        log_query(source="BrickLink:metadata", set_number=set_number, params={"url": url}, cache_hit=True, summary=out.get("Set Name") or "")
        return out
    except Exception as e:
        log_query(source="BrickLink:metadata", set_number=set_number, params={"url": url}, cache_hit=False, summary=f"ERR {e}")
        return {"_error": str(e)}


def bl_fetch_image_url(set_number: str, oauth: OAuth1) -> str:
    url = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}/images/0"
    try:
        raw = _cached_get_json(url, params=None, oauth=oauth)
        img = (raw.get("data") or {}).get("thumbnail_url") or ""
        log_query(source="BrickLink:image", set_number=set_number, params={"url": url}, cache_hit=True, summary="thumb")
        return img
    except Exception as e:
        log_query(source="BrickLink:image", set_number=set_number, params={"url": url}, cache_hit=False, summary=f"ERR {e}")
        return ""


def bl_fetch_price(set_number: str, oauth: OAuth1, guide_type: str = "stock", new_or_used: str = "N") -> Dict[str, Any]:
    url = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}/price"
    params = {"guide_type": guide_type, "new_or_used": new_or_used}
    try:
        raw = _cached_get_json(url, params=params, oauth=oauth)
        data = raw.get("data", {}) or {}
        log_query(
            source=f"BrickLink:price:{guide_type}:{new_or_used}",
            set_number=set_number,
            params=params,
            cache_hit=True,
            summary=f"avg={data.get('avg_price')}"
        )
        return data
    except Exception as e:
        log_query(
            source=f"BrickLink:price:{guide_type}:{new_or_used}",
            set_number=set_number,
            params=params,
            cache_hit=False,
            summary=f"ERR {e}"
        )
        return {"_error": str(e)}


# ---------------------
# API wrappers (BrickSet)
# ---------------------

def brickset_fetch(set_number: str, api_key: str) -> Dict[str, Any]:
    payload = {
        "apiKey": api_key,
        "userHash": "",
        "params": json.dumps({"setNumber": set_number, "extendedData": 1}),
    }
    try:
        data = _cached_brickset(payload)
        if data.get("status") == "success" and data.get("matches", 0) > 0:
            s = (data.get("sets") or [{}])[0]
            collections = s.get("collections", {}) or {}
            out = {
                "Set Name (BrickSet)": s.get("name", "N/A"),
                "Pieces": s.get("pieces", "N/A"),
                "Minifigs": s.get("minifigs", "N/A"),
                "Theme": s.get("theme", "N/A"),
                "Year": s.get("year", "N/A"),
                "Rating": s.get("rating", "N/A"),
                "Users Owned": collections.get("ownedBy", "N/A"),
                "Users Wanted": collections.get("wantedBy", "N/A"),
            }
            log_query(source="BrickSet:getSets", set_number=set_number, params={"extendedData": 1}, cache_hit=True, summary=out["Set Name (BrickSet)"])
            return out
        else:
            log_query(source="BrickSet:getSets", set_number=set_number, params={"extendedData": 1}, cache_hit=True, summary="no match")
            return {"_error": "No match"}
    except Exception as e:
        log_query(source="BrickSet:getSets", set_number=set_number, params={"extendedData": 1}, cache_hit=False, summary=f"ERR {e}")
        return {"_error": str(e)}


# ---------------------
# API wrappers (BrickEconomy)
# ---------------------

def brickeconomy_fetch(set_number: str, api_key: str, currency: str = "USD") -> Dict[str, Any]:
    url = f"https://www.brickeconomy.com/api/v1/set/{set_number}"
    params = {"currency": currency} if currency else {}
    headers = {
        "accept": "application/json",
        "x-apikey": api_key,
        "User-Agent": "ReUseBricks-Streamlit-App/1.0",
    }
    try:
        status, payload = _cached_brickeconomy(url, headers, params)
        data = payload.get("data") if (status == 200 and isinstance(payload, dict)) else None
        if not data:
            log_query(source="BrickEconomy:set", set_number=set_number, params={"currency": currency}, cache_hit=True, summary=f"HTTP {status}")
            return {"_error": f"No data (HTTP {status})"}
        out = {
            "Name": data.get("name"),
            "Theme": data.get("theme"),
            "Year": data.get("year"),
            "Retail Price": (data.get("msrp") or {}).get("value"),
            "Current Value": (data.get("current_value") or {}).get("value"),
            "Growth %": data.get("growth_percentage"),
            "URL": data.get("url"),
        }
        log_query(source="BrickEconomy:set", set_number=set_number, params={"currency": currency}, cache_hit=True, summary=out["Name"] or "")
        return out
    except Exception as e:
        log_query(source="BrickEconomy:set", set_number=set_number, params={"currency": currency}, cache_hit=False, summary=f"ERR {e}")
        return {"_error": str(e)}


# ---------------------
# UI Components
# ---------------------
with st.sidebar:
    # Show log first so it's visible even if later widgets error
    with st.expander("Search Log (today)", expanded=True):
        rows = get_todays_log()
        if rows:
            for ts_utc, source, sn, ph, cache_hit, summary in rows[:200]:
                st.markdown(f"- `{ts_utc}` • **{source}** • *{sn}* • {'cache' if cache_hit else 'live'} • {summary or ''}")
            if st.button("Clear today's log", key="btn_clear_log"):
                clear_todays_log()
                st.success("Cleared today's log.")
        else:
            st.caption("No queries yet today.")

    st.title("ReUseBricks")
    st.subheader("Credentials")
    with st.expander("BrickLink OAuth1", expanded=False):
        bl_consumer_key = st.text_input("Consumer Key", type="password", key="bl_consumer_key")
        bl_consumer_secret = st.text_input("Consumer Secret", type="password", key="bl_consumer_secret")
        bl_token = st.text_input("Token", type="password", key="bl_token")
        bl_token_secret = st.text_input("Token Secret", type="password", key="bl_token_secret")
    with st.expander("BrickSet", expanded=False):
        brickset_key = st.text_input("BrickSet API Key", type="password", key="brickset_api_key")
    with st.expander("BrickEconomy", expanded=False):
        be_key = st.text_input("BrickEconomy API Key", type="password", key="brickeconomy_api_key")
        be_currency = st.text_input("Currency (e.g., USD, EUR)", value="USD", key="brickeconomy_currency")

st.title("LEGO Purchasing Assistant")
raw_sets = st.text_area("Enter set numbers (comma or newline separated)")
set_list_raw = parse_set_input(raw_sets)
set_list = [normalize_set_number(s) for s in set_list_raw]

colA, colB = st.columns([1, 2])
with colA:
    st.write("")
    st.caption("Duplicate queries within 24h are served from cache and still logged.")

# Tabs for sources
Tabs = st.tabs(["BrickLink", "BrickSet", "BrickEconomy", "Combined View"])

# ---------------------
# BrickLink Tab
# ---------------------
with Tabs[0]:
    st.subheader("BrickLink")
    bl_ready = all([
        'bl_consumer_key' in st.session_state and st.session_state.bl_consumer_key,
        'bl_consumer_secret' in st.session_state and st.session_state.bl_consumer_secret,
        'bl_token' in st.session_state and st.session_state.bl_token,
        'bl_token_secret' in st.session_state and st.session_state.bl_token_secret,
    ])
    if not bl_ready:
        st.info("Enter BrickLink OAuth1 credentials in the sidebar.")
    else:
        oauth = OAuth1(
            client_key=st.session_state.bl_consumer_key,
            client_secret=st.session_state.bl_consumer_secret,
            resource_owner_key=st.session_state.bl_token,
            resource_owner_secret=st.session_state.bl_token_secret,
        )
        guide_type = st.selectbox("Guide Type", ["stock", "sold"], index=0)
        condition = st.selectbox("Condition", ["N", "U"], index=0, help="N = New, U = Used")
        if st.button("Fetch BrickLink Data", key="btn_bl"):
            if not set_list:
                st.warning("Please enter at least one set number.")
            rows_out = []
            for s in set_list:
                log_query(source="UI:BrickLink:bulk", set_number=s, params={"action": "bulk_fetch"}, cache_hit=True, summary="requested")
                meta = bl_fetch_set_metadata(s, oauth)
                price = bl_fetch_price(s, oauth, guide_type=guide_type, new_or_used=condition)
                img = bl_fetch_image_url(s, oauth)
                row = {
                    "Set": s,
                    "Name": meta.get("Set Name"),
                    "Avg Price": price.get("avg_price"),
                    "Qty Avg Price": price.get("qty_avg_price"),
                    "Unit Price Min": price.get("min_price"),
                    "Unit Price Max": price.get("max_price"),
                    "Currency": price.get("currency_code"),
                    "Image": img,
                }
                rows_out.append(row)
            if rows_out:
                df = pd.DataFrame(rows_out)
                st.dataframe(df, use_container_width=True)

# ---------------------
# BrickSet Tab
# ---------------------
with Tabs[1]:
    st.subheader("BrickSet")
    brickset_key_val = st.session_state.get("brickset_api_key", "")
    if not brickset_key_val:
        st.info("Enter BrickSet API key in the sidebar.")
    else:
        if st.button("Fetch BrickSet Data", key="btn_bs"):
            if not set_list:
                st.warning("Please enter at least one set number.")
            rows_out = []
            for s in set_list:
                log_query(source="UI:BrickSet:bulk", set_number=s, params={"action": "bulk_fetch"}, cache_hit=True, summary="requested")
                bs = brickset_fetch(s, brickset_key_val)
                row = {"Set": s}
                row.update(bs)
                rows_out.append(row)
            if rows_out:
                df = pd.DataFrame(rows_out)
                st.dataframe(df, use_container_width=True)

# ---------------------
# BrickEconomy Tab
# ---------------------
with Tabs[2]:
    st.subheader("BrickEconomy")
    be_key_val = st.session_state.get("brickeconomy_api_key", "")
    be_currency_val = st.session_state.get("brickeconomy_currency", "USD") or "USD"
    if not be_key_val:
        st.info("Enter BrickEconomy API key in the sidebar.")
    else:
        if st.button("Fetch BrickEconomy Data", key="btn_be"):
            if not set_list:
                st.warning("Please enter at least one set number.")
            rows_out = []
            for s in set_list:
                log_query(source="UI:BrickEconomy:bulk", set_number=s, params={"action": "bulk_fetch"}, cache_hit=True, summary="requested")
                be = brickeconomy_fetch(s, be_key_val, currency=be_currency_val)
                row = {"Set": s}
                row.update(be)
                rows_out.append(row)
            if rows_out:
                df = pd.DataFrame(rows_out)
                st.dataframe(df, use_container_width=True)

# ---------------------
# Combined View Tab
# ---------------------
with Tabs[3]:
    st.subheader("Combined View (quick demo)")
    st.caption("Fetches each source and merges on Set. Extend/modify as needed for your scoring pipeline.")
    run = st.button("Fetch All Sources", key="btn_all")
    if run:
        if not set_list:
            st.warning("Please enter at least one set number.")
        rows = []
        # Prepare OAuth if ready
        oauth = None
        if all([
            'bl_consumer_key' in st.session_state and st.session_state.bl_consumer_key,
            'bl_consumer_secret' in st.session_state and st.session_state.bl_consumer_secret,
            'bl_token' in st.session_state and st.session_state.bl_token,
            'bl_token_secret' in st.session_state and st.session_state.bl_token_secret,
        ]):
            oauth = OAuth1(
                client_key=st.session_state.bl_consumer_key,
                client_secret=st.session_state.bl_consumer_secret,
                resource_owner_key=st.session_state.bl_token,
                resource_owner_secret=st.session_state.bl_token_secret,
            )
        for s in set_list:
            record: Dict[str, Any] = {"Set": s}
            # BrickLink (optional if creds)
            if oauth:
                log_query(source="UI:BrickLink:bulk", set_number=s, params={"action": "bulk_fetch"}, cache_hit=True, summary="requested")
                meta = bl_fetch_set_metadata(s, oauth)
                price = bl_fetch_price(s, oauth)
                record.update({
                    "BL Name": meta.get("Set Name"),
                    "BL Avg Price": price.get("avg_price"),
                    "BL Currency": price.get("currency_code"),
                })
            # BrickSet (optional)
            if st.session_state.get("brickset_api_key"):
                log_query(source="UI:BrickSet:bulk", set_number=s, params={"action": "bulk_fetch"}, cache_hit=True, summary="requested")
                bs = brickset_fetch(s, st.session_state["brickset_api_key"]) 
                record.update({
                    "BS Name": bs.get("Set Name (BrickSet)"),
                    "Pieces": bs.get("Pieces"),
                    "Minifigs": bs.get("Minifigs"),
                    "BS Theme": bs.get("Theme"),
                    "BS Year": bs.get("Year"),
                })
            # BrickEconomy (optional)
            if st.session_state.get("brickeconomy_api_key"):
                log_query(source="UI:BrickEconomy:bulk", set_number=s, params={"action": "bulk_fetch"}, cache_hit=True, summary="requested")
                be = brickeconomy_fetch(s, st.session_state["brickeconomy_api_key"], currency=st.session_state.get("brickeconomy_currency", "USD"))
                record.update({
                    "BE Name": be.get("Name"),
                    "BE Current Value": be.get("Current Value"),
                    "BE Growth %": be.get("Growth %"),
                })
            rows.append(record)
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True)

# ---------------------
# Footer
# ---------------------
st.markdown(
    """
    <small>Tip: Cache TTL is 24h. To bypass cache (e.g., for debugging), change a parameter, or clear Streamlit cache from the app menu.</small>
    """,
    unsafe_allow_html=True,
)
