# purchasing_dashboard.py
# LEGO Purchasing Assistant with 24h caching + per‑source History tables + Scoring tab

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

st.set_page_config(page_title="LEGO Purchasing Assistant", layout="wide")

# =====================
# Query Log + Result Store (SQLite)
# =====================
DB_PATH = os.environ.get("QUERY_LOG_DB_PATH", "search_log.db")


def _init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # Log of queries
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
    # Persistent results so History can show full data without re-calling APIs
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS query_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc TEXT NOT NULL,
            source TEXT NOT NULL,
            set_number TEXT NOT NULL,
            params_hash TEXT NOT NULL,
            payload TEXT NOT NULL
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


def save_result(*, source: str, set_number: str, params: dict, payload: dict):
    """Persist the exact table row we showed to the user so History can render it without any API calls."""
    _init_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO query_results (ts_utc, source, set_number, params_hash, payload) VALUES (?,?,?,?,?)",
        (
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
            source,
            set_number,
            _hash_params(params),
            json.dumps(payload, separators=(",", ":")),
        ),
    )
    conn.commit()
    conn.close()


def results_today_df(source_exact: str) -> pd.DataFrame:
    """Return today's saved table rows for a given source (exact match), newest first."""
    _init_db()
    today = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        SELECT ts_utc, set_number, payload
        FROM query_results
        WHERE substr(ts_utc,1,10)=? AND source = ?
        ORDER BY ts_utc DESC
        """,
        (today, source_exact),
    )
    rows = c.fetchall()
    conn.close()
    parsed = []
    for ts_utc, set_number, payload in rows:
        try:
            row = json.loads(payload)
            row = {**{"Time (UTC)": ts_utc, "Set": set_number}, **row}
            parsed.append(row)
        except Exception:
            continue
    df = pd.DataFrame(parsed)
    return df


def clear_history_today():
    _init_db()
    today = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM query_log WHERE substr(ts_utc,1,10)=?", (today,))
    c.execute("DELETE FROM query_results WHERE substr(ts_utc,1,10)=?", (today,))
    conn.commit()
    conn.close()


# =====================
# Helpers
# =====================
def normalize_set_number(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    return s if "-" in s else f"{s}-1"


def parse_set_input(raw: str) -> List[str]:
    if not raw:
        return []
    parts = [p.strip() for p in raw.replace("\n", ",").split(",")]
    return [p for p in parts if p]


# =====================
# Cached HTTP
# =====================
@st.cache_data(ttl=86400, hash_funcs={OAuth1: lambda _: "oauth1"})
def _cached_get_json(url: str, params: Optional[dict], oauth: OAuth1, timeout: int = 20) -> Dict[str, Any]:
    r = requests.get(url, params=params, auth=oauth, timeout=timeout)
    r.raise_for_status()
    return r.json()


@st.cache_data(ttl=86400)
def _cached_post_json(url: str, payload: dict) -> Dict[str, Any]:
    r = requests.post(url, data=payload, timeout=20)
    return r.json()


@st.cache_data(ttl=86400)
def _cached_get_json_noauth(url: str, headers: dict, params: dict):
    r = requests.get(url, headers=headers, params=params, timeout=20)
    return r.status_code, r.json()


# =====================
# API Wrappers + Logging
# =====================

def bl_fetch_set_metadata(set_number: str, oauth: OAuth1) -> Dict[str, Any]:
    url = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}"
    raw = _cached_get_json(url, None, oauth)
    data = raw.get("data", {}) or {}
    log_query(source="BrickLink:metadata", set_number=set_number, params={"url": url}, cache_hit=True, summary=data.get("name") or "")
    return {"Set Name": data.get("name"), "Category ID": data.get("category_id")}


def bl_fetch_price(set_number: str, oauth: OAuth1, guide_type: str = "stock", new_or_used: str = "N") -> Dict[str, Any]:
    url = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}/price"
    params = {"guide_type": guide_type, "new_or_used": new_or_used}
    raw = _cached_get_json(url, params, oauth)
    data = raw.get("data", {}) or {}
    log_query(source=f"BrickLink:price:{guide_type}:{new_or_used}", set_number=set_number, params=params, cache_hit=True, summary=f"avg={data.get('avg_price')}")
    return data


def brickset_fetch(set_number: str, api_key: str) -> Dict[str, Any]:
    payload = {"apiKey": api_key, "userHash": "", "params": json.dumps({"setNumber": set_number, "extendedData": 1})}
    data = _cached_post_json("https://brickset.com/api/v3.asmx/getSets", payload)
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
    log_query(source="BrickSet:getSets", set_number=set_number, params={"extendedData": 1}, cache_hit=True, summary="no match")
    return {"_error": "No match"}


def brickeconomy_fetch(set_number: str, api_key: str, currency: str = "USD") -> Dict[str, Any]:
    url = f"https://www.brickeconomy.com/api/v1/set/{set_number}"
    headers = {"accept": "application/json", "x-apikey": api_key, "User-Agent": "ReUseBricks-Streamlit-App/1.0"}
    status, payload = _cached_get_json_noauth(url, headers, {"currency": currency})
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


# =====================
# Sidebar
# =====================
with st.sidebar:
    st.title("ReUseBricks")
    st.subheader("Credentials")
    with st.expander("BrickLink OAuth1"):
        st.text_input("Consumer Key", type="password", key="bl_consumer_key")
        st.text_input("Consumer Secret", type="password", key="bl_consumer_secret")
        st.text_input("Token", type="password", key="bl_token")
        st.text_input("Token Secret", type="password", key="bl_token_secret")
    with st.expander("BrickSet"):
        st.text_input("BrickSet API Key", type="password", key="brickset_api_key")
    with st.expander("BrickEconomy"):
        st.text_input("BrickEconomy API Key", type="password", key="brickeconomy_api_key")
        st.text_input("Currency (e.g., USD, EUR)", value="USD", key="brickeconomy_currency")
    st.markdown("---")
    if st.button("Clear today's history", key="btn_clear_history"):
        clear_history_today()
        st.success("Cleared.")

# =====================
# Main
# =====================
st.title("LEGO Purchasing Assistant")
raw_sets = st.text_area("Enter set numbers (comma or newline separated)")
set_list = [normalize_set_number(s) for s in parse_set_input(raw_sets)]

Tabs = st.tabs(["BrickLink", "BrickSet", "BrickEconomy", "Scoring"])

# BrickLink Tab
with Tabs[0]:
    st.subheader("BrickLink Data")
    creds_ok = all([
        st.session_state.get("bl_consumer_key"),
        st.session_state.get("bl_consumer_secret"),
        st.session_state.get("bl_token"),
        st.session_state.get("bl_token_secret"),
    ])
    if not creds_ok:
        st.info("Enter BrickLink OAuth1 credentials in the sidebar.")
    else:
        oauth = OAuth1(
            client_key=st.session_state.bl_consumer_key,
            client_secret=st.session_state.bl_consumer_secret,
            resource_owner_key=st.session_state.bl_token,
            resource_owner_secret=st.session_state.bl_token_secret,
        )
        if st.button("Fetch BrickLink Data", key="btn_fetch_bl"):
            rows = []
            for s in set_list:
                log_query(source="UI:BrickLink:fetch", set_number=s, params={"action": "fetch"}, cache_hit=True, summary="requested")
                meta = bl_fetch_set_metadata(s, oauth)
                price = bl_fetch_price(s, oauth, "stock", "N")
                img = bl_fetch_image_url(s, oauth)
                row_payload = {
                    "Name": meta.get("Set Name"),
                    "Avg Price": price.get("avg_price"),
                    "Qty Avg Price": price.get("qty_avg_price"),
                    "Min": price.get("min_price"),
                    "Max": price.get("max_price"),
                    "Currency": price.get("currency_code"),
                    "Image": img,
                }
                rows.append({"Set": s, **row_payload})
                save_result(source="BrickLink:row", set_number=s, params={"guide": "stock", "cond": "N"}, payload=row_payload)
            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True)
        st.markdown("### History (today)")
        hist_bl = results_today_df("BrickLink:row")
        st.dataframe(hist_bl if not hist_bl.empty else pd.DataFrame(columns=["Time (UTC)","Set","Name","Avg Price","Qty Avg Price","Min","Max","Currency","Image"]), use_container_width=True)

# BrickSet Tab
with Tabs[1]:
    st.subheader("BrickSet Data")
    api = st.session_state.get("brickset_api_key", "")
    if not api:
        st.info("Enter BrickSet API key in the sidebar.")
    else:
        if st.button("Fetch BrickSet Data", key="btn_fetch_bs"):
            rows = []
            for s in set_list:
                log_query(source="UI:BrickSet:fetch", set_number=s, params={"action": "fetch"}, cache_hit=True, summary="requested")
                data = brickset_fetch(s, api)
                row_payload = {
                    "Set Name (BrickSet)": data.get("Set Name (BrickSet)"),
                    "Pieces": data.get("Pieces"),
                    "Minifigs": data.get("Minifigs"),
                    "Theme": data.get("Theme"),
                    "Year": data.get("Year"),
                    "Rating": data.get("Rating"),
                    "Users Owned": data.get("Users Owned"),
                    "Users Wanted": data.get("Users Wanted"),
                }
                rows.append({"Set": s, **row_payload})
                save_result(source="BrickSet:row", set_number=s, params={}, payload=row_payload)
            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True)
        st.markdown("### History (today)")
        hist_bs = results_today_df("BrickSet:row")
        st.dataframe(hist_bs if not hist_bs.empty else pd.DataFrame(columns=["Time (UTC)","Set","Set Name (BrickSet)","Pieces","Minifigs","Theme","Year","Rating","Users Owned","Users Wanted"]), use_container_width=True)

# BrickEconomy Tab
with Tabs[2]:
    st.subheader("BrickEconomy Data")
    api = st.session_state.get("brickeconomy_api_key", "")
    currency = st.session_state.get("brickeconomy_currency", "USD") or "USD"
    if not api:
        st.info("Enter BrickEconomy API key in the sidebar.")
    else:
        if st.button("Fetch BrickEconomy Data", key="btn_fetch_be"):
            rows = []
            for s in set_list:
                log_query(source="UI:BrickEconomy:fetch", set_number=s, params={"action": "fetch"}, cache_hit=True, summary="requested")
                data = brickeconomy_fetch(s, api, currency)
                row_payload = {
                    "Name": data.get("Name"),
                    "Theme": data.get("Theme"),
                    "Year": data.get("Year"),
                    "Retail Price": data.get("Retail Price"),
                    "Current Value": data.get("Current Value"),
                    "Growth %": data.get("Growth %"),
                    "URL": data.get("URL"),
                }
                rows.append({"Set": s, **row_payload})
                save_result(source="BrickEconomy:row", set_number=s, params={"currency": currency}, payload=row_payload)
            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True)
        st.markdown("### History (today)")
        hist_be = results_today_df("BrickEconomy:row")
        st.dataframe(hist_be if not hist_be.empty else pd.DataFrame(columns=["Time (UTC)","Set","Name","Theme","Year","Retail Price","Current Value","Growth %","URL"]), use_container_width=True)

# Scoring Tab
with Tabs[3]:
    st.subheader("Scoring")
    if st.button("Compute Score (example)", key="btn_score"):
        scores = []
        api_bs = st.session_state.get("brickset_api_key", "")
        api_be = st.session_state.get("brickeconomy_api_key", "")
        cur = st.session_state.get("brickeconomy_currency", "USD") or "USD"
        for s in set_list:
            bs = brickset_fetch(s, api_bs) if api_bs else {}
            be = brickeconomy_fetch(s, api_be, cur) if api_be else {}
            pieces = (bs or {}).get("Pieces") or 0
            rating = (bs or {}).get("Rating") or 0
            value = (be or {}).get("Current Value") or 0
            try:
                p = float(pieces or 0)
                r = float(rating or 0)
                v = float(value or 0)
            except Exception:
                p, r, v = 0.0, 0.0, 0.0
            score = 0.4*(p/1000) + 0.4*r + 0.2*(v/100)
            scores.append({"Set": s, "Pieces": pieces, "Rating": rating, "Current Value": value, "Score": round(score, 2)})
        st.dataframe(pd.DataFrame(scores), use_container_width=True)
    st.markdown("### History (today – combined)")
    df_bl = results_today_df("BrickLink:row")
    df_bs = results_today_df("BrickSet:row")
    df_be = results_today_df("BrickEconomy:row")
    df_all = pd.concat([df_bl, df_bs, df_be], ignore_index=True) if not (df_bl.empty and df_bs.empty and df_be.empty) else pd.DataFrame()
    st.dataframe(df_all if not df_all.empty else pd.DataFrame(columns=["Time (UTC)","Set","Name","Set Name (BrickSet)","Pieces","Minifigs","Theme","Year","Rating","Users Owned","Users Wanted","Retail Price","Current Value","Growth %","Currency","Image","URL"]), use_container_width=True)

st.markdown("<small>Cache TTL: 24h. History shows today's queries and whether they were served from cache.</small>", unsafe_allow_html=True)

