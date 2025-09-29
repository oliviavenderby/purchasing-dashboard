# purchasing_dashboard.py
# Streamlit app: LEGO purchasing assistant with 24h API caching and a persistent query log

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

# ---------------------
# Query Log Helpers
# ---------------------
DB_PATH = "search_log.db"

def _init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS query_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc TEXT NOT NULL,
            source TEXT NOT NULL,
            set_number TEXT NOT NULL,
            params_hash TEXT NOT NULL,
            cache_hit INTEGER NOT NULL,
            summary TEXT
        )
    """)
    conn.commit()
    conn.close()

def _hash_params(d: dict) -> str:
    blob = json.dumps(d or {}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()[:16]

def log_query(*, source: str, set_number: str, params: dict, cache_hit: bool, summary: str = ""):
    _init_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO query_log (ts_utc, source, set_number, params_hash, cache_hit, summary) VALUES (?,?,?,?,?,?)",
        (datetime.now(timezone.utc).isoformat(timespec="seconds"), source, set_number, _hash_params(params), int(cache_hit), summary[:300])
    )
    conn.commit()
    conn.close()

def get_todays_log():
    _init_db()
    today = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT ts_utc, source, set_number, params_hash, cache_hit, summary
        FROM query_log
        WHERE substr(ts_utc,1,10)=?
        ORDER BY ts_utc DESC
    """, (today,))
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
# Helpers
# ---------------------
def normalize_set_number(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    return s if "-" in s else f"{s}-1"

def parse_set_input(raw: str) -> List[str]:
    if not raw:
        return []
    # Fix: properly close the string literals
    parts = [p.strip() for p in raw.replace("\n", ",").split(",")]
    return [p for p in parts if p]

# ---------------------
# Cached HTTP
# ---------------------
@st.cache_data(ttl=86400, hash_funcs={OAuth1: lambda _: "oauth1"})
def _cached_get_json(url: str, params: Optional[dict], oauth: OAuth1, timeout: int=20):
    r = requests.get(url, params=params, auth=oauth, timeout=timeout)
    r.raise_for_status()
    return r.json()

@st.cache_data(ttl=86400)
def _cached_post_json(url: str, payload: dict):
    r = requests.post(url, data=payload, timeout=20)
    return r.json()

@st.cache_data(ttl=86400)
def _cached_get_json_noauth(url: str, headers: dict, params: dict):
    r = requests.get(url, headers=headers, params=params, timeout=20)
    return r.status_code, r.json()

# ---------------------
# API Wrappers
# ---------------------
def bl_fetch_set_metadata(set_number: str, oauth: OAuth1):
    url = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}"
    data = _cached_get_json(url, None, oauth).get("data", {})
    log_query(source="BrickLink:metadata", set_number=set_number, params={"url": url}, cache_hit=True, summary=data.get("name",""))
    return {"Set Name": data.get("name"), "Category ID": data.get("category_id")}

def bl_fetch_price(set_number: str, oauth: OAuth1, guide_type: str, cond: str):
    url = f"https://api.bricklink.com/api/store/v1/items/SET/{set_number}/price"
    params = {"guide_type": guide_type, "new_or_used": cond}
    data = _cached_get_json(url, params, oauth).get("data", {})
    log_query(source=f"BrickLink:price:{guide_type}:{cond}", set_number=set_number, params=params, cache_hit=True, summary=f"avg={data.get('avg_price')}")
    return data

def brickset_fetch(set_number: str, api_key: str):
    payload = {"apiKey": api_key, "userHash": "", "params": json.dumps({"setNumber": set_number, "extendedData": 1})}
    data = _cached_post_json("https://brickset.com/api/v3.asmx/getSets", payload)
    if data.get("status") == "success" and data.get("matches", 0) > 0:
        s = data["sets"][0]
        return {"Set Name (BrickSet)": s.get("name"), "Pieces": s.get("pieces"), "Minifigs": s.get("minifigs"), "Theme": s.get("theme"), "Year": s.get("year"), "Rating": s.get("rating"), "Users Owned": s.get("collections",{}).get("ownedBy"), "Users Wanted": s.get("collections",{}).get("wantedBy")}
    return {"_error": "no match"}

def brickeconomy_fetch(set_number: str, api_key: str, currency: str="USD"):
    url = f"https://www.brickeconomy.com/api/v1/set/{set_number}"
    headers = {"accept":"application/json","x-apikey":api_key,"User-Agent":"ReUseBricks-Streamlit-App/1.0"}
    status, payload = _cached_get_json_noauth(url, headers, {"currency":currency})
    data = payload.get("data") if status==200 else {}
    return {"Name": data.get("name"), "Theme": data.get("theme"), "Year": data.get("year"), "Current Value": data.get("current_value",{}).get("value")}

# ---------------------
# Sidebar
# ---------------------
with st.sidebar:
    with st.expander("Search Log (today)", expanded=True):
        rows = get_todays_log()
        if rows:
            for ts_utc, source, sn, ph, cache_hit, summary in rows[:100]:
                st.markdown(f"- `{ts_utc}` • **{source}** • {sn} • {'cache' if cache_hit else 'live'} • {summary}")
            if st.button("Clear today's log"):
                clear_todays_log()
                st.experimental_rerun()
        else:
            st.caption("No queries yet today.")
    st.title("ReUseBricks")
    bl_consumer_key = st.text_input("BrickLink Consumer Key", type="password")
    bl_consumer_secret = st.text_input("BrickLink Consumer Secret", type="password")
    bl_token = st.text_input("BrickLink Token", type="password")
    bl_token_secret = st.text_input("BrickLink Token Secret", type="password")
    brickset_key = st.text_input("BrickSet API Key", type="password")
    be_key = st.text_input("BrickEconomy API Key", type="password")
    be_currency = st.text_input("Currency", value="USD")

# ---------------------
# Main
# ---------------------
st.title("LEGO Purchasing Assistant")
raw_sets = st.text_area("Enter set numbers")
set_list = [normalize_set_number(s) for s in parse_set_input(raw_sets)]

Tabs = st.tabs(["BrickLink","BrickSet","BrickEconomy"])

with Tabs[0]:
    if st.button("Fetch BrickLink") and all([bl_consumer_key, bl_consumer_secret, bl_token, bl_token_secret]):
        oauth = OAuth1(bl_consumer_key, bl_consumer_secret, bl_token, bl_token_secret)
        out=[]
        for s in set_list:
            meta = bl_fetch_set_metadata(s, oauth)
            price = bl_fetch_price(s, oauth, "stock", "N")
            out.append({"Set":s, "Name":meta.get("Set Name"), "Avg Price":price.get("avg_price")})
        st.dataframe(pd.DataFrame(out))

with Tabs[1]:
    if st.button("Fetch BrickSet") and brickset_key:
        out=[]
        for s in set_list:
            bs = brickset_fetch(s, brickset_key)
            row={"Set":s}
            row.update(bs)
            out.append(row)
        st.dataframe(pd.DataFrame(out))

with Tabs[2]:
    if st.button("Fetch BrickEconomy") and be_key:
        out=[]
        for s in set_list:
            be = brickeconomy_fetch(s, be_key, be_currency)
            row={"Set":s}
            row.update(be)
            out.append(row)
        st.dataframe(pd.DataFrame(out))
