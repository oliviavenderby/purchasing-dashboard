# purchasing_dashboard.py
# LEGO Purchasing Assistant with 24h caching + per-source History tables + Scoring tab

import os
import json
import sqlite3
import hashlib
import re
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
            row = {**{"Time (UTC)": ts_utc, "Item": set_number}, **row}
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
    """(Kept for other tabs) Add -1 to plain digits that are sets."""
    if not s:
        return ""
    s = s.strip()
    return s if "-" in s else f"{s}-1"


def parse_set_input(raw: str) -> List[str]:
    if not raw:
        return []
    parts = [p.strip() for p in raw.replace("\n", ",").split(",")]
    return [p for p in parts if p]


def infer_item_type_and_no(raw: str) -> tuple[str, str]:
    """
    Heuristics:
      - If string contains any letters -> MINIFIG (e.g., sw0701).
      - Else treat as SET; add '-1' if not present.
    """
    s = (raw or "").strip()
    if not s:
        return "SET", s
    if re.search(r"[A-Za-z]", s):
        return "MINIFIG", s
    return "SET", (s if "-" in s else f"{s}-1")


def get_public_ip() -> str:
    try:
        return requests.get("https://api.ipify.org", timeout=10).text
    except Exception:
        return "unknown"


def bl_raw_get(url_path: str, oauth: OAuth1):
    """Low-level GET for diagnostics."""
    url = f"https://api.bricklink.com/api/store/v1/{url_path.lstrip('/')}"
    r = requests.get(url, auth=oauth, timeout=20)
    try:
        body = r.json()
    except Exception:
        body = {"raw_text": r.text[:400]}
    return r.status_code, dict(r.headers), body


def _bl_cache_key() -> str:
    """Vary cache by current creds so new tokens take effect immediately."""
    vals = [
        st.session_state.get("bl_consumer_key", ""),
        st.session_state.get("bl_consumer_secret", ""),
        st.session_state.get("bl_token", ""),
        st.session_state.get("bl_token_secret", ""),
    ]
    return hashlib.sha256("|".join(vals).encode()).hexdigest()[:16]


# =====================
# Cached HTTP
# =====================
@st.cache_data(ttl=86400, hash_funcs={OAuth1: lambda _: "oauth1"})
def _cached_get_json(
    url: str,
    params: Optional[dict],
    oauth: OAuth1,
    cache_key: str,          # makes cache sensitive to credentials
    timeout: int = 20
) -> Dict[str, Any]:
    r = requests.get(url, params=params, auth=oauth, timeout=timeout)
    # Don't raise on 4xx; BrickLink returns useful meta
    try:
        return r.json()
    except Exception:
        return {"meta": {"code": r.status_code, "description": "non-JSON response"}, "raw_text": r.text[:400]}


@st.cache_data(ttl=86400)
def _cached_post_json(url: str, payload: dict) -> Dict[str, Any]:
    r = requests.post(url, data=payload, timeout=20)
    try:
        return r.json()
    except Exception:
        return {"status": "error", "raw_text": r.text[:400]}


@st.cache_data(ttl=86400)
def _cached_get_json_noauth(url: str, headers: dict, params: dict):
    r = requests.get(url, headers=headers, params=params, timeout=20)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {"raw_text": r.text[:400]}


# =====================
# BrickLink API Wrappers + Logging
# =====================

def bl_fetch_metadata(item_type: str, item_no: str, oauth: OAuth1) -> Dict[str, Any]:
    url = f"https://api.bricklink.com/api/store/v1/items/{item_type}/{item_no}"
    raw = _cached_get_json(url, None, oauth, _bl_cache_key())
    meta = raw.get("meta", {})
    data = raw.get("data")
    if not isinstance(data, dict):
        return {"_error": f"{meta.get('code', '?')}: {meta.get('description', 'No data')}"}
    log_query(
        source="BrickLink:metadata",
        set_number=item_no,
        params={"url": url, "type": item_type},
        cache_hit=True,
        summary=data.get("name") or "",
    )
    return {"Name": data.get("name"), "Category ID": data.get("category_id")}


def bl_fetch_price(
    item_type: str,
    item_no: str,
    oauth: OAuth1,
    guide_type: str = "stock",        # "stock" | "sold"
    new_or_used: str = "N",           # "N" | "U"
    currency_code: Optional[str] = None,
    country_code: Optional[str] = None,
    region: Optional[str] = None,     # asia, africa, north_america, south_america, middle_east, europe, eu, oceania
    color_id: Optional[int] = None,   # ignored for SET/MINIFIG
    vat: Optional[str] = None,        # "N" (default), "Y", "O"
) -> Dict[str, Any]:
    url = f"https://api.bricklink.com/api/store/v1/items/{item_type}/{item_no}/price"
    params: Dict[str, Any] = {"guide_type": guide_type, "new_or_used": new_or_used}
    if currency_code: params["currency_code"] = currency_code
    if country_code:  params["country_code"]  = country_code
    if region:        params["region"]        = region
    if color_id is not None: params["color_id"] = int(color_id)
    if vat in {"N", "Y", "O"}: params["vat"] = vat

    raw = _cached_get_json(url, params, oauth, _bl_cache_key())
    meta = raw.get("meta", {})
    data = raw.get("data")
    if not isinstance(data, dict):
        desc = meta.get("description") or meta.get("message") or "No data"
        return {"_error": f"{meta.get('code', '?')}: {desc}"}

    log_query(
        source=f"BrickLink:price:{item_type}:{guide_type}:{new_or_used}",
        set_number=item_no,
        params=params,
        cache_hit=True,
        summary=f"avg={data.get('avg_price')}",
    )
    return data


# =====================
# BrickSet Wrapper
# =====================

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
        log_query(
            source="BrickSet:getSets",
            set_number=set_number,
            params={"extendedData": 1},
            cache_hit=True,
            summary=out["Set Name (BrickSet)"],
        )
        return out
    log_query(
        source="BrickSet:getSets",
        set_number=set_number,
        params={"extendedData": 1},
        cache_hit=True,
        summary="no match",
    )
    return {"_error": "No match"}


# =====================
# BrickEconomy Wrappers (SET + MINIFIG)
# =====================

def _be_common_out(data: dict, set_or_fig_no: str, item_type: str, currency: str) -> Dict[str, Any]:
    """
    Normalize BrickEconomy payload for both Sets and Minifigs into a single row schema.
    """
    # Theme/Series can vary by object; try a few sensible keys
    theme = data.get("theme") or data.get("series") or data.get("subtheme") or data.get("category")
    # Year keys can vary as well
    year = data.get("year") or data.get("release_year") or data.get("first_year")

    # Retail only makes sense for sets (keep Nones for figs)
    retail_key_by_currency = {
        "USD": "retail_price_us",
        "GBP": "retail_price_uk",
        "CAD": "retail_price_ca",
        "EUR": "retail_price_eu",
        "AUD": "retail_price_au",
    }
    retail_val = None
    if item_type == "SET":
        retail_key = retail_key_by_currency.get(currency.upper(), "retail_price_us")
        retail_val = data.get(retail_key)

    return {
        "Item": set_or_fig_no,
        "Type": item_type,
        "Name": data.get("name") or data.get("title"),
        "Theme/Series": theme,
        "Year": year,
        "Retail Price": retail_val,
        "Current Value (New)": data.get("current_value_new"),
        "Current Value (Used)": data.get("current_value_used"),
        "Growth % (12m)": data.get("rolling_growth_12months"),
        "Currency": data.get("currency") or currency.upper(),
        "URL": f"https://www.brickeconomy.com/{'set' if item_type=='SET' else 'minifig'}/{set_or_fig_no}",
    }


def brickeconomy_fetch_any(item_type: str, code: str, api_key: str, currency: str = "USD") -> Dict[str, Any]:
    """
    Calls BrickEconomy for either a SET or a MINIFIG and returns a normalized row.
    """
    if item_type not in {"SET", "MINIFIG"}:
        return {"_error": f"Unsupported type: {item_type}"}

    endpoint = "set" if item_type == "SET" else "minifig"
    url = f"https://www.brickeconomy.com/api/v1/{endpoint}/{code}"
    headers = {
        "accept": "application/json",
        "x-apikey": api_key,
        "User-Agent": "ReUseBricks-Streamlit-App/1.0",
    }
    status, payload = _cached_get_json_noauth(url, headers, {"currency": currency})
    data = payload.get("data") if (status == 200 and isinstance(payload, dict)) else None
    if not data:
        log_query(
            source=f"BrickEconomy:{endpoint}",
            set_number=code,
            params={"currency": currency},
            cache_hit=True,
            summary=f"HTTP {status}",
        )
        return {"_error": (payload.get("error") if isinstance(payload, dict) else f"HTTP {status}")}

    out = _be_common_out(data, code, item_type, currency)
    log_query(
        source=f"BrickEconomy:{endpoint}",
        set_number=code,
        params={"currency": currency},
        cache_hit=True,
        summary=out["Name"] or "",
    )
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
# Keep this for tabs that only handle sets (BrickSet/Scoring). BrickLink/BrickEconomy infer per item.
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
            signature_method='HMAC-SHA1',
            signature_type='auth_header',
        )

        with st.expander("BrickLink diagnostics"):
            st.caption("If you see 401/403 or meta errors, (re)create the Access Token using this IP, or set IP to 0.0.0.0.")
            st.code(f"Server public IP: {get_public_ip()}", language="text")
            if st.button("Test BrickLink connection", key="btn_bl_diag"):
                code, headers, body = bl_raw_get("items/SET/75131-1", oauth)
                st.write("Status:", code)
                st.write("Headers:", headers)
                st.json(body)
            if st.button("Clear API cache", key="btn_clear_api_cache"):
                st.cache_data.clear()
                st.success("Cleared API cache.")

        if st.button("Fetch BrickLink Data", key="btn_fetch_bl"):
            rows = []
            per_item_details: Dict[str, Any] = {}

            for raw in parse_set_input(raw_sets):
                item_type, item_no = infer_item_type_and_no(raw)

                log_query(
                    source="UI:BrickLink:fetch",
                    set_number=item_no,
                    params={"action": "fetch", "type": item_type},
                    cache_hit=True,
                    summary="requested"
                )

                meta = bl_fetch_metadata(item_type, item_no, oauth)
                if "_error" in meta:
                    st.warning(f"{item_type} {item_no}: {meta['_error']}")

                price = bl_fetch_price(
                    item_type, item_no, oauth,
                    guide_type="stock",
                    new_or_used="N",
                    currency_code=None,
                    country_code=None,
                    region=None,
                    vat=None,
                )

                if "_error" in price:
                    st.warning(f"{item_type} {item_no}: {price['_error']}")
                    row_payload = {
                        "Name": meta.get("Name"),
                        "Avg Price": None,
                        "Qty Avg Price": None,
                        "Min": None,
                        "Max": None,
                        "Currency": None,
                        "Type": item_type,
                    }
                else:
                    row_payload = {
                        "Name": meta.get("Name"),
                        "Avg Price": price.get("avg_price"),
                        "Qty Avg Price": price.get("qty_avg_price"),
                        "Min": price.get("min_price"),
                        "Max": price.get("max_price"),
                        "Currency": price.get("currency_code"),
                        "Type": item_type,
                    }
                    per_item_details[item_no] = price.get("price_detail")

                rows.append({"Item": item_no, **row_payload})
                save_result(
                    source="BrickLink:row",
                    set_number=item_no,
                    params={"type": item_type, "guide": "stock", "cond": "N"},
                    payload=row_payload,
                )

            if rows:
                df = pd.DataFrame(rows)
                cols = ["Item", "Type", "Name", "Avg Price", "Qty Avg Price", "Min", "Max", "Currency"]
                st.dataframe(df[cols] if all(c in df.columns for c in cols) else df, use_container_width=True)

            for raw in parse_set_input(raw_sets):
                _, item_no = infer_item_type_and_no(raw)
                details = per_item_details.get(item_no)
                with st.expander(f"Price detail for {item_no}"):
                    if details:
                        df_pg = pd.json_normalize(details).rename(columns={
                            "seller_name": "Seller",
                            "seller_country_code": "Seller Country",
                            "seller_rating": "Seller Rating",
                            "unit_price": "Unit Price",
                            "new_or_used": "Condition",
                            "quantity": "Quantity",
                            "shipping_available": "Shipping Available",
                            "date_ordered": "Date Ordered",
                        })
                        st.dataframe(df_pg, use_container_width=True)
                    else:
                        st.info("No price_detail returned for the current parameters.")

        st.markdown("### History (today)")
        hist_bl = results_today_df("BrickLink:row")
        st.dataframe(
            hist_bl if not hist_bl.empty else pd.DataFrame(
                columns=["Time (UTC)", "Item", "Name", "Avg Price", "Qty Avg Price", "Min", "Max", "Currency"]
            ),
            use_container_width=True,
        )

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
        st.dataframe(
            hist_bs if not hist_bs.empty else pd.DataFrame(
                columns=["Time (UTC)", "Item", "Set Name (BrickSet)", "Pieces", "Minifigs", "Theme", "Year", "Rating", "Users Owned", "Users Wanted"]
            ),
            use_container_width=True,
        )

# BrickEconomy Tab (now supports SET + MINIFIG)
with Tabs[2]:
    st.subheader("BrickEconomy Data")
    api = st.session_state.get("brickeconomy_api_key", "")
    currency = st.session_state.get("brickeconomy_currency", "USD") or "USD"
    if not api:
        st.info("Enter BrickEconomy API key in the sidebar.")
    else:
        if st.button("Fetch BrickEconomy Data", key="btn_fetch_be"):
            rows = []
            # Parse raw input and infer per-item (SET or MINIFIG)
            for raw in parse_set_input(raw_sets):
                item_type, item_no = infer_item_type_and_no(raw)
                log_query(source="UI:BrickEconomy:fetch", set_number=item_no, params={"action": "fetch", "type": item_type}, cache_hit=True, summary="requested")

                data = brickeconomy_fetch_any(item_type, item_no, api, currency)

                if "_error" in data:
                    st.warning(f"{item_type} {item_no}: {data['_error']}")
                    # still add a row placeholder for visibility
                    row_payload = {
                        "Item": item_no,
                        "Type": item_type,
                        "Name": None,
                        "Theme/Series": None,
                        "Year": None,
                        "Retail Price": None,
                        "Current Value (New)": None,
                        "Current Value (Used)": None,
                        "Growth % (12m)": None,
                        "Currency": None,
                        "URL": None,
                    }
                else:
                    row_payload = data

                rows.append(row_payload)
                save_result(source="BrickEconomy:row", set_number=item_no, params={"currency": currency, "type": item_type}, payload=row_payload)

            if rows:
                df = pd.DataFrame(rows)
                cols = [
                    "Item", "Type", "Name", "Theme/Series", "Year",
                    "Retail Price", "Current Value (New)", "Current Value (Used)",
                    "Growth % (12m)", "Currency", "URL"
                ]
                st.dataframe(df[cols] if all(c in df.columns for c in cols) else df, use_container_width=True)

        st.markdown("### History (today)")
        hist_be = results_today_df("BrickEconomy:row")
        st.dataframe(
            hist_be if not hist_be.empty else pd.DataFrame(
                columns=[
                    "Time (UTC)", "Item", "Type", "Name", "Theme/Series", "Year",
                    "Retail Price", "Current Value (New)", "Current Value (Used)",
                    "Growth % (12m)", "Currency", "URL"
                ]
            ),
            use_container_width=True,
        )

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
            be = brickeconomy_fetch_any("SET", s, api_be, cur) if api_be else {}
            pieces = (bs or {}).get("Pieces") or 0
            rating = (bs or {}).get("Rating") or 0
            value = (be or {}).get("Current Value (New)") or (be or {}).get("Current Value") or 0
            try:
                p = float(pieces or 0)
                r = float(rating or 0)
                v = float(value or 0)
            except Exception:
                p, r, v = 0.0, 0.0, 0.0
            score_val = 0.4*(p/1000) + 0.4*r + 0.2*(v/100)

            row_payload = {"Pieces": pieces, "BrickSet Rating": rating, "Current Value": value, "Score": round(score_val, 2)}
            scores.append({"Set": s, **row_payload})

            save_result(
                source="Scoring:row",
                set_number=s,
                params={"formula":"0.4p/1000+0.4r+0.2v/100"},
                payload=row_payload
            )

        df_scores = pd.DataFrame(scores)
        cols = ["Set", "Pieces", "BrickSet Rating", "Current Value", "Score"]
        st.dataframe(df_scores[cols] if all(c in df_scores.columns for c in cols) else df_scores,
                     use_container_width=True)

    st.markdown("### History (today – Scoring)")
    hist_sc = results_today_df("Scoring:row")
    st.dataframe(
        hist_sc if not hist_sc.empty else pd.DataFrame(
            columns=["Time (UTC)", "Item", "Pieces", "BrickSet Rating", "Current Value", "Score"]
        ),
        use_container_width=True,
    )

st.markdown("<small>Cache TTL: 24h. History shows today's queries and whether they were served from cache.</small>", unsafe_allow_html=True)
