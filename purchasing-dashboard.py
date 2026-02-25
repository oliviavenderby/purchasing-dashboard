# purchasing_dashboard.py
# LEGO Purchasing Assistant with 24h caching + per-source History tables + Scoring tab
# UPDATED: History now shows a rolling 7-day window (configurable) instead of "today" only.
# UPDATED (Scoring): Adds BrickSet Owned/Wanted ratios + Wanted/Owned, displayed as percentages/ratio in UI
# while saving raw numeric ratios to SQLite history. Final score equation unchanged.
# UPDATED (Demand): Adds smoothed Demand Index + ABSOLUTE Demand Score (0-10) (stable over time; NOT based on current input list)

import os
import json
import sqlite3
import hashlib
import re
from datetime import datetime, timezone, date, timedelta
from typing import Optional, List, Dict, Any

import requests
import pandas as pd
import streamlit as st
from requests_oauthlib import OAuth1

# Load API keys and other constants from Streamlit secrets.
# Configure these in Streamlit Cloud (or .streamlit/secrets.toml) rather than hard-coding.
BL_CONSUMER_KEY = st.secrets.get("BRICKLINK_CONSUMER_KEY", "")
BL_CONSUMER_SECRET = st.secrets.get("BRICKLINK_CONSUMER_SECRET", "")
BL_TOKEN = st.secrets.get("BRICKLINK_TOKEN", "")
BL_TOKEN_SECRET = st.secrets.get("BRICKLINK_TOKEN_SECRET", "")

BRICKSET_API_KEY = st.secrets.get("BRICKSET_API_KEY", "")

BRICKECONOMY_API_KEY = st.secrets.get("BRICKECONOMY_API_KEY", "")
BRICKECONOMY_CURRENCY = st.secrets.get("BRICKECONOMY_CURRENCY", "USD") or "USD"

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
    # Results store
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS results_store (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc TEXT NOT NULL,
            source TEXT NOT NULL,
            set_number TEXT NOT NULL,
            params_hash TEXT NOT NULL,
            payload_json TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


_init_db()


def _hash_params(params: Dict[str, Any]) -> str:
    """Deterministic tiny hash of params for logging / store dedupe."""
    if not params:
        return "no-params"
    s = json.dumps(params, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode()).hexdigest()[:16]


def log_query(
    *,
    source: str,
    set_number: str,
    params: Optional[Dict[str, Any]],
    cache_hit: bool,
    summary: Optional[str] = None,
):
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


def save_result(
    *,
    source: str,
    set_number: str,
    params: Optional[Dict[str, Any]],
    payload: Dict[str, Any],
    cache_hit: bool = False,
    summary: Optional[str] = None,
):
    """
    Upsert into results_store keyed by (source,set_number,params_hash),
    and ensure a matching row exists in query_log for history joins.
    """
    key_hash = _hash_params(params)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        SELECT id FROM results_store
        WHERE source=? AND set_number=? AND params_hash=?
        """,
        (source, set_number, key_hash),
    )
    row = c.fetchone()
    ts_now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    if row:
        c.execute(
            """
            UPDATE results_store
            SET ts_utc=?, payload_json=?
            WHERE id=?
            """,
            (ts_now, json.dumps(payload), row[0]),
        )
    else:
        c.execute(
            """
            INSERT INTO results_store (ts_utc, source, set_number, params_hash, payload_json)
            VALUES (?,?,?,?,?)
            """,
            (ts_now, source, set_number, key_hash, json.dumps(payload)),
        )

    conn.commit()
    conn.close()

    # Make sure history has something to join against
    log_query(
        source=source,
        set_number=set_number,
        params=params,
        cache_hit=cache_hit,
        summary=summary,
    )


def results_last_n_days_df(source_prefix: str, days: int = 7) -> pd.DataFrame:
    """
    Return last N days of results for a given source prefix, joined with query_log.
    Uses a rolling window in UTC based on the stored ISO ts_utc values.
    """
    start_dt = datetime.now(timezone.utc) - timedelta(days=days)
    start_iso = start_dt.isoformat(timespec="seconds")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        SELECT
            q.ts_utc,
            r.source,
            r.set_number,
            r.params_hash,
            r.payload_json
        FROM results_store r
        JOIN query_log q
          ON r.source = q.source
         AND r.set_number = q.set_number
         AND r.params_hash = q.params_hash
        WHERE r.source LIKE ?
          AND q.ts_utc >= ?
        ORDER BY q.ts_utc DESC
        """,
        (f"{source_prefix}%", start_iso),
    )
    rows = c.fetchall()
    conn.close()

    records = []
    for ts_utc, src, set_number, _p_hash, payload_json in rows:
        try:
            payload = json.loads(payload_json)
        except Exception:
            payload = {"raw": payload_json}
        rec = {
            "Time (UTC)": ts_utc,
            "Source": src,
            "Item": set_number,
            **payload,
        }
        records.append(rec)

    return pd.DataFrame(records)


def clear_history_today():
    """Delete today's query_log + results_store rows (UTC date)."""
    today_str = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM query_log WHERE substr(ts_utc,1,10)=?", (today_str,))
    c.execute("DELETE FROM results_store WHERE substr(ts_utc,1,10)=?", (today_str,))
    conn.commit()
    conn.close()


def clear_history_last_n_days(days: int = 7):
    """Delete query_log + results_store rows from the last N rolling days (UTC)."""
    start_dt = datetime.now(timezone.utc) - timedelta(days=days)
    start_iso = start_dt.isoformat(timespec="seconds")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM query_log WHERE ts_utc >= ?", (start_iso,))
    c.execute("DELETE FROM results_store WHERE ts_utc >= ?", (start_iso,))
    conn.commit()
    conn.close()


# =====================
# BrickLink API helpers
# =====================


def get_public_ip() -> str:
    """Used only for diagnostics."""
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
    vals = [
        BL_CONSUMER_KEY or "",
        BL_CONSUMER_SECRET or "",
        BL_TOKEN or "",
        BL_TOKEN_SECRET or "",
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
    cache_group: str,
    cache_key_extra: Optional[str] = None,
) -> Dict[str, Any]:
    """Wrap requests.get with standardized error handling & 24h caching."""
    key_extra = cache_key_extra or ""
    cache_key = f"{cache_group}:{_bl_cache_key()}:{url}:{json.dumps(params, sort_keys=True)}:{key_extra}"
    _ = cache_key  # to influence hashing
    resp = requests.get(url, params=params, auth=oauth, timeout=20)
    try:
        data = resp.json()
    except Exception:
        data = {
            "meta": {"code": resp.status_code, "message": "non-JSON"},
            "raw_text": resp.text[:400],
        }
    return data


def bl_get(resource: str, oauth: OAuth1, params: Optional[dict] = None, cache_group: str = "bl") -> Dict[str, Any]:
    url = f"https://api.bricklink.com/api/store/v1/{resource.lstrip('/')}"
    return _cached_get_json(url, params, oauth, cache_group=cache_group)


def bl_get_catalog_item(item_type: str, item_no: str, oauth: OAuth1) -> Dict[str, Any]:
    """
    BrickLink 'Get Catalog Item' API:
    GET /items/{type}/{no}
    """
    item_type = item_type.upper()
    url = f"https://api.bricklink.com/api/store/v1/items/{item_type}/{item_no}"
    r = requests.get(url, auth=oauth, timeout=20)
    try:
        return r.json()
    except Exception:
        return {
            "meta": {"code": r.status_code, "message": "non-JSON"},
            "raw": r.text[:400],
        }


def bl_get_price_guide(
    item_type: str,
    item_no: str,
    oauth: OAuth1,
    guide_type: str = "stock",      # "stock" or "sold"
    new_or_used: str = "N",         # "N" or "U"
    country_code: Optional[str] = None,
    region: Optional[str] = None,
    currency_code: Optional[str] = None,
    vat: Optional[str] = None,
) -> Dict[str, Any]:
    """
    BrickLink 'Get Price Guide' API.

    Docs: GET /items/{type}/{no}/price
    """
    item_type = item_type.upper()  # SET, MINIFIG, PART, etc.
    url = f"https://api.bricklink.com/api/store/v1/items/{item_type}/{item_no}/price"

    params: Dict[str, str] = {
        "guide_type": guide_type,      # "stock" (default) or "sold"
        "new_or_used": new_or_used,    # "N" (default) or "U"
    }
    if country_code:
        params["country_code"] = country_code
    if region:
        params["region"] = region
    if currency_code:
        params["currency_code"] = currency_code
    if vat:
        params["vat"] = vat

    r = requests.get(url, params=params, auth=oauth, timeout=20)
    try:
        data = r.json()
    except Exception:
        data = {
            "meta": {"code": r.status_code, "message": "non-JSON"},
            "raw_text": r.text[:400],
        }
    return data


# =====================
# BrickSet helpers
# =====================
def brickset_fetch(set_no: str, api_key: str) -> Dict[str, Any]:
    """Fetch basic BrickSet info for a SET (not minifig)."""
    set_no_clean = set_no.strip()
    if not set_no_clean:
        return {}

    url = "https://brickset.com/api/v3.asmx/getSets"
    payload = {
        "apiKey": api_key,
        "userHash": "",  # not needed unless you’re using owned/wanted flags
        "params": json.dumps({"setNumber": set_no_clean}),
    }

    try:
        r = requests.post(url, data=payload, timeout=20)
    except requests.exceptions.RequestException as e:
        return {"_error": f"Request to BrickSet failed: {e.__class__.__name__}"}

    try:
        resp = r.json()
    except Exception:
        return {"_error": "Non-JSON response from BrickSet"}

    # Brickset v3 format: {"status":"success","matches":<n>,"sets":[...]}
    if not isinstance(resp, dict):
        return {"_error": "Unexpected response from BrickSet"}

    if resp.get("status") != "success":
        return {"_error": resp.get("message", "BrickSet API error")}

    sets = resp.get("sets") or []
    if not sets:
        return {"_error": "No results from BrickSet"}

    first = sets[0]
    collections = first.get("collections") or {}

    return {
        "Set Name (BrickSet)": first.get("name"),
        "Pieces": first.get("pieces"),
        "Minifigs": first.get("minifigs"),
        "Theme": first.get("theme"),
        "Year": first.get("year"),
        "Rating": first.get("rating"),
        "Users Owned": collections.get("ownedBy"),
        "Users Wanted": collections.get("wantedBy"),
    }


# =====================
# BrickEconomy helpers
# =====================
def brickeconomy_fetch_any(
    item_type: str,
    code: str,
    api_key: str,
    currency: str = "USD",
) -> Dict[str, Any]:
    """
    Fetch BrickEconomy info for either:
      item_type="SET"     and code="75131-1"
      item_type="MINIFIG" and code="sw0001"
    using the official BrickEconomy API v1.
    """
    item_type = item_type.upper()
    base = "https://www.brickeconomy.com/api/v1"

    if item_type == "SET":
        url = f"{base}/set/{code}"
    elif item_type == "MINIFIG":
        url = f"{base}/minifig/{code}"
    else:
        return {"error": f"Unsupported BrickEconomy type: {item_type}"}

    headers = {
        "accept": "application/json",
        "x-apikey": api_key,
        "User-Agent": "ReUseBricksApp/1.0",
    }
    params = {}
    if currency:
        params["currency"] = currency

    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        return {"error": f"Request to BrickEconomy failed: {e.__class__.__name__}"}

    try:
        data = r.json()
    except Exception:
        return {"error": "Non-JSON response from BrickEconomy"}

    if isinstance(data, dict) and "error" in data:
        return {"error": data["error"]}

    info = data.get("data") or {}

    name = info.get("name")
    theme = info.get("theme") or info.get("series")
    year = info.get("year")
    retail = info.get("retail_price_us") or info.get("retail_price")
    current_new = info.get("current_value_new")
    current_used = info.get("current_value_used")
    growth_12 = info.get("rolling_growth_12months") or info.get("growth_12m")
    if item_type == "SET":
        url_be = f"https://www.brickeconomy.com/set/{code}"
    else:
        url_be = f"https://www.brickeconomy.com/minifig/{code}"

    out = {
        "Name": name,
        "Theme/Series": theme,
        "Year": year,
        "Retail Price": retail,
        "Current Value (New)": current_new,
        "Current Value (Used)": current_used,
        "Growth % (12m)": growth_12,
        "Currency": info.get("currency") or currency,
        "URL": url_be,
        "Type": item_type,
    }

    # This is fine to keep as an extra UI-level log
    log_query(
        source=f"BrickEconomy:{item_type}",
        set_number=code,
        params={"currency": currency},
        cache_hit=True,
        summary=out["Name"] or "",
    )
    return out


# =====================
# Input parsing helpers
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
    Infer BrickEconomy / BrickLink item_type + item_no from raw input.

    Examples
    --------
    "75131-1"  -> ("SET", "75131-1")
    "75131"    -> ("SET", "75131-1")
    "sw0001"   -> ("MINIFIG", "sw0001")
    "SW0001"   -> ("MINIFIG", "sw0001")
    """
    raw = raw.strip()
    if not raw:
        return "SET", ""

    m = re.match(r"^([a-zA-Z]+)(\d+)$", raw)
    if m:
        return "MINIFIG", raw.lower()

    return "SET", normalize_set_number(raw)


# =====================
# Sidebar
# =====================
with st.sidebar:
    st.title("ReUseBricks")

    st.subheader("BrickLink")
    st.caption("All BrickLink keys and tokens are configured via Streamlit Secrets (0.0.0.0 wildcard).")

    bl_ok = all([BL_CONSUMER_KEY, BL_CONSUMER_SECRET, BL_TOKEN, BL_TOKEN_SECRET])

    st.markdown("---")
    st.subheader("Connections status")
    st.markdown(f"**BrickLink:** {'✅ Connected' if bl_ok else '❌ Missing one or more keys in secrets.'}")
    st.markdown(f"**BrickSet:** {'✅ Configured' if BRICKSET_API_KEY else '❌ Missing API key in secrets.'}")
    st.markdown(f"**BrickEconomy:** {'✅ Configured' if BRICKECONOMY_API_KEY else '❌ Missing API key in secrets.'}")

    st.markdown("---")

    history_days = st.number_input(
        "History window (days)",
        min_value=1,
        max_value=30,
        value=7,
        step=1,
        help="History tables show queries/results from the last N days (rolling, UTC).",
        key="history_days",
    )

    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Clear today", key="btn_clear_history_today"):
            clear_history_today()
            st.success("Cleared today (UTC).")
    with col_b:
        if st.button("Clear last N days", key="btn_clear_history_ndays"):
            clear_history_last_n_days(int(history_days))
            st.success(f"Cleared last {int(history_days)} day(s).")


# =====================
# Main
# =====================
st.title("LEGO Purchasing Assistant")
raw_sets = st.text_area("Enter set numbers (comma or newline separated)")
set_list = [normalize_set_number(s) for s in parse_set_input(raw_sets)]

Tabs = st.tabs(["BrickLink", "BrickSet", "BrickEconomy", "Scoring"])

# ---------------------
# BrickLink Tab
# ---------------------
with Tabs[0]:
    st.subheader("BrickLink Data")

    creds_ok = all([BL_CONSUMER_KEY, BL_CONSUMER_SECRET, BL_TOKEN, BL_TOKEN_SECRET])
    if not creds_ok:
        st.info(
            "BrickLink keys/tokens are not fully configured. "
            "Add BRICKLINK_CONSUMER_KEY, BRICKLINK_CONSUMER_SECRET, "
            "BRICKLINK_TOKEN and BRICKLINK_TOKEN_SECRET to Streamlit Secrets."
        )
    else:
        oauth = OAuth1(
            client_key=BL_CONSUMER_KEY,
            client_secret=BL_CONSUMER_SECRET,
            resource_owner_key=BL_TOKEN,
            resource_owner_secret=BL_TOKEN_SECRET,
            signature_method="HMAC-SHA1",
            signature_type="auth_header",
        )

        with st.expander("BrickLink diagnostics"):
            st.caption(
                "If you see 401/403 or meta errors, recreate the Access Token using this IP, "
                "or set IP to 0.0.0.0 in BrickLink."
            )
            st.code(f"Server public IP: {get_public_ip()}", language="text")
            if st.button("Test BrickLink connection", key="btn_bl_diag"):
                code, headers, body = bl_raw_get("items/SET/10236-1", oauth)
                st.write("Status:", code)
                st.write("Headers:", headers)
                st.json(body)
            if st.button("Clear API cache", key="btn_clear_api_cache"):
                st.cache_data.clear()
                st.success("Cleared API cache.")

        if st.button("Fetch BrickLink Data", key="btn_fetch_bl"):
            raw_items = parse_set_input(raw_sets)
            if not raw_items:
                st.info("No set or minifig numbers entered above.")
            else:
                rows = []
                errors = []

                for raw in raw_items:
                    item_type, item_no = infer_item_type_and_no(raw)

                    # 1) Catalog metadata
                    meta_resp = bl_get_catalog_item(item_type, item_no, oauth)
                    meta_code = meta_resp.get("meta", {}).get("code")
                    if meta_code != 200:
                        msg = meta_resp.get("meta", {}).get("message", "Unknown error")
                        errors.append(f"{item_type} {item_no}: catalog error {meta_code} – {msg}")
                        continue
                    meta_info = meta_resp.get("data") or {}

                    # 2) Price guide (current stock, new)
                    price_resp = bl_get_price_guide(
                        item_type=item_type,
                        item_no=item_no,
                        oauth=oauth,
                        guide_type="stock",
                        new_or_used="N",
                    )
                    price_code = price_resp.get("meta", {}).get("code")
                    if price_code != 200:
                        msg = price_resp.get("meta", {}).get("message", "Unknown error")
                        errors.append(f"{item_type} {item_no}: priceguide error {price_code} – {msg}")
                        continue
                    price_info = price_resp.get("data") or {}

                    row_payload = {
                        "Name": meta_info.get("name"),
                        "Avg Price": price_info.get("avg_price"),
                        "Qty Avg Price": price_info.get("qty_avg_price"),
                        "Min": price_info.get("min_price"),
                        "Max": price_info.get("max_price"),
                        "Currency": price_info.get("currency_code"),
                        "Type": item_type,
                    }
                    rows.append({"Item": item_no, **row_payload})
                    save_result(
                        source="BrickLink:row",
                        set_number=item_no,
                        params={"item_type": item_type},
                        payload=row_payload,
                        cache_hit=False,
                        summary=row_payload.get("Name"),
                    )

                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True)
                else:
                    if errors:
                        st.warning("No BrickLink rows returned. Possible reasons:\n- " + "\n- ".join(errors))
                    else:
                        st.warning("No BrickLink rows returned for the given input.")

        st.markdown(f"### History (last {int(history_days)} day(s))")
        hist_bl = results_last_n_days_df("BrickLink:row", days=int(history_days))
        st.dataframe(
            hist_bl
            if not hist_bl.empty
            else pd.DataFrame(
                columns=["Time (UTC)", "Item", "Name", "Avg Price", "Qty Avg Price", "Min", "Max", "Currency"]
            ),
            use_container_width=True,
        )

# ---------------------
# BrickSet Tab
# ---------------------
with Tabs[1]:
    st.subheader("BrickSet Data")
    api = BRICKSET_API_KEY
    if not api:
        st.info("BrickSet API key is not configured. Add BRICKSET_API_KEY to Streamlit Secrets.")
    else:
        if st.button("Fetch BrickSet Data", key="btn_fetch_bs"):
            if not set_list:
                st.info("No set numbers entered above.")
            else:
                rows = []
                errors = []
                for s in set_list:
                    data = brickset_fetch(s, api)
                    if "_error" in data:
                        errors.append(f"{s}: {data['_error']}")
                        continue

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
                    save_result(
                        source="BrickSet:row",
                        set_number=s,
                        params={},
                        payload=row_payload,
                        cache_hit=False,
                        summary=row_payload.get("Set Name (BrickSet)"),
                    )

                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True)
                else:
                    st.warning(
                        "No BrickSet rows returned."
                        + ("" if not errors else " Possible reasons:\n- " + "\n- ".join(errors))
                    )

        st.markdown(f"### History (last {int(history_days)} day(s))")
        hist_bs = results_last_n_days_df("BrickSet:row", days=int(history_days))
        st.dataframe(
            hist_bs
            if not hist_bs.empty
            else pd.DataFrame(
                columns=[
                    "Time (UTC)",
                    "Item",
                    "Set Name (BrickSet)",
                    "Pieces",
                    "Minifigs",
                    "Theme",
                    "Year",
                    "Rating",
                    "Users Owned",
                    "Users Wanted",
                ]
            ),
            use_container_width=True,
        )

# ---------------------
# BrickEconomy Tab
# ---------------------
with Tabs[2]:
    st.subheader("BrickEconomy Data")
    api = BRICKECONOMY_API_KEY
    currency = BRICKECONOMY_CURRENCY
    if not api:
        st.info("BrickEconomy API key is not configured. Add BRICKECONOMY_API_KEY to Streamlit Secrets.")
    else:
        if st.button("Fetch BrickEconomy Data", key="btn_fetch_be"):
            rows = []
            for raw in parse_set_input(raw_sets):
                item_type, item_no = infer_item_type_and_no(raw)
                if not item_no:
                    continue

                # Optional extra UI log; doesn't affect history join
                log_query(
                    source="UI:BrickEconomy:fetch",
                    set_number=item_no,
                    params={"type": item_type},
                    cache_hit=True,
                    summary="requested",
                )
                data = brickeconomy_fetch_any(item_type, item_no, api, currency)
                rows.append({"Item": item_no, **data})
                save_result(
                    source="BrickEconomy:row",
                    set_number=item_no,
                    params={"type": item_type},
                    payload=data,
                    cache_hit=False,
                    summary=data.get("Name"),
                )
            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True)

        st.markdown(f"### History (last {int(history_days)} day(s))")
        hist_be = results_last_n_days_df("BrickEconomy:row", days=int(history_days))
        st.dataframe(
            hist_be
            if not hist_be.empty
            else pd.DataFrame(
                columns=[
                    "Time (UTC)",
                    "Item",
                    "Type",
                    "Name",
                    "Theme/Series",
                    "Year",
                    "Retail Price",
                    "Current Value (New)",
                    "Current Value (Used)",
                    "Growth % (12m)",
                    "Currency",
                    "URL",
                ]
            ),
            use_container_width=True,
        )

# ---------------------
# Scoring Tab (UPDATED)
# ---------------------
with Tabs[3]:
    st.subheader("Scoring")

    # Brickset total user base used for normalization ratios (can be made configurable later)
    BRICKSET_TOTAL_USERS = 374_621

    # Demand settings (tunable, but stable over time)
    DEMAND_SMOOTHING_K = 50.0
    WANTED_SHARE_FOR_10 = 0.02  # 2% wanted share -> max baseline demand
    PRESSURE_MIN = 0.5          # cap how low pressure can drag the score
    PRESSURE_MAX = 1.5          # cap how high pressure can boost the score

    def _clip(x: float, lo: float, hi: float) -> float:
        try:
            x = float(x)
        except Exception:
            return lo
        return max(lo, min(hi, x))

    def _fmt_pct(x: float) -> str:
        try:
            return f"{100.0 * float(x):.2f}%"
        except Exception:
            return ""

    def _fmt_ratio(x: float) -> str:
        try:
            return f"{float(x):.2f}"
        except Exception:
            return ""

    def _fmt_num6(x: float) -> str:
        try:
            return f"{float(x):.6f}"
        except Exception:
            return ""

    if st.button("Compute Score (example)", key="btn_score"):
        scores = []
        api_bs = BRICKSET_API_KEY
        api_be = BRICKECONOMY_API_KEY
        cur = BRICKECONOMY_CURRENCY

        for s in set_list:
            bs = brickset_fetch(s, api_bs) if api_bs else {}
            be = brickeconomy_fetch_any("SET", s, api_be, cur) if api_be else {}

            pieces = (bs or {}).get("Pieces") or 0
            rating = (bs or {}).get("Rating") or 0
            owned = (bs or {}).get("Users Owned") or 0
            wanted = (bs or {}).get("Users Wanted") or 0

            value = (be or {}).get("Current Value (New)") or (be or {}).get("Current Value") or 0

            try:
                p = float(pieces or 0)
                r = float(rating or 0)
                v = float(value or 0)
                o = float(owned or 0)
                w = float(wanted or 0)
            except Exception:
                p, r, v, o, w = 0.0, 0.0, 0.0, 0.0, 0.0

            # Ratios (raw values saved; formatted values displayed)
            owned_ratio = (o / BRICKSET_TOTAL_USERS) if BRICKSET_TOTAL_USERS else 0.0
            wanted_ratio = (w / BRICKSET_TOTAL_USERS) if BRICKSET_TOTAL_USERS else 0.0
            wanted_owned_ratio = (w / o) if o else 0.0

            # Demand components
            demand_pressure_smoothed = (w + DEMAND_SMOOTHING_K) / (o + DEMAND_SMOOTHING_K) if (o + DEMAND_SMOOTHING_K) else 0.0
            demand_index = wanted_ratio * demand_pressure_smoothed

            # ABSOLUTE Demand Score (0-10), stable over time (NOT list-relative)
            base = _clip(wanted_ratio / WANTED_SHARE_FOR_10, 0.0, 1.0)
            pressure_factor = _clip(demand_pressure_smoothed, PRESSURE_MIN, PRESSURE_MAX)
            demand_score_0_10 = _clip(10.0 * base * pressure_factor, 0.0, 10.0)

            # Keep final score equation unchanged for now
            score_val = 0.4 * (p / 1000) + 0.4 * r + 0.2 * (v / 100)

            row_payload = {
                "Set": s,
                "Pieces": p,
                "BrickSet Rating": r,
                "Users Owned": o,
                "Users Wanted": w,

                # Raw numeric ratios (saved to SQLite/history)
                "Owned / Total Users": owned_ratio,
                "Wanted / Total Users": wanted_ratio,
                "Wanted / Owned": wanted_owned_ratio,

                # Demand metrics (saved raw)
                "Demand Pressure (smoothed)": demand_pressure_smoothed,
                "Demand Index": demand_index,
                "Demand Score (0-10)": demand_score_0_10,

                "Current Value": v,
                "Score": score_val,
            }
            scores.append(row_payload)

            save_result(
                source="Scoring:row",
                set_number=s,
                params={},
                payload=row_payload,
                cache_hit=False,
                summary=f"Score {score_val:.2f}",
            )

        df_scores = pd.DataFrame(scores)

        # UI-only formatted columns (do not save these)
        df_scores["Owned / Total Users (%)"] = df_scores["Owned / Total Users"].apply(_fmt_pct)
        df_scores["Wanted / Total Users (%)"] = df_scores["Wanted / Total Users"].apply(_fmt_pct)
        df_scores["Wanted / Owned (x)"] = df_scores["Wanted / Owned"].apply(_fmt_ratio)
        df_scores["Demand Pressure (smoothed) (x)"] = df_scores["Demand Pressure (smoothed)"].apply(_fmt_ratio)
        df_scores["Demand Index (raw)"] = df_scores["Demand Index"].apply(_fmt_num6)
        df_scores["Demand Score (0-10)"] = df_scores["Demand Score (0-10)"].apply(_fmt_ratio)

        cols = [
            "Set",
            "Pieces",
            "BrickSet Rating",
            "Users Owned",
            "Users Wanted",
            "Owned / Total Users (%)",
            "Wanted / Total Users (%)",
            "Wanted / Owned (x)",
            "Demand Pressure (smoothed) (x)",
            "Demand Index (raw)",
            "Demand Score (0-10)",
            "Current Value",
            "Score",
        ]

        st.dataframe(
            df_scores[cols] if all(c in df_scores.columns for c in cols) else df_scores,
            use_container_width=True,
        )

    st.markdown(f"### History (last {int(history_days)} day(s) – Scoring)")
    hist_sc = results_last_n_days_df("Scoring:row", days=int(history_days))

    # UI formatting for history too (still based on raw saved ratios)
    if not hist_sc.empty:
        if "Owned / Total Users" in hist_sc.columns:
            hist_sc["Owned / Total Users (%)"] = hist_sc["Owned / Total Users"].apply(_fmt_pct)
        if "Wanted / Total Users" in hist_sc.columns:
            hist_sc["Wanted / Total Users (%)"] = hist_sc["Wanted / Total Users"].apply(_fmt_pct)
        if "Wanted / Owned" in hist_sc.columns:
            hist_sc["Wanted / Owned (x)"] = hist_sc["Wanted / Owned"].apply(_fmt_ratio)
        if "Demand Pressure (smoothed)" in hist_sc.columns:
            hist_sc["Demand Pressure (smoothed) (x)"] = hist_sc["Demand Pressure (smoothed)"].apply(_fmt_ratio)
        if "Demand Index" in hist_sc.columns:
            hist_sc["Demand Index (raw)"] = hist_sc["Demand Index"].apply(_fmt_num6)
        if "Demand Score (0-10)" in hist_sc.columns:
            hist_sc["Demand Score (0-10)"] = hist_sc["Demand Score (0-10)"].apply(_fmt_ratio)

        hist_cols = [
            "Time (UTC)",
            "Item",
            "Pieces",
            "BrickSet Rating",
            "Users Owned",
            "Users Wanted",
            "Owned / Total Users (%)",
            "Wanted / Total Users (%)",
            "Wanted / Owned (x)",
            "Demand Pressure (smoothed) (x)",
            "Demand Index (raw)",
            "Demand Score (0-10)",
            "Current Value",
            "Score",
        ]
        # Older rows won't have new columns; filter safely
        hist_cols = [c for c in hist_cols if c in hist_sc.columns]
        st.dataframe(hist_sc[hist_cols], use_container_width=True)
    else:
        st.dataframe(
            pd.DataFrame(
                columns=[
                    "Time (UTC)",
                    "Item",
                    "Pieces",
                    "BrickSet Rating",
                    "Users Owned",
                    "Users Wanted",
                    "Owned / Total Users (%)",
                    "Wanted / Total Users (%)",
                    "Wanted / Owned (x)",
                    "Demand Pressure (smoothed) (x)",
                    "Demand Index (raw)",
                    "Demand Score (0-10)",
                    "Current Value",
                    "Score",
                ]
            ),
            use_container_width=True,
        )

st.markdown(
    f"<small>Cache TTL: 24h. History shows the last {int(history_days)} day(s) of queries (rolling window, UTC).</small>",
    unsafe_allow_html=True,
)
