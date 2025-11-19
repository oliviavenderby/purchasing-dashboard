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
):
    """Upsert into results_store keyed by (source,set_number,params_hash)."""
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
    payload_json = json.dumps(payload)
    if row:
        c.execute(
            """
            UPDATE results_store
            SET ts_utc=?, payload_json=?
            WHERE id=?
            """,
            (
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
                payload_json,
                row[0],
            ),
        )
    else:
        c.execute(
            """
            INSERT INTO results_store (ts_utc, source, set_number, params_hash, payload_json)
            VALUES (?,?,?,?,?)
            """,
            (
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
                source,
                set_number,
                key_hash,
                payload_json,
            ),
        )
    conn.commit()
    conn.close()


def results_today_df(source_prefix: str) -> pd.DataFrame:
    """Return today's results for a given source prefix, joined with query_log."""
    today_str = date.today().isoformat()
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
          AND substr(q.ts_utc,1,10) = ?
        ORDER BY q.ts_utc DESC
        """,
        (f"{source_prefix}%", today_str),
    )
    rows = c.fetchall()
    conn.close()
    records = []
    for ts_utc, src, set_number, p_hash, payload_json in rows:
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
    """Delete today's query_log + results_store rows."""
    today_str = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM query_log WHERE substr(ts_utc,1,10)=?", (today_str,))
    c.execute("DELETE FROM results_store WHERE substr(ts_utc,1,10)=?", (today_str,))
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
    # Streamlit's cache_data uses all args; we also inject cache_key so that new tokens invalidate.
    _ = cache_key  # just to visibly use it
    resp = requests.get(url, params=params, auth=oauth, timeout=20)
    try:
        data = resp.json()
    except Exception:
        data = {"meta": {"code": resp.status_code, "message": "non-JSON"}, "raw_text": resp.text[:400]}
    return data


def bl_get(resource: str, oauth: OAuth1, params: Optional[dict] = None, cache_group: str = "bl") -> Dict[str, Any]:
    url = f"https://api.bricklink.com/api/store/v1/{resource.lstrip('/')}"
    return _cached_get_json(url, params, oauth, cache_group=cache_group)


from typing import Optional, Dict, Any

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
    params = {
        "apiKey": api_key,
        "userHash": "",
        "params": json.dumps({"setNumber": set_no_clean}),
    }
    r = requests.post(url, data=params, timeout=20)
    try:
        data = r.json()
    except Exception:
        return {"error": "Non-JSON from BrickSet"}

    if not isinstance(data, list) or not data:
        return {"error": "No results from BrickSet"}

    first = data[0]
    return {
        "Set Name (BrickSet)": first.get("name"),
        "Pieces": first.get("pieces"),
        "Minifigs": first.get("minifigs"),
        "Theme": first.get("theme"),
        "Year": first.get("year"),
        "Rating": first.get("rating"),
        "Users Owned": first.get("ownedBy"),
        "Users Wanted": first.get("wantedBy"),
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
        # /set/<set number>
        url = f"{base}/set/{code}"
    elif item_type == "MINIFIG":
        # /minifig/<minifig number>
        url = f"{base}/minifig/{code}"
    else:
        return {"error": f"Unsupported BrickEconomy type: {item_type}"}

    headers = {
        "accept": "application/json",
        "x-apikey": api_key,                     # NOTE: header name is x-apikey (no dash)
        "User-Agent": "ReUseBricksApp/1.0",      # any non-empty UA string is required
    }
    params = {}
    if currency:
        params["currency"] = currency

    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        # Avoid crashing the whole app – return a friendly error object instead
        return {"error": f"Request to BrickEconomy failed: {e.__class__.__name__}"}

    try:
        data = r.json()
    except Exception:
        return {"error": "Non-JSON response from BrickEconomy"}

    # BrickEconomy wraps the object inside "data"
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
    # There isn't a direct "URL" field in the API; construct one from the set/minifig number:
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

    # Minifig heuristic: starts with letters, then digits.
    m = re.match(r"^([a-zA-Z]+)(\d+)$", raw)
    if m:
        return "MINIFIG", raw.lower()

    # Otherwise treat as SET, normalize with -1.
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
    st.markdown(
        f"**BrickLink:** {'✅ Connected' if bl_ok else '❌ Missing one or more keys in secrets.'}"
    )
    st.markdown(
        f"**BrickSet:** {'✅ Configured' if BRICKSET_API_KEY else '❌ Missing API key in secrets.'}"
    )
    st.markdown(
        f"**BrickEconomy:** {'✅ Configured' if BRICKECONOMY_API_KEY else '❌ Missing API key in secrets.'}"
    )

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

        # --- Diagnostics block ---
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

        # --- Main fetch button ---
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
                        guide_type="stock",   # current items for sale
                        new_or_used="N",      # new condition
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
                        "Currency": price_info.get("currency"),
                        "Type": item_type,
                    }
                    rows.append({"Item": item_no, **row_payload})
                    save_result(
                        source="BrickLink:row",
                        set_number=item_no,
                        params={"item_type": item_type},
                        payload=row_payload,
                    )

                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True)
                else:
                    if errors:
                        st.warning(
                            "No BrickLink rows returned. Possible reasons:\n- "
                            + "\n- ".join(errors)
                        )
                    else:
                        st.warning("No BrickLink rows returned for the given input.")

        st.markdown("### History (today)")
        hist_bl = results_today_df("BrickLink:row")
        st.dataframe(
            hist_bl
            if not hist_bl.empty
            else pd.DataFrame(
                columns=[
                    "Time (UTC)",
                    "Item",
                    "Name",
                    "Avg Price",
                    "Qty Avg Price",
                    "Min",
                    "Max",
                    "Currency",
                ]
            ),
            use_container_width=True,
        )



# BrickSet Tab
# BrickSet Tab
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
                    save_result(source="BrickSet:row", set_number=s, params={}, payload=row_payload)

                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True)
                else:
                    st.warning(
                        "No BrickSet rows returned."
                        + ("" if not errors else " Possible reasons:\n- " + "\n- ".join(errors))
                    )

        st.markdown("### History (today)")
        hist_bs = results_today_df("BrickSet:row")
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


# BrickEconomy Tab (now supports SET + MINIFIG)
with Tabs[2]:
    st.subheader("BrickEconomy Data")
    api = BRICKECONOMY_API_KEY
    currency = BRICKECONOMY_CURRENCY
    if not api:
        st.info("BrickEconomy API key is not configured. Add BRICKECONOMY_API_KEY to Streamlit Secrets.")
    else:
        if st.button("Fetch BrickEconomy Data", key="btn_fetch_be"):
            rows = []
            # Parse raw input and infer per-item (SET or MINIFIG)
            for raw in parse_set_input(raw_sets):
                item_type, item_no = infer_item_type_and_no(raw)
                if not item_no:
                    continue
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
                )
            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True)
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
        api_bs = BRICKSET_API_KEY
        api_be = BRICKECONOMY_API_KEY
        cur = BRICKECONOMY_CURRENCY
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
            score_val = 0.4 * (p / 1000) + 0.4 * r + 0.2 * (v / 100)
            row_payload = {
                "Set": s,
                "Pieces": p,
                "BrickSet Rating": r,
                "Current Value": v,
                "Score": score_val,
            }
            scores.append(row_payload)
            save_result(source="Scoring:row", set_number=s, params={}, payload=row_payload)

        df_scores = pd.DataFrame(scores)
        cols = ["Set", "Pieces", "BrickSet Rating", "Current Value", "Score"]
        st.dataframe(
            df_scores[cols] if all(c in df_scores.columns for c in cols) else df_scores,
            use_container_width=True,
        )

    st.markdown("### History (today – Scoring)")
    hist_sc = results_today_df("Scoring:row")
    st.dataframe(
        hist_sc if not hist_sc.empty else pd.DataFrame(
            columns=["Time (UTC)", "Item", "Pieces", "BrickSet Rating", "Current Value", "Score"]
        ),
        use_container_width=True,
    )

st.markdown(
    "<small>Cache TTL: 24h. History shows today's queries whether they hit cache or not.</small>",
    unsafe_allow_html=True,
)
