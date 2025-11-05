# fetch_census_data.py — Live Census API helpers with resilience and short-TTL caching
# What this file does:
# - Loads your Census API key from .env
# - Defines geographies for Long Beach (place 43000 in CA) and US
# - Maps topics -> one or more variable codes (DP or B/S tables)
# - Handles ZCTA (ZIP) special cases where DP codes don’t exist (uses B-table equivalents)
# - Provides robust HTTP with retries + a small in-memory TTL cache (still "live")
# - Provides tidy_long() to reshape wide API responses to tidy format for plotting

from __future__ import annotations
import os
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Tuple

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
# --- ADD THIS to fetch_census_data.py ---

from concurrent.futures import ThreadPoolExecutor, as_completed
_TTL_MINUTES = 60  # cache API results for 1 hour


def call_api_ttl_many(years: list[int], code: str, geo: dict, ttl_minutes: int = _TTL_MINUTES) -> list[pd.DataFrame]:
    """
    Fetch the same variable code for multiple years concurrently.
    Uses the same short-TTL cache as call_api_ttl(), but parallelized.
    """
    def one(y):
        return call_api_ttl(y, code, geo)

    # 6 years -> 6 workers is fine; adjust if you add more years
    frames = []
    with ThreadPoolExecutor(max_workers=min(6, len(years))) as ex:
        futs = {ex.submit(one, y): y for y in years}
        for fut in as_completed(futs):
            df = fut.result()
            if not df.empty:
                frames.append(df)
    # Keep the chronological order
    frames.sort(key=lambda d: int(d["Year"].iloc[0]) if "Year" in d.columns and not d.empty else 0)
    return frames


# ------------------ CONFIG ------------------
load_dotenv()  # read .env if present
API_KEY = os.getenv("CENSUS_API_KEY")
if not API_KEY:
    raise SystemExit("No CENSUS_API_KEY found in .env")

BASE = "https://api.census.gov/data"

# Available ACS 5-year releases you want to support in the UI (inclusive range)
YEARS = list(range(2018, 2024))  # 2018,2019,2020,2021,2022,2023

# Geographies
CITY_GEO = {"for": "place:43000", "in": "state:06"}  # Long Beach city, CA
US_GEO   = {"for": "us:1"}                           # United States

# Your Long Beach-focused ZCTA list (edit as you like)
LB_ZCTAS = [
    "90712","90755","90802","90803","90804","90805",
    "90806","90807","90808","90810","90813","90814","90815"
]

# Topics (UI uses the keys). Values are lists of variable codes:
# - You can mix a single code (e.g., DP05_0001E) and entire groups (e.g., group(B01001))
# - DPxx are "Data Profile" tables; Bxxxx are "Detailed tables"; Sxxxx are "Subject tables"
TOPICS = {
    "Total Population": ["DP05_0001E"],
    "Median Age": ["DP05_0018E"],  # we'll auto-swap to B01002_001E for ZCTAs
    "Sex by Age (full)": ["group(B01001)"],
    "Race (full)": ["group(B02001)"],
    "Hispanic/Latino Origin (full)": ["group(B03002)"],
}

# ZCTAs do not have DP** profile variables; override to B- equivalents for ZIP geography
ZCTA_OVERRIDES = {
    "DP05_0001E": "B01003_001E",  # Total population
    "DP05_0018E": "B01002_001E",  # Median age
}

# Friendly labels for commonly plotted single variables
LABELS = {
    "DP05_0001E": "Total population",
    "DP05_0018E": "Median age (years)",
    "B01003_001E": "Total population",
    "B01002_001E": "Median age (years)",
}

# ------------------ ROBUST HTTP SESSION ------------------
def _make_session() -> requests.Session:
    """
    Create a requests.Session with retry/backoff and HTTP connection pooling.
    Keeps the app responsive even if the API has transient errors.
    """
    sess = requests.Session()

    retry = Retry(
        total=6,                   # total retries (connect + read)
        connect=6,
        read=6,
        backoff_factor=0.6,        # exponential backoff base
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.headers.update({"User-Agent": "LB-Dashboard/1.0 (+student project)"})
    return sess

SESSION = _make_session()

# ------------------ HELPERS ------------------
def is_zcta_geo(geo: dict) -> bool:
    """Return True if the request uses ZIP Code Tabulation Area geography."""
    return str(geo.get("for", "")).startswith("zip code tabulation area:")

def zcta_geo(z: str) -> dict:
    """
    Build a ZCTA geo dict. We start WITHOUT 'in=state:06' and add/remove it
    dynamically based on Census API error messages (see _call_api_cached).
    """
    return {"for": f"zip code tabulation area:{z}"}

def dataset_for(code: str) -> str:
    """
    Choose which dataset to call based on the variable code:
      - Data Profile tables (DP**) live at /acs/acs5/profile
      - Detailed & Subject tables (B****/S****) live at /acs/acs5
    """
    c = str(code).strip().upper()
    return "acs/acs5/profile" if c.startswith("DP") else "acs/acs5"

def resolve_code_for_geo(code: str, geo: dict) -> str:
    """
    If the geography is ZCTA and a DP code is requested, swap to the B-table override.
    Otherwise, return the code as-is.
    """
    c = str(code).strip()
    if is_zcta_geo(geo) and c in ZCTA_OVERRIDES:
        return ZCTA_OVERRIDES[c]
    return c

# ---------- internal cache key for identical API requests ----------
def _geo_key(geo: dict) -> Tuple[str | None, str | None]:
    """Return (for, in) tuple used to key the lru_cache below."""
    return (geo.get("for"), geo.get("in"))

@lru_cache(maxsize=4096)
def _call_api_cached(year: int, code_eff: str, dataset: str,
                     for_val: str | None, in_val: str | None) -> pd.DataFrame:
    """
    Low-level Census API caller. Cached by args so repeated identical requests
    in a session don't refetch.
    - Adds/strips 'in=state:06' automatically for ZCTAs depending on API feedback.
    - Always returns a pandas DataFrame with an extra 'Year' column.
    """
    url = f"{BASE}/{year}/{dataset}"

    def req(params: dict) -> requests.Response:
        # You could add a very small sleep here as a courtesy if you like.
        return SESSION.get(url, params=params, timeout=15)

    # Build base params
    params = {"get": ",".join(["NAME", code_eff]), "key": API_KEY}
    if for_val: params["for"] = for_val
    if in_val:  params["in"]  = in_val

    # First attempt
    r = req(params)

    # ZCTA quirk handler:
    # Some years need 'in=state:06' to disambiguate; other endpoints reject it.
    if r.status_code == 400 and (for_val or "").startswith("zip code tabulation area:"):
        txt = r.text.lower()

        # If ambiguous, add the 'in' clause
        if "ambiguous geography" in txt and "in" not in params:
            r = req({**params, "in": "state:06"})

        # If hierarchy unsupported, remove the 'in' clause
        elif "unknown/unsupported geography hierarchy" in txt and "in" in params:
            p2 = dict(params); p2.pop("in", None)
            r = req(p2)

        # If still failing, flip once more the other way (last-ditch)
        if r.status_code == 400:
            if "in" in params:
                p2 = dict(params); p2.pop("in", None)
                r = req(p2)
            else:
                r = req({**params, "in": "state:06"})

    # Raise if still not OK
    r.raise_for_status()

    # Parse JSON to DataFrame
    js = r.json()
    df = pd.DataFrame(js[1:], columns=js[0])

    # Drop accidental duplicate columns (can happen with some group calls)
    df = df.loc[:, ~df.columns.duplicated()].copy()

    # Add the release year for later grouping
    df["Year"] = year
    return df

def call_api(year: int, code: str, geo: dict) -> pd.DataFrame:
    """
    Public wrapper: choose dataset and swap code if needed, then call the cached low-level API.
    """
    code_eff = resolve_code_for_geo(code, geo)
    dataset  = dataset_for(code_eff)
    for_val, in_val = _geo_key(geo)
    return _call_api_cached(year, code_eff, dataset, for_val, in_val)


_cache: dict[tuple, tuple[pd.DataFrame, datetime]] = {}

def call_api_ttl(year: int, code: str, geo: dict, ttl_minutes: int = _TTL_MINUTES) -> pd.DataFrame:
    """
    Same as call_api(), but caches the DataFrame in-memory for ttl_minutes.
    This keeps your dashboard snappy while staying "live" within the last hour.
    """
    code_eff = resolve_code_for_geo(code, geo)
    dataset  = dataset_for(code_eff)
    key = (year, code_eff, dataset, geo.get("for"), geo.get("in"))
    now = datetime.utcnow()

    hit = _cache.get(key)
    if hit:
        df, ts = hit
        if now - ts < timedelta(minutes=ttl_minutes):
            return df.copy()

    df = call_api(year, code, geo)
    _cache[key] = (df.copy(), now)
    return df

# ---------- reshape/labels ----------
def tidy_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert a wide ACS response into tidy format with columns:
      - Year, Variable, Value, Display (human label)
    We keep 'NAME' and geography IDs if present.
    """
    if df.empty:
        return df

    id_cols = [c for c in df.columns if c in (
        "NAME", "Year", "state", "place", "us", "zip code tabulation area",
        "Geo", "Topic", "Area"
    )]
    val_cols = [c for c in df.columns if c not in id_cols]

    # Melt to long
    long = df.melt(id_vars=id_cols, value_vars=val_cols, var_name="Variable", value_name="Value")

    # Coerce numeric where possible
    long["Value"] = pd.to_numeric(long["Value"], errors="coerce")

    # Attach friendly labels (fallback to raw var code)
    long["Display"] = long["Variable"].map(LABELS).fillna(long["Variable"])
    return long

# --- FAST: call a list of variables in ONE request (per dataset) ---
def _dataset_for(code: str) -> str:
    c = str(code).upper()
    return "acs/acs5/profile" if c.startswith("DP") else "acs/acs5"

def call_api_vars(year: int, codes: list[str], geo: dict) -> pd.DataFrame:
    """
    Make 1–2 calls total by splitting codes by dataset (DP vs B/S).
    Much smaller payloads than group(table).
    """
    buckets = {"acs/acs5/profile": [], "acs/acs5": []}
    for c in codes:
        buckets[_dataset_for(c)].append(c)

    frames = []
    for dataset, vars_ in buckets.items():
        if not vars_:
            continue
        url = f"{BASE}/{year}/{dataset}"
        params = {"get": "NAME," + ",".join(vars_), "key": API_KEY}
        if "for" in geo: params["for"] = geo["for"]
        if "in"  in geo: params["in"]  = geo["in"]
        r = SESSION.get(url, params=params, timeout=15)
        if r.status_code == 400 and "zip code tabulation area" in params.get("for",""):
            txt = r.text.lower()
            if "ambiguous geography" in txt and "in" not in params:
                r = SESSION.get(url, params={**params, "in": "state:06"}, timeout=60)
            elif "unsupported geography hierarchy" in txt and "in" in params:
                p2 = dict(params); p2.pop("in", None); r = SESSION.get(url, params=p2, timeout=60)
        r.raise_for_status()
        js = r.json()
        df = pd.DataFrame(js[1:], columns=js[0])
        df["Year"] = year
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# --- TTL wrapper for the multi-var call ---
_multi_cache: dict[tuple, tuple[pd.DataFrame, datetime]] = {}

def call_api_vars_ttl(year: int, codes: list[str], geo: dict, ttl_minutes: int = _TTL_MINUTES) -> pd.DataFrame:
    key = (year, tuple(sorted(codes)), geo.get("for"), geo.get("in"))
    now = datetime.utcnow()
    hit = _multi_cache.get(key)
    if hit:
        df, ts = hit
        if now - ts < timedelta(minutes=ttl_minutes):
            return df.copy()
    df = call_api_vars(year, codes, geo)
    _multi_cache[key] = (df.copy(), now)
    return df

# Map each dashboard "topic" to a *small* default var list.
# (adjust to your exact topics/labels — these are examples)
TOPIC_MIN_VARS = {
    "Total Population": ["DP05_0001E"],          # total pop
    "Median Age": ["B01002_001E"],               # median age
    "Sex": ["DP05_0001E", "DP05_0001M", "DP05_0001C"],  # example; replace with what you actually chart
    "Race/Ethnicity": ["DP05_0071E","DP05_0072E","DP05_0077E","DP05_0081E"],  # pick a few key lines
    # …add the rest of your topics with a minimal set each…
}

def topic_to_vars_min(topic: str) -> list[str]:
    """
    Return a small, explicit variable list for the given topic.
    IMPORTANT: Never return 'group(Bxxxx)' here; keep it lean for speed.
    """
    if topic in TOPIC_MIN_VARS:
        return TOPIC_MIN_VARS[topic]
    # Fallback: safest tiny default
    return ["DP05_0001E"]
