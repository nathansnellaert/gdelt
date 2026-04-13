"""GDELT connector utilities: schemas, HTTP, parsing, and code denormalization.

All GDELT-specific logic lives here. Node files should only call these helpers.
"""
import io
import zipfile
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc

from subsets_utils import get, save_raw_parquet, load_raw_parquet, raw_asset_exists


# =============================================================================
# URLs and constants
# =============================================================================

GDELT_START_DATE = datetime(2015, 2, 18)
GH_ACTIONS_MAX_RUN_SECONDS = 5.8 * 60 * 60
MASTER_FILE_LIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

CAMEO_LOOKUP_BASE = "http://gdeltproject.org/data/lookups"
CAMEO_CODELISTS = {
    "cameo_eventcodes":  f"{CAMEO_LOOKUP_BASE}/CAMEO.eventcodes.txt",
    "cameo_country":     f"{CAMEO_LOOKUP_BASE}/CAMEO.country.txt",
    "cameo_type":        f"{CAMEO_LOOKUP_BASE}/CAMEO.type.txt",
    "cameo_religion":    f"{CAMEO_LOOKUP_BASE}/CAMEO.religion.txt",
    "cameo_ethnic":      f"{CAMEO_LOOKUP_BASE}/CAMEO.ethnic.txt",
    "cameo_knowngroup":  f"{CAMEO_LOOKUP_BASE}/CAMEO.knowngroup.txt",
    "fips_country":      f"{CAMEO_LOOKUP_BASE}/FIPS.country.txt",
}


# =============================================================================
# Events schema
# =============================================================================
#
# Column order MUST match the GDELT 2.0 Events file layout (60 columns, tab-
# delimited, no header). The raw CSV uses strings for everything; cast_events()
# converts to the target types below at transform time.

EVENTS_RAW_COLUMNS = [
    "global_event_id", "day", "month_year", "year", "fraction_date",
    "actor1_code", "actor1_name", "actor1_country_code", "actor1_known_group_code",
    "actor1_ethnic_code", "actor1_religion1_code", "actor1_religion2_code",
    "actor1_type1_code", "actor1_type2_code", "actor1_type3_code",
    "actor2_code", "actor2_name", "actor2_country_code", "actor2_known_group_code",
    "actor2_ethnic_code", "actor2_religion1_code", "actor2_religion2_code",
    "actor2_type1_code", "actor2_type2_code", "actor2_type3_code",
    "is_root_event", "event_code", "event_base_code", "event_root_code",
    "quad_class", "goldstein_scale", "num_mentions", "num_sources", "num_articles",
    "avg_tone",
    "actor1_geo_type", "actor1_geo_fullname", "actor1_geo_country_code",
    "actor1_geo_adm1_code", "actor1_geo_adm2_code", "actor1_geo_lat",
    "actor1_geo_long", "actor1_geo_feature_id",
    "actor2_geo_type", "actor2_geo_fullname", "actor2_geo_country_code",
    "actor2_geo_adm1_code", "actor2_geo_adm2_code", "actor2_geo_lat",
    "actor2_geo_long", "actor2_geo_feature_id",
    "action_geo_type", "action_geo_fullname", "action_geo_country_code",
    "action_geo_adm1_code", "action_geo_adm2_code", "action_geo_lat",
    "action_geo_long", "action_geo_feature_id",
    "date_added", "source_url",
]

EVENTS_TARGET_TYPES: dict[str, pa.DataType] = {
    "global_event_id":          pa.int64(),
    "day":                      pa.date32(),
    "month_year":               pa.int32(),
    "year":                     pa.int32(),
    "fraction_date":            pa.float64(),
    "is_root_event":            pa.bool_(),
    "quad_class":               pa.int8(),
    "goldstein_scale":          pa.float64(),
    "num_mentions":             pa.int32(),
    "num_sources":              pa.int32(),
    "num_articles":             pa.int32(),
    "avg_tone":                 pa.float64(),
    "actor1_geo_type":          pa.int8(),
    "actor1_geo_lat":           pa.float64(),
    "actor1_geo_long":          pa.float64(),
    "actor2_geo_type":          pa.int8(),
    "actor2_geo_lat":           pa.float64(),
    "actor2_geo_long":          pa.float64(),
    "action_geo_type":          pa.int8(),
    "action_geo_lat":           pa.float64(),
    "action_geo_long":          pa.float64(),
    "date_added":               pa.timestamp("s"),
    # everything else stays pa.string()
}

QUAD_CLASS_LABELS = {
    1: "Verbal Cooperation",
    2: "Material Cooperation",
    3: "Verbal Conflict",
    4: "Material Conflict",
}


# =============================================================================
# FIPS 10-4 → ISO 3166-1 alpha-2 mapping
# =============================================================================
#
# GDELT's *_geo_country_code columns use FIPS 10-4 (NGA GEC) two-letter codes.
# This map translates them to the ISO 3166-1 alpha-2 codes required by the
# repo data standards (guides/data_standards.md). Sourced from the canonical
# NGA FIPS 10-4 / GEC → ISO 3166-1 concordance. Non-country entities (oceans,
# Antarctic bases, etc.) and obsolete codes map to None.

FIPS_TO_ISO2: dict[str, str] = {
    "AA": "AW", "AC": "AG", "AE": "AE", "AF": "AF", "AG": "DZ", "AJ": "AZ",
    "AL": "AL", "AM": "AM", "AN": "AD", "AO": "AO", "AQ": "AS", "AR": "AR",
    "AS": "AU", "AT": "AT", "AU": "AT", "AV": "AI", "AY": "AQ", "BA": "BH",
    "BB": "BB", "BC": "BW", "BD": "BM", "BE": "BE", "BF": "BS", "BG": "BD",
    "BH": "BZ", "BK": "BA", "BL": "BO", "BM": "MM", "BN": "BJ", "BO": "BY",
    "BP": "SB", "BR": "BR", "BS": "PM", "BT": "BT", "BU": "BG", "BV": "BV",
    "BX": "BN", "BY": "BI", "CA": "CA", "CB": "KH", "CD": "TD", "CE": "LK",
    "CF": "CG", "CG": "CD", "CH": "CN", "CI": "CL", "CJ": "KY", "CK": "CC",
    "CM": "CM", "CN": "KM", "CO": "CO", "CR": "CR", "CS": "CR", "CT": "CF",
    "CU": "CU", "CV": "CV", "CW": "CK", "CY": "CY", "DA": "DK", "DJ": "DJ",
    "DO": "DM", "DR": "DO", "EC": "EC", "EG": "EG", "EI": "IE", "EK": "GQ",
    "EN": "EE", "ER": "ER", "ES": "SV", "ET": "ET", "EU": "RE", "EZ": "CZ",
    "FI": "FI", "FJ": "FJ", "FK": "FK", "FM": "FM", "FO": "FO", "FP": "PF",
    "FR": "FR", "FS": "TF", "GA": "GM", "GB": "GA", "GG": "GE", "GH": "GH",
    "GI": "GI", "GJ": "GD", "GK": "GG", "GL": "GL", "GM": "DE", "GP": "GP",
    "GQ": "GU", "GR": "GR", "GT": "GT", "GV": "GN", "GY": "GY", "HA": "HT",
    "HK": "HK", "HM": "HM", "HO": "HN", "HR": "HR", "HU": "HU", "IC": "IS",
    "ID": "ID", "IM": "IM", "IN": "IN", "IO": "IO", "IP": "CX", "IR": "IR",
    "IS": "IL", "IT": "IT", "IV": "CI", "IZ": "IQ", "JA": "JP", "JE": "JE",
    "JM": "JM", "JO": "JO", "JQ": "UM", "JU": "JU", "KE": "KE", "KG": "KG",
    "KN": "KP", "KQ": "UM", "KR": "KI", "KS": "KR", "KT": "CX", "KU": "KW",
    "KV": "XK", "KZ": "KZ", "LA": "LA", "LE": "LB", "LG": "LV", "LH": "LT",
    "LI": "LR", "LO": "SK", "LS": "LI", "LT": "LS", "LU": "LU", "LY": "LY",
    "MA": "MG", "MB": "MQ", "MC": "MO", "MD": "MD", "MF": "YT", "MG": "MN",
    "MH": "MS", "MI": "MW", "MJ": "ME", "MK": "MK", "ML": "ML", "MN": "MC",
    "MO": "MA", "MP": "MU", "MR": "MR", "MT": "MT", "MU": "OM", "MV": "MV",
    "MX": "MX", "MY": "MY", "MZ": "MZ", "NC": "NC", "NE": "NU", "NF": "NF",
    "NG": "NE", "NH": "VU", "NI": "NG", "NL": "NL", "NO": "NO", "NP": "NP",
    "NR": "NR", "NS": "SR", "NU": "NI", "NZ": "NZ", "PA": "PY", "PC": "PN",
    "PE": "PE", "PK": "PK", "PL": "PL", "PM": "PA", "PO": "PT", "PP": "PG",
    "PS": "PW", "PU": "GW", "QA": "QA", "RE": "MU", "RI": "RS", "RM": "MH",
    "RN": "MF", "RO": "RO", "RP": "PH", "RQ": "PR", "RS": "RU", "RW": "RW",
    "SA": "SA", "SB": "PM", "SC": "KN", "SE": "SC", "SF": "ZA", "SG": "SN",
    "SH": "SH", "SI": "SI", "SL": "SL", "SM": "SM", "SN": "SG", "SO": "SO",
    "SP": "ES", "ST": "LC", "SU": "SD", "SV": "SJ", "SW": "SE", "SX": "GS",
    "SY": "SY", "SZ": "CH", "TC": "AE", "TD": "TT", "TH": "TH", "TI": "TJ",
    "TK": "TC", "TL": "TK", "TN": "TO", "TO": "TG", "TP": "ST", "TS": "TN",
    "TT": "TL", "TU": "TR", "TV": "TV", "TW": "TW", "TX": "TM", "TZ": "TZ",
    "UC": "CW", "UG": "UG", "UK": "GB", "UM": "UM", "UP": "UA", "US": "US",
    "UV": "BF", "UY": "UY", "UZ": "UZ", "VC": "VC", "VE": "VE", "VI": "VG",
    "VM": "VN", "VQ": "VI", "VT": "VA", "WA": "NA", "WE": "PS", "WF": "WF",
    "WI": "EH", "WS": "WS", "WZ": "SZ", "YM": "YE", "ZA": "ZM", "ZI": "ZW",
}


# =============================================================================
# Master file list & URL fetching
# =============================================================================

def fetch_master_file_list() -> str:
    """Download the GDELT v2 master file list (~50 MB)."""
    resp = get(MASTER_FILE_LIST_URL)
    resp.raise_for_status()
    return resp.text


def urls_for_date(file_list_content: str, date_str: str, file_type: str) -> list[str]:
    """Return sorted URLs for a specific YYYYMMDD date and file_type (export / mentions / gkg)."""
    urls = []
    for line in file_list_content.strip().split("\n"):
        parts = line.split()
        if len(parts) < 3:
            continue
        url = parts[2]
        if date_str in url and f".{file_type}." in url.lower():
            urls.append(url)
    return sorted(urls)


def fetch_zip_csv(url: str) -> str | None:
    """Fetch a .CSV.zip file from GDELT and return the inner CSV as text.

    Returns None if the file returned 404 (intermittent GDELT availability).
    """
    resp = get(url)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        inner = zf.namelist()[0]
        with zf.open(inner) as f:
            return f.read().decode("utf-8", errors="replace")


# =============================================================================
# Events CSV parsing (string-typed; cast happens in cast_events)
# =============================================================================

def parse_events_tsv(csv_content: str) -> pa.Table:
    """Parse a GDELT events tab-delimited CSV into a string-typed PyArrow table.

    Empty fields become null. Oversized rows are truncated; undersized rows are
    right-padded with nulls.
    """
    n_cols = len(EVENTS_RAW_COLUMNS)
    columns: list[list[str | None]] = [[] for _ in range(n_cols)]

    for line in csv_content.strip().split("\n"):
        if not line:
            continue
        fields = line.split("\t")
        if len(fields) < n_cols:
            fields = fields + [""] * (n_cols - len(fields))
        elif len(fields) > n_cols:
            fields = fields[:n_cols]
        for i in range(n_cols):
            v = fields[i]
            columns[i].append(v if v else None)

    return pa.table({
        EVENTS_RAW_COLUMNS[i]: pa.array(columns[i], type=pa.string())
        for i in range(n_cols)
    })


# =============================================================================
# Cast a string-typed raw events table to EVENTS_TARGET_TYPES
# =============================================================================

def _cast_column(col: pa.ChunkedArray, target: pa.DataType) -> pa.Array:
    """Cast a string column to the target type, tolerating empty/null inputs."""
    if target == pa.date32():
        ts = pc.strptime(col, format="%Y%m%d", unit="s")
        return pc.cast(ts, pa.date32())
    if target == pa.timestamp("s"):
        return pc.strptime(col, format="%Y%m%d%H%M%S", unit="s")
    if target == pa.bool_():
        return pc.equal(col, "1")
    return pc.cast(col, target)


def cast_events(table: pa.Table) -> pa.Table:
    """Cast a string-typed raw events table to EVENTS_TARGET_TYPES.

    Columns not listed in EVENTS_TARGET_TYPES stay as strings.
    """
    new_columns = []
    new_names = []
    for name in table.column_names:
        col = table[name]
        target = EVENTS_TARGET_TYPES.get(name)
        if target is not None and col.type != target:
            col = _cast_column(col, target)
        new_columns.append(col)
        new_names.append(name)
    return pa.table(new_columns, names=new_names)


# =============================================================================
# CAMEO + FIPS codelist fetching
# =============================================================================

def _parse_tsv_codelist(text: str) -> pa.Table:
    """Parse a tab-delimited CODE<TAB>LABEL codelist. Drops header row."""
    codes, labels = [], []
    for i, line in enumerate(text.strip().split("\n")):
        if i == 0 or not line.strip():
            continue
        parts = line.split("\t", 1)
        if len(parts) < 2:
            continue
        codes.append(parts[0].strip())
        labels.append(parts[1].strip())
    return pa.table({"code": codes, "label": labels})


def download_codelists() -> dict[str, pa.Table]:
    """Fetch all CAMEO lookups + FIPS country names, save each as raw parquet.

    Returns a dict mapping raw asset id (e.g. 'cameo_eventcodes') to its PyArrow
    table with columns ['code', 'label'].
    """
    out: dict[str, pa.Table] = {}
    for asset_id, url in CAMEO_CODELISTS.items():
        print(f"  Fetching {asset_id} from {url}")
        resp = get(url)
        resp.raise_for_status()
        tbl = _parse_tsv_codelist(resp.text)
        save_raw_parquet(tbl, asset_id)
        out[asset_id] = tbl
    return out


def load_codelists() -> dict[str, pa.Table]:
    """Load previously-downloaded codelists from raw storage."""
    return {
        asset_id: load_raw_parquet(asset_id)
        for asset_id in CAMEO_CODELISTS.keys()
    }


# =============================================================================
# Denormalization
# =============================================================================

def _codelist_to_dict(tbl: pa.Table) -> dict[str, str]:
    return dict(zip(tbl["code"].to_pylist(), tbl["label"].to_pylist()))


def denormalize_events(table: pa.Table, codelists: dict[str, pa.Table]) -> pa.Table:
    """Add label/ISO2 columns to a typed events table.

    New columns (all left joins — never drops rows):
      - quad_class_label                             (from QUAD_CLASS_LABELS)
      - event_root_label, event_base_label, event_label  (from CAMEO eventcodes)
      - actor1_country_label, actor2_country_label       (from CAMEO country)
      - {actor1,actor2,action}_geo_country_iso2           (from FIPS_TO_ISO2)
      - {actor1,actor2,action}_geo_country_name           (from FIPS country list)
    """
    event_lookup    = _codelist_to_dict(codelists["cameo_eventcodes"])
    cameo_country   = _codelist_to_dict(codelists["cameo_country"])
    fips_country    = _codelist_to_dict(codelists["fips_country"])

    def map_col(col_name: str, mapping: dict[str, str]) -> pa.Array:
        values = table[col_name].to_pylist()
        mapped = [mapping.get(v) if v is not None else None for v in values]
        return pa.array(mapped, type=pa.string())

    def map_fips_iso(col_name: str) -> pa.Array:
        values = table[col_name].to_pylist()
        mapped = [FIPS_TO_ISO2.get(v) if v is not None else None for v in values]
        return pa.array(mapped, type=pa.string())

    def map_quad(col_name: str) -> pa.Array:
        values = table[col_name].to_pylist()
        mapped = [QUAD_CLASS_LABELS.get(v) if v is not None else None for v in values]
        return pa.array(mapped, type=pa.string())

    additions = {
        "quad_class_label":              map_quad("quad_class"),
        "event_root_label":              map_col("event_root_code", event_lookup),
        "event_base_label":              map_col("event_base_code", event_lookup),
        "event_label":                   map_col("event_code", event_lookup),
        "actor1_country_label":          map_col("actor1_country_code", cameo_country),
        "actor2_country_label":          map_col("actor2_country_code", cameo_country),
        "actor1_geo_country_iso2":       map_fips_iso("actor1_geo_country_code"),
        "actor1_geo_country_name":       map_col("actor1_geo_country_code", fips_country),
        "actor2_geo_country_iso2":       map_fips_iso("actor2_geo_country_code"),
        "actor2_geo_country_name":       map_col("actor2_geo_country_code", fips_country),
        "action_geo_country_iso2":       map_fips_iso("action_geo_country_code"),
        "action_geo_country_name":       map_col("action_geo_country_code", fips_country),
    }

    result = table
    for name, arr in additions.items():
        result = result.append_column(name, arr)
    return result


# =============================================================================
# Raw presence check (used by download() to skip already-fetched days)
# =============================================================================

def raw_day_exists(date_str: str) -> bool:
    """True if events_<YYYYMMDD>.parquet already exists in raw storage."""
    return raw_asset_exists(f"events_{date_str}")


# =============================================================================
# Metadata truncation for the Delta 4KB table-description cap
# =============================================================================

_DELTA_DESCRIPTION_MAX = 4000


def fit_metadata_for_delta(metadata: dict) -> dict:
    """Return a copy of metadata whose JSON-serialized form fits the Delta 4KB cap.

    Progressively shrinks the longest column_descriptions until the payload fits.
    The top-level fields (id, title, description, license, ...) are preserved
    verbatim; only column_descriptions values are trimmed.
    """
    import json

    def size_of(m: dict) -> int:
        return len(json.dumps(m))

    if size_of(metadata) <= _DELTA_DESCRIPTION_MAX:
        return metadata

    out = {k: v for k, v in metadata.items()}
    col_descs = dict(out.get("column_descriptions") or {})
    out["column_descriptions"] = col_descs

    while size_of(out) > _DELTA_DESCRIPTION_MAX and col_descs:
        longest_key = max(col_descs, key=lambda k: len(col_descs[k]))
        v = col_descs[longest_key]
        if len(v) <= 12:
            col_descs.pop(longest_key)
        else:
            col_descs[longest_key] = v[: max(12, len(v) // 2)].rstrip() + "…"

    return out
