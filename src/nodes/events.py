"""GDELT 2.0 events ingestion and publishing.

Three nodes:
- download: fetch 15-minute export files, save one parquet per day to raw storage.
- transform_events: cast, denormalize (FIPS→ISO, CAMEO codes→labels), merge into
  two published datasets: gdelt_events (row-level) and gdelt_events_daily
  (country × event_root × quad_class aggregate).
"""
import time
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.compute as pc

from subsets_utils import (
    load_state, save_state,
    save_raw_parquet, load_raw_parquet,
    merge, publish,
    raw_asset_exists,
)
from subsets_utils.testing import assert_in_range, assert_in_set

from connector_utils import (
    GDELT_START_DATE, GH_ACTIONS_MAX_RUN_SECONDS,
    fetch_master_file_list, urls_for_date, fetch_zip_csv,
    parse_events_tsv, cast_events, denormalize_events,
    download_codelists, load_codelists,
    raw_day_exists,
    fit_metadata_for_delta,
)


# =============================================================================
# Published dataset IDs and metadata
# =============================================================================

EVENTS_ID = "gdelt_events"
EVENTS_DAILY_ID = "gdelt_events_daily"

TRANSFORM_BATCH_DAYS = 7


def _events_column_descriptions() -> dict[str, str]:
    return {
        "global_event_id": "Unique event identifier assigned by GDELT.",
        "day": "Date of the event (UTC).",
        "month_year": "Month of the event as YYYYMM integer.",
        "year": "Year of the event.",
        "fraction_date": "Fractional year of the event (e.g. 2026.0192).",
        "actor1_code": "Full CAMEO actor code for the initiating party (country + attributes).",
        "actor1_name": "Actor1 name as extracted by GDELT.",
        "actor1_country_code": "CAMEO country code for Actor1 (3-letter, CAMEO-specific; not ISO).",
        "actor1_country_label": "Human-readable CAMEO country label for Actor1.",
        "actor1_known_group_code": "CAMEO known-group code for Actor1, if any.",
        "actor1_ethnic_code": "CAMEO ethnic code for Actor1, if any.",
        "actor1_religion1_code": "Primary CAMEO religion code for Actor1, if any.",
        "actor1_religion2_code": "Secondary CAMEO religion code for Actor1, if any.",
        "actor1_type1_code": "Primary CAMEO role/type code for Actor1 (e.g. GOV, MIL, COP).",
        "actor1_type2_code": "Secondary CAMEO role/type code for Actor1.",
        "actor1_type3_code": "Tertiary CAMEO role/type code for Actor1.",
        "actor2_code": "Full CAMEO actor code for the receiving party.",
        "actor2_name": "Actor2 name as extracted by GDELT.",
        "actor2_country_code": "CAMEO country code for Actor2 (3-letter, CAMEO-specific; not ISO).",
        "actor2_country_label": "Human-readable CAMEO country label for Actor2.",
        "actor2_known_group_code": "CAMEO known-group code for Actor2, if any.",
        "actor2_ethnic_code": "CAMEO ethnic code for Actor2, if any.",
        "actor2_religion1_code": "Primary CAMEO religion code for Actor2, if any.",
        "actor2_religion2_code": "Secondary CAMEO religion code for Actor2, if any.",
        "actor2_type1_code": "Primary CAMEO role/type code for Actor2.",
        "actor2_type2_code": "Secondary CAMEO role/type code for Actor2.",
        "actor2_type3_code": "Tertiary CAMEO role/type code for Actor2.",
        "is_root_event": "True if this is a top-level event in its source document.",
        "event_code": "Full CAMEO event code (specific action).",
        "event_label": "Human-readable label for event_code.",
        "event_base_code": "CAMEO event base code (intermediate granularity).",
        "event_base_label": "Human-readable label for event_base_code.",
        "event_root_code": "CAMEO event root code (01-20, broadest granularity).",
        "event_root_label": "Human-readable label for event_root_code.",
        "quad_class": "1=Verbal Cooperation, 2=Material Cooperation, 3=Verbal Conflict, 4=Material Conflict.",
        "quad_class_label": "Human-readable label for quad_class.",
        "goldstein_scale": "Theoretical impact of the event on the stability of a country (-10 to +10).",
        "num_mentions": "Count of all mentions of this event across the source document set.",
        "num_sources": "Count of distinct sources mentioning this event.",
        "num_articles": "Count of distinct articles mentioning this event.",
        "avg_tone": "Average tone of all documents mentioning the event (-100 to +100).",
        "actor1_geo_type": "Actor1 location granularity (0=none, 1=country, 2=US state, 3=US city, 4=world city, 5=world state).",
        "actor1_geo_fullname": "Actor1 location as a human-readable string.",
        "actor1_geo_country_code": "FIPS 10-4 country code of Actor1 location.",
        "actor1_geo_country_iso2": "ISO 3166-1 alpha-2 country code of Actor1 location (denormalized).",
        "actor1_geo_country_name": "Country name of Actor1 location.",
        "actor1_geo_adm1_code": "ADM1 (state/province) code of Actor1 location.",
        "actor1_geo_adm2_code": "ADM2 (county/district) code of Actor1 location.",
        "actor1_geo_lat": "Latitude of Actor1 location.",
        "actor1_geo_long": "Longitude of Actor1 location.",
        "actor1_geo_feature_id": "GeoNames feature ID or country code of Actor1 location.",
        "actor2_geo_type": "Actor2 location granularity (see actor1_geo_type).",
        "actor2_geo_fullname": "Actor2 location as a human-readable string.",
        "actor2_geo_country_code": "FIPS 10-4 country code of Actor2 location.",
        "actor2_geo_country_iso2": "ISO 3166-1 alpha-2 country code of Actor2 location (denormalized).",
        "actor2_geo_country_name": "Country name of Actor2 location.",
        "actor2_geo_adm1_code": "ADM1 code of Actor2 location.",
        "actor2_geo_adm2_code": "ADM2 code of Actor2 location.",
        "actor2_geo_lat": "Latitude of Actor2 location.",
        "actor2_geo_long": "Longitude of Actor2 location.",
        "actor2_geo_feature_id": "GeoNames feature ID or country code of Actor2 location.",
        "action_geo_type": "Action location granularity (see actor1_geo_type).",
        "action_geo_fullname": "Action location as a human-readable string.",
        "action_geo_country_code": "FIPS 10-4 country code of action location.",
        "action_geo_country_iso2": "ISO 3166-1 alpha-2 country code of action location (denormalized).",
        "action_geo_country_name": "Country name of action location.",
        "action_geo_adm1_code": "ADM1 code of action location.",
        "action_geo_adm2_code": "ADM2 code of action location.",
        "action_geo_lat": "Latitude of action location.",
        "action_geo_long": "Longitude of action location.",
        "action_geo_feature_id": "GeoNames feature ID or country code of action location.",
        "date_added": "Timestamp when this event was added to the GDELT database.",
        "source_url": "URL of a source document reporting this event.",
    }


EVENTS_METADATA = {
    "id": EVENTS_ID,
    "title": "GDELT 2.0 Events (CAMEO-coded global news events)",
    "description": (
        "Global events extracted from news media by the GDELT Project, coded "
        "using the CAMEO taxonomy. Each row is one event with actors, location, "
        "event type, Goldstein conflict-cooperation score, and average tone of "
        "coverage. Covers 2015-02-18 onward at 15-minute source resolution. "
        "CAMEO event and quad-class codes are denormalized to human-readable "
        "labels; location country codes are denormalized from FIPS 10-4 to ISO "
        "3166-1 alpha-2. Actor country codes remain as CAMEO codes (they are "
        "3-letter CAMEO-specific codes, not ISO) with human-readable labels added."
    ),
    "license": "CC BY-NC 4.0",
    "license_url": "https://www.gdeltproject.org/about.html#termsofuse",
    "source_url": "https://www.gdeltproject.org/",
    "column_descriptions": _events_column_descriptions(),
}


EVENTS_DAILY_METADATA = {
    "id": EVENTS_DAILY_ID,
    "title": "GDELT 2.0 Events — Daily Country Aggregate",
    "description": (
        "Daily aggregate of GDELT events by action-location country (ISO 3166-1 "
        "alpha-2), CAMEO event root category, and quad class (Verbal/Material × "
        "Cooperation/Conflict). Convenience dataset for fast country-level "
        "conflict-cooperation queries; derived from gdelt_events."
    ),
    "license": "CC BY-NC 4.0",
    "license_url": "https://www.gdeltproject.org/about.html#termsofuse",
    "source_url": "https://www.gdeltproject.org/",
    "column_descriptions": {
        "day": "Date of events (UTC).",
        "action_geo_country_iso2": "ISO 3166-1 alpha-2 country where the action took place.",
        "event_root_code": "CAMEO event root code (01-20).",
        "event_root_label": "Human-readable CAMEO root category.",
        "quad_class": "1=Verbal Cooperation, 2=Material Cooperation, 3=Verbal Conflict, 4=Material Conflict.",
        "quad_class_label": "Human-readable quad class label.",
        "num_events": "Number of events in this bucket.",
        "sum_mentions": "Total mentions across source documents.",
        "sum_articles": "Total distinct articles.",
        "avg_goldstein": "Mean Goldstein score (-10 to +10).",
        "avg_tone": "Mean document tone (-100 to +100).",
    },
}


# =============================================================================
# download — fetch raw events for each day into raw storage
# =============================================================================

def download() -> bool:
    """Fetch GDELT events day-by-day to raw parquet. Returns True if more work remains."""
    print("Ingesting GDELT events...")
    start_time = time.time()

    print("  Refreshing CAMEO + FIPS codelists...")
    download_codelists()

    print("  Fetching master file list...")
    file_list_content = fetch_master_file_list()

    state = load_state("events_ingest")
    last_processed_date = state.get("last_processed_date")
    cumulative_events = state.get("total_events_ingested", 0)

    if last_processed_date:
        start_date = datetime.strptime(last_processed_date, "%Y-%m-%d") + timedelta(days=1)
    else:
        start_date = GDELT_START_DATE

    yesterday = datetime.now() - timedelta(days=1)
    end_date = yesterday.replace(hour=23, minute=59, second=59)

    if start_date > end_date:
        print("  Already caught up - no new data to process")
        return False

    days_remaining = (end_date - start_date).days + 1
    print(f"  Processing {days_remaining} days from {start_date:%Y-%m-%d} to {end_date:%Y-%m-%d}")

    current_date = start_date
    days_processed = 0
    total_events_run = 0

    while current_date <= end_date:
        elapsed = time.time() - start_time
        if elapsed >= GH_ACTIONS_MAX_RUN_SECONDS:
            print(f"\n  Time budget exhausted after {days_processed} days ({elapsed/3600:.1f}h)")
            print(f"  Total events fetched this run: {total_events_run:,}")
            return True

        date_str = current_date.strftime("%Y%m%d")

        # Guard: skip if raw file already exists. Defends against state loss.
        if raw_day_exists(date_str):
            print(f"  [{days_processed + 1}/{days_remaining}] {current_date:%Y-%m-%d}  (raw exists, skip)")
            save_state("events_ingest", {
                "last_processed_date": current_date.strftime("%Y-%m-%d"),
                "total_events_ingested": cumulative_events,
            })
            days_processed += 1
            current_date += timedelta(days=1)
            continue

        print(f"  [{days_processed + 1}/{days_remaining}] {current_date:%Y-%m-%d}")
        events_count = _fetch_day_events(date_str, file_list_content)
        total_events_run += events_count
        cumulative_events += events_count
        print(f"    -> {events_count:,} events")

        save_state("events_ingest", {
            "last_processed_date": current_date.strftime("%Y-%m-%d"),
            "total_events_ingested": cumulative_events,
        })
        days_processed += 1
        current_date += timedelta(days=1)

    print(f"\n  Completed! Processed {days_processed} days, {total_events_run:,} events this run")
    return False


def _fetch_day_events(date_str: str, file_list_content: str) -> int:
    urls = urls_for_date(file_list_content, date_str, "export")
    if not urls:
        print(f"    No export files found for {date_str}")
        return 0

    all_tables: list[pa.Table] = []
    skipped = 0
    for i, url in enumerate(urls, 1):
        csv_content = fetch_zip_csv(url)
        if csv_content is None:
            skipped += 1
            continue
        all_tables.append(parse_events_tsv(csv_content))
        if i % 24 == 0:
            print(f"      Processed {i}/{len(urls)} files...")
    if skipped > 0:
        print(f"      Skipped {skipped} unavailable files")

    if not all_tables:
        print(f"    No events for {date_str}")
        return 0

    combined = pa.concat_tables(all_tables)
    save_raw_parquet(combined, f"events_{date_str}")
    return combined.num_rows


# =============================================================================
# transform_events — cast, denormalize, merge into gdelt_events + daily rollup
# =============================================================================

def transform_events() -> bool:
    """Cast, denormalize, and publish gdelt_events + gdelt_events_daily.

    Returns True if more work remains (used by orchestrator to re-trigger).
    """
    print("Transforming GDELT events...")
    start_time = time.time()

    try:
        codelists = load_codelists()
    except FileNotFoundError:
        print("  Codelists missing; fetching now...")
        codelists = download_codelists()

    state = load_state("gdelt_events_transform")
    processed = set(state.get("processed_days", []))
    print(f"  Already processed: {len(processed)} days")

    download_state = load_state("events_ingest")
    last_downloaded = download_state.get("last_processed_date")
    if not last_downloaded:
        print("  No downloaded data available; run download first.")
        return False

    end = datetime.strptime(last_downloaded, "%Y-%m-%d")
    current = GDELT_START_DATE
    pending: list[str] = []
    while current <= end:
        date_str = current.strftime("%Y%m%d")
        if date_str not in processed and raw_day_exists(date_str):
            pending.append(date_str)
        current += timedelta(days=1)

    if not pending:
        print("  Transform already caught up.")
        publish(EVENTS_ID, EVENTS_METADATA)
        publish(EVENTS_DAILY_ID, EVENTS_DAILY_METADATA)
        return False

    print(f"  {len(pending)} days pending transform")
    batch: list[pa.Table] = []
    batch_days: list[str] = []
    days_done = 0

    for date_str in pending:
        elapsed = time.time() - start_time
        if elapsed >= GH_ACTIONS_MAX_RUN_SECONDS:
            if batch:
                _flush_batch(batch, batch_days, processed)
            print(f"\n  Time budget exhausted after {days_done} days ({elapsed/3600:.1f}h)")
            return True

        raw = load_raw_parquet(f"events_{date_str}")
        typed = cast_events(raw)
        denormed = denormalize_events(typed, codelists)
        batch.append(denormed)
        batch_days.append(date_str)
        days_done += 1

        if len(batch_days) >= TRANSFORM_BATCH_DAYS:
            _flush_batch(batch, batch_days, processed)
            batch = []
            batch_days = []

    if batch:
        _flush_batch(batch, batch_days, processed)

    publish(EVENTS_ID, fit_metadata_for_delta(EVENTS_METADATA))
    publish(EVENTS_DAILY_ID, fit_metadata_for_delta(EVENTS_DAILY_METADATA))
    print(f"\n  Completed! Transformed {days_done} days this run.")
    return False


def _flush_batch(batch: list[pa.Table], batch_days: list[str], processed: set[str]) -> None:
    combined = pa.concat_tables(batch)
    print(f"  Flushing batch: {len(batch_days)} days, {combined.num_rows:,} rows")

    _validate_events_batch(combined)

    merge(combined, EVENTS_ID, key=["global_event_id"], partition_by=["year"])

    daily = _aggregate_daily(combined)
    if daily.num_rows > 0:
        merge(
            daily,
            EVENTS_DAILY_ID,
            key=["day", "action_geo_country_iso2", "event_root_code", "quad_class"],
        )

    processed.update(batch_days)
    save_state("gdelt_events_transform", {"processed_days": sorted(processed)})


def _validate_events_batch(table: pa.Table) -> None:
    """Sanity checks before merging into gdelt_events."""
    assert_in_range(table, "goldstein_scale", -10, 10)
    assert_in_range(table, "avg_tone", -100, 100)
    assert_in_set(table, "quad_class", {1, 2, 3, 4})


def _aggregate_daily(events: pa.Table) -> pa.Table:
    """Aggregate a batch of denormalized events into the daily rollup shape.

    Drops rows where action_geo_country_iso2 is null (can't group without a country).
    """
    mask = pc.is_valid(events["action_geo_country_iso2"])
    filtered = events.filter(mask)
    if filtered.num_rows == 0:
        return pa.table({
            "day": pa.array([], pa.date32()),
            "action_geo_country_iso2": pa.array([], pa.string()),
            "event_root_code": pa.array([], pa.string()),
            "event_root_label": pa.array([], pa.string()),
            "quad_class": pa.array([], pa.int8()),
            "quad_class_label": pa.array([], pa.string()),
            "num_events": pa.array([], pa.int64()),
            "sum_mentions": pa.array([], pa.int64()),
            "sum_articles": pa.array([], pa.int64()),
            "avg_goldstein": pa.array([], pa.float64()),
            "avg_tone": pa.array([], pa.float64()),
        })

    group_keys = [
        "day",
        "action_geo_country_iso2",
        "event_root_code",
        "event_root_label",
        "quad_class",
        "quad_class_label",
    ]
    agg = filtered.group_by(group_keys).aggregate([
        ("global_event_id", "count"),
        ("num_mentions",    "sum"),
        ("num_articles",    "sum"),
        ("goldstein_scale", "mean"),
        ("avg_tone",        "mean"),
    ])

    renames = {
        "global_event_id_count": "num_events",
        "num_mentions_sum":      "sum_mentions",
        "num_articles_sum":      "sum_articles",
        "goldstein_scale_mean":  "avg_goldstein",
        "avg_tone_mean":         "avg_tone",
    }
    new_names = [renames.get(n, n) for n in agg.column_names]
    agg = agg.rename_columns(new_names)

    casts = {
        "num_events":    pa.int64(),
        "sum_mentions":  pa.int64(),
        "sum_articles":  pa.int64(),
        "avg_goldstein": pa.float64(),
        "avg_tone":      pa.float64(),
    }
    cols = []
    names = []
    for name in agg.column_names:
        col = agg[name]
        target = casts.get(name)
        if target is not None and col.type != target:
            col = pc.cast(col, target)
        cols.append(col)
        names.append(name)
    return pa.table(cols, names=names)


NODES = {
    download: [],
    transform_events: [download],
}


if __name__ == "__main__":
    from subsets_utils import validate_environment
    validate_environment()
    download()
    transform_events()
