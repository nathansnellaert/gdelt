# GDELT Global Events

**Source:** Global Database of Events, Language, and Tone (GDELT Project)
**Data portal:** https://www.gdeltproject.org/
**License:** Creative Commons Attribution-NonCommercial 4.0 (https://www.gdeltproject.org/about.html#termsofuse)

## Published datasets

| ID | Grain | Row count (approx) |
|---|---|---|
| `gdelt_events` | One row per CAMEO-coded event (primary key `global_event_id`) | ~600M |
| `gdelt_events_daily` | `day` x `action_geo_country_iso2` x `event_root_code` x `quad_class` aggregate | ~5-10M |

Both datasets cover **GDELT v2 events, 2015-02-18 to present** and update daily.

## What we denormalize

- **Event codes (CAMEO).** `event_root_code`, `event_base_code`, and `event_code` are joined to the CAMEO event codelist (http://gdeltproject.org/data/lookups/CAMEO.eventcodes.txt) to produce `event_root_label`, `event_base_label`, `event_label`.
- **Quad class.** 1..4 is expanded to `quad_class_label` (Verbal Cooperation / Material Cooperation / Verbal Conflict / Material Conflict).
- **Actor countries (CAMEO).** `actor1_country_code` / `actor2_country_code` are 3-letter CAMEO country codes (not ISO). They are joined to CAMEO.country.txt to add `actor{1,2}_country_label`. CAMEO country codes include non-country entities (cities, continents) so we do **not** attempt an ISO mapping for actor countries.
- **Geo countries (FIPS).** `actor{1,2}_geo_country_code` and `action_geo_country_code` are FIPS 10-4 two-letter codes. A static FIPS to ISO 3166-1 alpha-2 map produces `*_geo_country_iso2`; FIPS.country.txt provides `*_geo_country_name`.

All numeric columns are properly typed (`float64` for Goldstein / tone / lat / long, `int32` for counts, `int64` for `global_event_id`, `date32` for `day`, `timestamp[s]` for `date_added`).

## Coverage decisions

**Included:** events (row-level) and events daily aggregate.

**Deliberately skipped:**
- **mentions** (~400-800 GB). Every mention of every event across every source. Over our storage budget; low marginal value vs events.
- **gkg** (~1-3 TB). Global Knowledge Graph with themes/entities/sentiment per article. Wide rows, expensive to parse. Revisit if a specific NLP use case justifies it.
- **GDELT v1 (1979-2015).** Different schema and cadence. Not worth a second code path; v2 covers all real-time use cases.

**Known data gap:** 2025-06-15 to 2025-07-01 (17 days, GDELT source outage). Files were unavailable upstream; not re-attempted.

## Architecture

```
nodes/events.py
  download()          fetches raw CSV.zip files into data/raw/events_YYYYMMDD.parquet
  transform_events()  casts, denormalizes, merges into gdelt_events + gdelt_events_daily

connector_utils.py    schemas, HTTP helpers, CAMEO/FIPS fetching, denormalization
```

The `download()` node respects a 5.8-hour GH Actions runtime budget and returns `True` to signal re-triggering if the backfill is incomplete. It skips any day whose raw parquet already exists in storage, so a lost state file cannot cause re-ingestion.

`transform_events()` processes raw days in 7-day batches, casting and denormalizing each batch before merging. The daily aggregate is computed incrementally within the same node (scanning the full 600M-row `gdelt_events` table from scratch would not fit in memory).

## Merge keys

- `gdelt_events`: `global_event_id` (verified unique and non-null on sampled days). Partitioned by `year`.
- `gdelt_events_daily`: composite `(day, action_geo_country_iso2, event_root_code, quad_class)`.
