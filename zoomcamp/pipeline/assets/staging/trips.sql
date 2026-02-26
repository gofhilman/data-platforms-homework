/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

name: staging.trips
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
type: bq.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table
  # TODO: set a materialization strategy.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  # suggested strategy: time_interval
  #
  # Incremental strategies (what does "incremental" mean?):
  # Incremental means you update only part of the destination instead of rebuilding everything every run.
  # In Bruin, this is controlled by `strategy` plus keys like `incremental_key` and `time_granularity`.
  #
  # Common strategies you can choose from (see docs for full list):
  # - create+replace (full rebuild)
  # - truncate+insert (full refresh without drop/create)
  # - append (insert new rows only)
  # - delete+insert (refresh partitions based on incremental_key values)
  # - merge (upsert based on primary key)
  # - time_interval (refresh rows within a time window)
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: trip_id
    type: string
    description: "Unique identifier for the trip"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: unique
  - name: pickup_datetime
    type: timestamp
    description: "When the trip started"
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: "The base fare amount"
    checks:
      - name: not_null

custom_checks:
  - name: validate_payment_match
    description: "Ensure all records have a valid mapped payment type"
    query: |
      SELECT count(*)
      FROM staging.trips
      WHERE payment_type_name IS NULL AND payment_type_id IS NOT NULL
    value: 0

@bruin */

-- TODO: Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

WITH base_trips AS (
  SELECT 
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    vendor_id,
    passenger_count,
    trip_distance,
    ratecode_id,
    store_and_fwd_flag,
    pu_location_id AS pickup_location_id,
    do_location_id AS dropoff_location_id,
    payment_type AS payment_type_id,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    taxi_type,
    extracted_at
  FROM ingestion.trips
  WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
    AND tpep_pickup_datetime < '{{ end_datetime }}'
),
deduplicated_trips AS (
  SELECT 
    *,
    GENERATE_UUID() as trip_id,
    ROW_NUMBER() OVER (
      PARTITION BY 
        pickup_datetime, 
        dropoff_datetime, 
        pickup_location_id, 
        dropoff_location_id, 
        CAST(fare_amount AS STRING)
      ORDER BY extracted_at DESC
    ) as rn
  FROM base_trips
)
SELECT 
  t.trip_id,
  t.vendor_id,
  t.pickup_datetime,
  t.dropoff_datetime,
  t.passenger_count,
  t.trip_distance,
  t.ratecode_id,
  t.store_and_fwd_flag,
  t.pickup_location_id,
  t.dropoff_location_id,
  t.payment_type_id,
  p.payment_type_name,
  t.fare_amount,
  t.extra,
  t.mta_tax,
  t.tip_amount,
  t.tolls_amount,
  t.improvement_surcharge,
  t.total_amount,
  t.congestion_surcharge,
  t.airport_fee,
  t.taxi_type,
  t.extracted_at
FROM deduplicated_trips t
LEFT JOIN ingestion.payment_lookup p
  ON t.payment_type_id = p.payment_type_id
WHERE t.rn = 1

