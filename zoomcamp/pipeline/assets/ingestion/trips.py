"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# Pick a Python image version (Bruin runs Python in isolated environments).
image: python:3.11

# Set the connection.
connection: gcp-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
# Pick a materialization schema matching column extraction
columns:
  - name: VendorID
    type: integer
  - name: tpep_pickup_datetime
    type: timestamp
  - name: tpep_dropoff_datetime
    type: timestamp
  - name: passenger_count
    type: float
  - name: trip_distance
    type: float
  - name: RatecodeID
    type: float
  - name: store_and_fwd_flag
    type: string
  - name: PULocationID
    type: integer
  - name: DOLocationID
    type: integer
  - name: payment_type
    type: integer
  - name: fare_amount
    type: float
  - name: extra
    type: float
  - name: mta_tax
    type: float
  - name: tip_amount
    type: float
  - name: tolls_amount
    type: float
  - name: improvement_surcharge
    type: float
  - name: total_amount
    type: float
  - name: congestion_surcharge
    type: float
  - name: airport_fee
    type: float
  - name: lpep_pickup_datetime
    type: timestamp
  - name: lpep_dropoff_datetime
    type: timestamp
  - name: ehail_fee
    type: float
  - name: trip_type
    type: float
  - name: taxi_type
    type: string
  - name: extracted_at
    type: timestamp

@bruin"""

import pandas as pd
import requests
from io import BytesIO
import datetime
from dateutil.relativedelta import relativedelta
import json
import os

def materialize():
    start_date_str = os.environ.get("BRUIN_START_DATE")
    end_date_str = os.environ.get("BRUIN_END_DATE")
    
    start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
    # Bruin end date is exclusive for time intervals (e.g. 2022-01-01 to 2022-02-01). 
    end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
    
    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow", "green"])
    
    dfs = []
    
    # We want to iterate by month from start_date to end_date - 1 day
    current_date = start_date
    while current_date < end_date:
        year = current_date.year
        month = current_date.month
        month_str = f"{month:02d}"
        
        for taxi_type in taxi_types:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month_str}.parquet"
            print(f"Fetching {url}")
            
            try:
                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    df = pd.read_parquet(BytesIO(response.content))
                    df["taxi_type"] = taxi_type
                    df["extracted_at"] = pd.Timestamp.utcnow()
                    dfs.append(df)
                    print(f"Loaded {len(df)} rows for {taxi_type} {year}-{month_str}")
                else:
                    print(f"Failed to fetch {url}: API Status {response.status_code}")
            except Exception as e:
                print(f"Error fetching {url}: {e}")
        
        current_date += relativedelta(months=1)

    if not dfs:
        # Return empty DF with expected schema if no data
        return pd.DataFrame()
        
    final_df = pd.concat(dfs, ignore_index=True)
    return final_df



