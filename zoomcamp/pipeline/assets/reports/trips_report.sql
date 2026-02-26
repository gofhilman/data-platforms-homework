/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

name: reports.trips_report

type: bq.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: pickup_date
    type: date
    description: "Date of the trip pickup"
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow, green)"
    primary_key: true
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - "yellow"
          - "green"
  - name: payment_type_name
    type: string
    description: "Name of the payment method"
    primary_key: true
  - name: total_trips
    type: integer
    description: "Total number of trips"
    checks:
      - name: positive
  - name: total_passengers
    type: float
    description: "Total number of passengers"
    checks:
      - name: non_negative
  - name: total_fare
    type: float
    description: "Total fare amount"
    checks:
      - name: not_null
  - name: total_tip
    type: float
    description: "Total tip amount"
    checks:
      - name: not_null
  - name: total_amount
    type: float
    description: "Total amount (fare + extras + tips + tolls)"
    checks:
      - name: not_null
  - name: avg_trip_distance
    type: float
    description: "Average trip distance"
    checks:
      - name: non_negative

@bruin */

SELECT
  CAST(pickup_datetime AS DATE) AS pickup_date,
  taxi_type,
  COALESCE(payment_type_name, 'Unknown') AS payment_type_name,
  COUNT(*) AS total_trips,
  SUM(passenger_count) AS total_passengers,
  SUM(fare_amount) AS total_fare,
  SUM(tip_amount) AS total_tip,
  SUM(total_amount) AS total_amount,
  AVG(trip_distance) AS avg_trip_distance
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
  CAST(pickup_datetime AS DATE),
  taxi_type,
  COALESCE(payment_type_name, 'Unknown')
