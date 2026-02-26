# data-platforms-homework

Homework solution for Data Platforms

Problems: <https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/05-data-platforms/homework.md>

1. The required files/directories are `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`.

    `/zoomcamp` directory contains end-to-end pipeline code for this project.

2. The best incremental strategy for processing a specific interval period by deleting and inserting data for that time period is `time_interval` - incremental based on a time column.

3. We can override default value when running the pipeline to only process yellow taxis by running `bruin run --var 'taxi_types=["yellow"]'`.

4. The command to run `ingestion/trips.py` plus all downstream assets is `bruin run ingestion/trips.py --downstream`.

5. The quality check to ensure the `pickup_datetime` column in the trips table never has `NULL` values is `name: not_null`.

6. Bruin command to visualize the dependency graph between assets is `bruin lineage`.

7. The flag to ensure tables are created from scratch is `--full-refresh`.
