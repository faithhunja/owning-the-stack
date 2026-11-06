import os
import requests
from io import BytesIO
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.duckdb.hooks.duckdb import DuckDBHook

# Configuration

BUCKET = "owning-the-stack"
S3_ENDPOINT = "host.docker.internal:8080"
YEAR = 2025
TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"

BRONZE_KEY   = "bronze/yellow_tripdata_%s.parquet"
SILVER_KEY   = "silver/yellow_tripdata_%s.parquet"
GOLD_KEY     = "gold/monthly_summary_%s.parquet"

# Helper functions

def sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"

def create_secret(conn) -> None:
    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")
    if not access_key or not secret_key:
        raise ValueError("S3_ACCESS_KEY and S3_SECRET_KEY must be set.")
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute(
        f"""
        CREATE OR REPLACE SECRET microceph_s3 (
            TYPE s3,
            KEY_ID {sql_str(access_key)},
            SECRET {sql_str(secret_key)},
            ENDPOINT {sql_str(S3_ENDPOINT)},
            URL_STYLE 'path',
            USE_SSL false
        )
        """
    )

def duckdb_hook() -> DuckDBHook:
    return DuckDBHook(
        extensions=["httpfs"],
        autoinstall_extensions=True,
        memory_limit="2GB",
        threads=4,
    )

# DAG

@dag(
    dag_id="nyc_yellow_taxi_pipeline",
    description="Incremental NYC Taxi data ingestion → Bronze/Silver/Gold on MicroCeph with DuckDB.",
    start_date=datetime(YEAR, 1, 1),
    schedule=None, # Manual trigger for demo
    catchup=False,
    params={"year": YEAR},
    tags=["demo", "microceph", "openinfra"],
)
def nyc_taxi_pipeline():

    # 1. DISCOVERY - pick the next missing month from the bucket
    @task
    def discover_next_month(**context):
        year = int(context["params"]["year"])

        # List every Bronze file for this year: bronze/yellow_tripdata_2025-*.parquet
        s3_hook = S3Hook(aws_conn_id="microceph_s3")
        prefix = "bronze/yellow_tripdata_%s-" % year
        keys = s3_hook.list_keys(bucket_name=BUCKET, prefix=prefix) or []

        present = set()
        for key in keys:
            filename = key.rsplit("/", 1)[-1] # Eg. yellow_tripdata_2025-01.parquet
            if not filename.startswith("yellow_tripdata_") or not filename.endswith(".parquet"):
                continue
            month_str = filename[len("yellow_tripdata_"):-len(".parquet")]   # Eg. 2025-01
            present.add(month_str)

        print("[discover] year=%s, bronze has: %s" % (year, sorted(present) or "(none)"))

        for month in range(1, 13):
            month_str = "%s-%02d" % (year, month)
            if month_str in present:
                continue

            url = TAXI_URL.format(month=month_str)
            try:
                head = requests.head(url, allow_redirects=True, timeout=10)
            except requests.RequestException as exc:
                print("[discover] HEAD failed for %s: %s. Stopping." % (url, exc))
                return []

            if head.status_code == 200:
                print("[discover] next month to process: year=%s month=%s" % (year, month))
                return [{"year": year, "month": month}]

            print("[discover] %s not yet published (HTTP %s). No-op." % (month_str, head.status_code))
            return []

        print("[discover] all months for %s already processed. No-op." % year)
        return []

    # 2. BRONZE LAYER - download raw parquet
    @task
    def ingest_to_bronze(year, month):
        month_str = "%s-%02d" % (year, month)
        url = TAXI_URL.format(month=month_str)
        response = requests.get(url, timeout=120)
        response.raise_for_status()

        s3_hook = S3Hook(aws_conn_id="microceph_s3")
        s3_hook.load_file_obj(
            file_obj=BytesIO(response.content),
            key=BRONZE_KEY % month_str,
            bucket_name=BUCKET,
            replace=True,
        )
        print("[bronze] wrote %s (%.1f MB)" % (BRONZE_KEY % month_str, len(response.content) / 1e6))

    # 3. SILVER LAYER - cleaning and validation
    @task
    def transform_to_silver(year, month):
        month_str = "%s-%02d" % (year, month)
        conn = duckdb_hook().get_conn()
        try:
            create_secret(conn)
            conn.execute(
                """
                COPY (
                    WITH derived AS (
                        SELECT
                            *,
                            -- Durations & speed
                            date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0
                                AS trip_duration_minutes,
                            CASE
                                WHEN date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) > 0
                                THEN trip_distance / (date_diff('second',
                                    tpep_pickup_datetime, tpep_dropoff_datetime) / 3600.0)
                            END AS speed_mph,

                            -- Tip economics
                            CASE WHEN fare_amount > 0
                                THEN tip_amount / fare_amount * 100
                                ELSE 0 END AS tip_percent,

                            -- Readable payment type
                            CASE payment_type
                                WHEN 1 THEN 'Credit card'
                                WHEN 2 THEN 'Cash'
                                WHEN 3 THEN 'No charge'
                                WHEN 4 THEN 'Dispute'
                                WHEN 5 THEN 'Unknown'
                                WHEN 6 THEN 'Voided trip'
                                ELSE 'Other'
                            END AS payment_type_label,

                            -- Time dimensions
                            EXTRACT(dow FROM tpep_pickup_datetime) AS pickup_dow,
                            CASE WHEN EXTRACT(dow FROM tpep_pickup_datetime) IN (0, 6)
                                THEN TRUE ELSE FALSE END AS is_weekend,
                            CASE
                                WHEN EXTRACT(hour FROM tpep_pickup_datetime) BETWEEN 7 AND 9  THEN TRUE
                                WHEN EXTRACT(hour FROM tpep_pickup_datetime) BETWEEN 16 AND 19 THEN TRUE
                                ELSE FALSE
                            END AS is_rush_hour

                        FROM read_parquet('s3://%s/%s')
                    )
                    SELECT *
                    FROM derived
                    WHERE
                        -- Base validity
                        fare_amount    > 0
                        AND passenger_count > 0
                        AND trip_distance   > 0
                        AND total_amount    > 0

                        -- Physical plausibility (the real filter)
                        AND tpep_dropoff_datetime > tpep_pickup_datetime
                        AND trip_duration_minutes BETWEEN 1 AND 180
                        AND speed_mph            BETWEEN 1 AND 80

                        -- Constrain to the target month
                        AND tpep_pickup_datetime >= TIMESTAMP '%s-01'
                        AND tpep_pickup_datetime <  TIMESTAMP '%s-01' + INTERVAL 1 MONTH
                ) TO 's3://%s/%s' (FORMAT PARQUET)
                """ % (
                    BUCKET, BRONZE_KEY % month_str,
                    month_str, month_str,
                    BUCKET, SILVER_KEY % month_str,
                )
            )
            print("[silver] wrote %s" % (SILVER_KEY % month_str))
        finally:
            conn.close()

    # 4. GOLD LAYER - aggregations for reporting
    @task
    def aggregate_to_gold(year, month):
        month_str = "%s-%02d" % (year, month)
        conn = duckdb_hook().get_conn()
        try:
            create_secret(conn)
            conn.execute(
                """
                COPY (
                    SELECT
                        %d AS year,
                        %d AS month,
                        EXTRACT(hour FROM tpep_pickup_datetime) AS pickup_hour,
                        is_weekend,
                        payment_type_label,
                        COUNT(*)                          AS total_trips,
                        ROUND(AVG(fare_amount), 2)        AS avg_fare,
                        ROUND(AVG(trip_distance), 2)      AS avg_distance,
                        ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_minutes,
                        ROUND(AVG(speed_mph), 2)          AS avg_speed_mph,
                        ROUND(AVG(tip_percent), 2)        AS avg_tip_percent,
                        ROUND(SUM(total_amount), 2)       AS total_revenue
                    FROM read_parquet('s3://%s/%s')
                    GROUP BY 1, 2, 3, 4, 5
                    ORDER BY 3, 4, 5
                ) TO 's3://%s/%s' (FORMAT PARQUET)
                """ % (
                    year, month,
                    BUCKET, SILVER_KEY % month_str,
                    BUCKET, GOLD_KEY % month_str,
                )
            )
            print("[gold] wrote %s" % (GOLD_KEY % month_str))
        finally:
            conn.close()

    # Wiring
    months = discover_next_month()
    ingest = ingest_to_bronze.expand_kwargs(months)
    silver = transform_to_silver.expand_kwargs(months)
    gold   = aggregate_to_gold.expand_kwargs(months)

    ingest >> silver >> gold


# Instantiate the DAG
nyc_taxi_pipeline()
