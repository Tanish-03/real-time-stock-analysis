import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# --- CONFIG ---
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET = "bronze-transactions"
LOCAL_DIR = "/tmp/minio_downloads"

SNOWFLAKE_USER = "USERNAME"
SNOWFLAKE_PASSWORD = "PASSWORD"
SNOWFLAKE_ACCOUNT = "ACCOUNT"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DB = "STOCKS_MDS"
SNOWFLAKE_SCHEMA = "COMMON"
SNOWFLAKE_TABLE = "BRONZE_STOCKS_QUOTES_RAW"   # 


def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    objects = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    print("DEBUG - Objects in bucket:", objects)

    local_files = []
    for obj in objects:
        key = obj["Key"]
        local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
        s3.download_file(BUCKET, key, local_file)
        print(f"Downloaded {key} -> {local_file}")
        local_files.append(local_file)

    print("DEBUG - Local files returned:", local_files)
    return local_files


def load_to_snowflake(**kwargs):
    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio")
    if not local_files:
        print("No files to load.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()

    try:
        cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
        cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

        for f in local_files:
            
    
            cur.execute(f"PUT file://{f} @%bronze_stocks_quotes_raw")
            print(f"Uploaded {f} to Snowflake stage")
            #f = f.replace("\\", "/")  # normalize Windows paths
            #cur.execute(f"PUT file://{f} @%{SNOWFLAKE_TABLE} OVERWRITE=TRUE")
            print(f"📤 Uploaded {f} to Snowflake stage")

        cur.execute(
            f"""
            COPY INTO {SNOWFLAKE_TABLE} (v)
            FROM @%{SNOWFLAKE_TABLE}
            FILE_FORMAT = (TYPE=JSON STRIP_OUTER_ARRAY=TRUE);
            """
        )
        print("COPY INTO executed successfully")
    finally:
        cur.close()
        conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "minio_to_snowflake",
    default_args=default_args,
    schedule="*/1 * * * *",  # every 1 minute
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
    )

    task1 >> task2
