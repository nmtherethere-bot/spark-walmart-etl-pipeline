from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="walmart_spark_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["spark", "bigquery", "walmart"],
) as dag:

    input_path = os.getenv("WALMART_INPUT_PATH", "/path/to/walmart_sales.csv")
    bq_project = os.getenv("BQ_PROJECT", "your-gcp-project")
    bq_dataset = os.getenv("BQ_DATASET", "retail")
    bq_table = os.getenv("BQ_TABLE", "walmart_sales")
    temp_bucket = os.getenv("BQ_TEMP_BUCKET", "your-temp-gcs-bucket")
    date_format = os.getenv("DATE_FORMAT", "yyyy-MM-dd")

    spark_submit = BashOperator(
        task_id="run_spark_etl",
        bash_command=(
            "spark-submit "
            "--class com.example.walmart.etl.ETLJob "
            "--packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.38.0 "
            "{{ var.value.jar_path }} "
            f"--inputPath={input_path} "
            f"--bqProject={bq_project} "
            f"--bqDataset={bq_dataset} "
            f"--bqTable={bq_table} "
            f"--tempGcsBucket={temp_bucket} "
            f"--dateFormat={date_format} "
            "--mode=append"
        ),
        env={
            "GOOGLE_APPLICATION_CREDENTIALS": "{{ var.value.google_credentials_json }}",
        },
    )

    spark_submit
