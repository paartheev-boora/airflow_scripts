from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
 
def process_csv():
    df = pd.read_csv("/tmp/order.csv", engine="python")
    print(df.head())
    df["total"] = df["Quantity"] * df["Price"]
    df.to_csv(".csv", index=False)
 
with DAG(
    dag_id="etl_csv_process",
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",  # every hour
    catchup=False,
) as dag:
    download = BashOperator(
        task_id="download_csv",
        bash_command=(
            "curl -L -o /tmp/order.csv "
            "https://raw.githubusercontent.com/paartheev-boora/airflow_scripts/main/order.csv"
        )
    )
 
    clean = PythonOperator(task_id="process_csv", python_callable=process_csv)
 
    upload = BashOperator(
        task_id="upload_output",
        bash_command="echo 'Uploading file...' && sleep 5",
    )
 
    download >> clean >> upload
 
 