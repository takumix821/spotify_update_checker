from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os
from utils.helper_functions import refresh_access_token
from utils.helper_functions import get_recently_played
from utils.helper_functions import upload_s3


### ==================== TASKS ==================== 

def refresh_access_token_task(**kwargs):
    refresh_access_token()

    token_file_path = '/home/ubuntu/tokens.json'
    if os.path.exists(token_file_path):
        with open(token_file_path, 'r') as token_file:
            exist_token_data = json.load(token_file)
    
    access_token = exist_token_data['access_token']
    
    headers = {
        'Authorization': 'Bearer  ' + access_token
    }
    
    kwargs['ti'].xcom_push(key='headers', value = headers)

def recently_played_task(**kwargs):
    headers = kwargs['ti'].xcom_pull(key='headers', task_ids='refresh_access_token')
    recently_played_json = get_recently_played(headers)
    
    # S3 設定
    timestamp = datetime.now().strftime("%Y%m%d%H") # current time in YYYYMMDDHH
    json_filename = f"recently_played_{timestamp}.json" # file name
    bucket_name = "spotify-download-bucket"
    s3_key = f"recently_played/{json_filename}"

    upload_s3(bucket_name, s3_key, recently_played_json)


### ==================== DAG ==================== 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

bihourly_download_dag = DAG(
    'spotify_data_pipeline_bihourly_download',
    default_args = default_args,
    description = 'bihourly_download',
    schedule_interval = '10 */2 * * *',
    start_date = datetime.now() - timedelta(days=1),
    catchup = False,
)

refresh_access_token_task = PythonOperator(
    task_id = 'refresh_access_token',
    python_callable = refresh_access_token_task,
    provide_context = True, 
    dag = bihourly_download_dag,
)

recently_played_task = PythonOperator(
    task_id = 'following_atrists',
    python_callable = recently_played_task,
    provide_context = True, 
    dag = bihourly_download_dag,
)

refresh_access_token_task >> recently_played_task