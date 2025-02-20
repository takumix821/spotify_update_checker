from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os
from utils.helper_functions import refresh_access_token
from utils.helper_functions import get_following_artist
from utils.helper_functions import upload_s3

### ==================== TASKS ==================== 

# ----- refresh access token ----- 
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

# ----- get following artist ----- 
def following_artist_task(**kwargs):
    headers = kwargs['ti'].xcom_pull(key='headers', task_ids='refresh_access_token')
    following_atrists_json = get_following_artist(headers)

    # S3 設定
    timestamp = datetime.now().strftime("%Y%m%d%H") # current time in YYYYMMDDHH
    json_filename = f"following_atrists_{timestamp}.json" 
    bucket_name = "spotify-download-bucket"
    s3_key = f"following_atrists/{json_filename}"

    upload_s3(bucket_name, s3_key, following_atrists_json)


### ==================== DAG ==================== 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

daily_download_dag = DAG(
    'spotify_data_pipeline_daily_download',
    default_args = default_args,
    description = 'daily_download',
    schedule_interval = '0 5 * * *',
    start_date = datetime.now() - timedelta(days=1),
    catchup = False,
)

refresh_access_token_task = PythonOperator(
    task_id = 'refresh_access_token',
    python_callable = refresh_access_token_task,
    provide_context = True, 
    dag = daily_download_dag,
)

following_artist_task = PythonOperator(
    task_id = 'following_atrists', 
    python_callable = following_artist_task, 
    provide_context = True, 
    dag = daily_download_dag, 
)

# Task Order
refresh_access_token_task >> following_artist_task