from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from utils.helper_functions import list_s3_files
from utils.helper_functions import download_s3
from utils.helper_functions import read_access_keys_txt
from utils.helper_functions import process_recently_played
from utils.helper_functions import upload_recently_played
from utils.helper_functions import process_following_atrists
from utils.helper_functions import upload_following_atrists


### ==================== TASKS ==================== 

# ----- refresh recently played ----- 
def refresh_recently_played_task():
    ### 1. create engine
    db_access_key = read_access_keys_txt("/home/ubuntu/db_access_key.txt")
    db = 'spotify_project'
    user = db_access_key['user']
    password = db_access_key['password']
    ip = db_access_key['ip']
    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{ip}/{db}')

    ### 2. download s3 data for today and yesterday
    bucket_name = 'spotify-download-bucket'
    today = datetime.now().strftime('%Y%m%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    dates = [yesterday, today]
    # list all recently_played s3 files
    all_recently_played_keys = []
    for date in dates:
        prefix = f"recently_played/recently_played_{date}"
        all_recently_played_keys.extend(list_s3_files(bucket_name, prefix))
    # process & upload by file
    for s3_key in all_recently_played_keys:
        recently_played = download_s3(bucket_name, s3_key)
        recently_played_table = process_recently_played(recently_played)
        upload_recently_played(recently_played_table, engine)

# ----- refresh following atrists ----- 
def refresh_following_atrists_task():
    ### 1. create engine
    db_access_key = read_access_keys_txt("/home/ubuntu/db_access_key.txt")
    db = 'spotify_project'
    user = db_access_key['user']
    password = db_access_key['password']
    ip = db_access_key['ip']
    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{ip}/{db}')

    ### 2. download s3 data for today and yesterday
    bucket_name = 'spotify-download-bucket'
    today = datetime.now().strftime('%Y%m%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    dates = [yesterday, today]
    # list all following_atrists s3 files
    all_following_atrists_keys = []
    for date in dates:
        prefix = f"following_atrists/following_atrists_{date}"
        all_following_atrists_keys.extend(list_s3_files(bucket_name, prefix))
    # process & upload by file
    for s3_key in all_following_atrists_keys:
        following_atrists = download_s3(bucket_name, s3_key)
        following_atrists_table = process_following_atrists(following_atrists)
        upload_following_atrists(following_atrists_table, engine)


### ==================== DAG ==================== 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

daily_refresh_dag = DAG(
    'spotify_data_pipeline_daily_refresh',
    default_args = default_args,
    description = 'daily_refresh_DB_track_id',
    schedule_interval = '0 6 * * *',
    start_date = datetime.now() - timedelta(days=1),
    catchup = False,
)

refresh_recently_played_task = PythonOperator(
    task_id = 'refresh_recently_played',
    python_callable = refresh_recently_played_task,
    provide_context = True, 
    dag = daily_refresh_dag,
)

refresh_following_atrists_task = PythonOperator(
    task_id = 'refresh_following_atrists', 
    python_callable = refresh_following_atrists_task, 
    provide_context = True, 
    dag = daily_refresh_dag, 
)

# Task Order
[refresh_recently_played_task, refresh_following_atrists_task]