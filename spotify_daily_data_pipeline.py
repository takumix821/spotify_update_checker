from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import json
import os
from utils.helper_functions import list_s3_files
from utils.helper_functions import download_s3
from utils.helper_functions import read_access_keys_txt
from utils.helper_functions import process_recently_played
from utils.helper_functions import upload_recently_played
from utils.helper_functions import process_following_atrists
from utils.helper_functions import upload_following_atrists

from utils.helper_functions import refresh_access_token
from utils.helper_functions import get_tracks_to_update
from utils.helper_functions import get_track_artists
from utils.helper_functions import upload_track_artists
from utils.helper_functions import get_track_info
from utils.helper_functions import upload_track_info
from utils.helper_functions import get_track_feature
from utils.helper_functions import upload_track_feature


### ==================== TASKS ==================== 

# ----- refresh recently played ----- 
def refresh_recently_played_task():
    # create engine
    db_access_key = read_access_keys_txt("/home/ubuntu/db_access_key.txt")
    user, password, ip = db_access_key['user'], db_access_key['password'], db_access_key['ip']
    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{ip}/spotify_project')

    # download s3 data for today and yesterday
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
    # create engine
    db_access_key = read_access_keys_txt("/home/ubuntu/db_access_key.txt")
    user, password, ip = db_access_key['user'], db_access_key['password'], db_access_key['ip']
    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{ip}/spotify_project')

    # download s3 data for today and yesterday
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

# ----- update track artists ----- 
def update_track_artists_task(**kwargs):
    # create engine
    db_access_key = read_access_keys_txt("/home/ubuntu/db_access_key.txt")
    user, password, ip = db_access_key['user'], db_access_key['password'], db_access_key['ip']
    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{ip}/spotify_project')
    # tracks to update
    tracks_to_update = get_tracks_to_update(engine, "track_artists")
    # get track artists
    headers = kwargs['ti'].xcom_pull(key='headers', task_ids='refresh_access_token')
    track_artists_table = get_track_artists(headers, tracks_to_update, max_idn = 100)
    # upload track artists
    upload_track_artists(track_artists_table, engine)

# ----- update track info ----- 
def update_track_info_task(**kwargs):
    # create engine
    db_access_key = read_access_keys_txt("/home/ubuntu/db_access_key.txt")
    user, password, ip = db_access_key['user'], db_access_key['password'], db_access_key['ip']
    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{ip}/spotify_project')
    # tracks to update
    tracks_to_update = get_tracks_to_update(engine, "track_info")
    # get track info
    headers = kwargs['ti'].xcom_pull(key='headers', task_ids='refresh_access_token')
    track_info_table = get_track_info(headers, tracks_to_update, max_idn = 100)
    # upload track info
    upload_track_info(track_info_table, engine)

# ----- update track feature ----- 
def update_track_feature_task(**kwargs):
    # create engine
    db_access_key = read_access_keys_txt("/home/ubuntu/db_access_key.txt")
    user, password, ip = db_access_key['user'], db_access_key['password'], db_access_key['ip']
    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{ip}/spotify_project')
    # tracks to update
    tracks_to_update = get_tracks_to_update(engine, "track_feature")
    # get track feature
    headers = kwargs['ti'].xcom_pull(key='headers', task_ids='refresh_access_token')
    track_feature_table = get_track_feature(headers, tracks_to_update, max_idn = 100)
    # upload track feature
    upload_track_feature(track_feature_table, engine)


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

refresh_access_token_task = PythonOperator(
    task_id = 'refresh_access_token', 
    python_callable = refresh_access_token_task, 
    provide_context = True, 
    dag = daily_refresh_dag, 
    trigger_rule = TriggerRule.ALL_DONE
)

update_track_artists_task = PythonOperator(
    task_id = 'update_track_artists', 
    python_callable = update_track_artists_task, 
    provide_context = True, 
    dag = daily_refresh_dag, 
)

update_track_info_task = PythonOperator(
    task_id = 'update_track_info_task', 
    python_callable = update_track_info_task, 
    provide_context = True, 
    dag = daily_refresh_dag, 
)

update_track_feature_task = PythonOperator(
    task_id = 'update_track_feature_task', 
    python_callable = update_track_feature_task, 
    provide_context = True, 
    dag = daily_refresh_dag, 
)


# Task Order
[
    refresh_recently_played_task, 
    refresh_following_atrists_task
] >> refresh_access_token_task >> [
    update_track_artists_task, 
    update_track_info_task, 
    update_track_feature_task
]