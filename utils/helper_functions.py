import requests
import json
import os
import boto3
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker


### ==================== Predefined Functions ==================== 

def read_spotify_client_info(file_path):
    client_info = {}
    with open(file_path, "r") as f:
        for line in f:
            key, value = line.strip().split(" = ")
            client_info[key] = value
    return client_info

def refresh_access_token():
    token_file_path = '/home/ubuntu/tokens.json'
    if os.path.exists(token_file_path):
        with open(token_file_path, 'r') as token_file:
            exist_token_data = json.load(token_file)

    # 配置
    spotify_client_info = read_spotify_client_info('/home/ubuntu/spotify_client_info.txt')
    client_id = spotify_client_info['CLIENT_ID']
    client_secret = spotify_client_info['CLIENT_SECRET']
    refresh_token = exist_token_data['refresh_token']
    token_endpoint = 'https://accounts.spotify.com/api/token'

    token_params = {
        'grant_type': 'refresh_token', 
        'refresh_token': refresh_token, 
        'client_id': client_id, 
        'client_secret': client_secret 
    }
    response = requests.post(token_endpoint, data = token_params)
    token_data = response.json()

    # 更新和保存新的token
    if 'refresh_token' in token_data:
        refresh_token = token_data['refresh_token']

    with open('/home/ubuntu/tokens.json', 'w') as token_file:
        json.dump(token_data, token_file)

def get_recently_played(headers):
    base_url = 'https://api.spotify.com/v1/me/player/recently-played'
    # get json data
    all_records = []
    next_url = base_url
    while next_url:
        response = requests.get(next_url, headers = headers)
        data = response.json()
        records_on_page = data.get('items', {})
        all_records.extend(records_on_page)
        next_url = data.get('next')
    
    return all_records

def get_following_artist(headers):
    base_url = 'https://api.spotify.com/v1/me/following?type=artist'
    next_url = base_url

    following_atrists_json = []
    while next_url:
        response = requests.get(next_url, headers = headers)
        data = response.json()

        curr_page = data.get('artists', {}).get('items')
        following_atrists_json.extend(curr_page)

        next_url = data.get('artists', {}).get('next')

    return following_atrists_json

def read_access_keys_txt(file_path):
    access_keys = {}
    with open(file_path, "r") as f:
        for line in f:
            key, value = line.strip().split(" = ")
            access_keys[key] = value
    return access_keys

def upload_s3(bucket_name, s3_key, upload_json):
    # 初始化 S3 client
    aws_access_keys = read_access_keys_txt("/home/ubuntu/aws_access_key.txt") # read key file
    s3_client = boto3.client(
        "s3",
        aws_access_key_id = aws_access_keys["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key = aws_access_keys["AWS_SECRET_ACCESS_KEY"],
        region_name = aws_access_keys["AWS_DEFAULT_REGION"]
    )

    json_data = json.dumps(upload_json, indent = 4)
    s3_client.put_object(
        Bucket = bucket_name,
        Key = s3_key,
        Body = json_data,
        ContentType = "application/json"
    )
    
    print("File {s3_key} upload successfully!")

def list_s3_files(bucket_name, prefix):
    aws_access_keys = read_access_keys_txt("/home/ubuntu/aws_access_key.txt")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_keys["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=aws_access_keys["AWS_SECRET_ACCESS_KEY"],
        region_name=aws_access_keys["AWS_DEFAULT_REGION"]
    )
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        return [obj['Key'] for obj in response.get('Contents', [])]
    except Exception as e:
        print(f"Error listing files: {e}")
        return []

def download_s3(bucket_name, s3_key):
    aws_access_keys = read_access_keys_txt("/home/ubuntu/aws_access_key.txt") # read key file
    s3_client = boto3.client(
        "s3",
        aws_access_key_id = aws_access_keys["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key = aws_access_keys["AWS_SECRET_ACCESS_KEY"],
        region_name = aws_access_keys["AWS_DEFAULT_REGION"]
    )
    try:
        obj = s3_client.get_object(
            Bucket = bucket_name, 
            Key = s3_key
        )
        data = obj['Body'].read().decode('utf-8')
        return json.loads(data)
    except s3_client.exceptions.NoSuchKey:
        print(f"Warning: {s3_key} not found in bucket {bucket_name}")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None

# process_recently_played
def process_recently_played(recently_played):
    recently_played_list = []
    if isinstance(recently_played, list): 
        rank = 1
        for item in recently_played:
            track_album = item.get('track').get('album')
            track = item.get('track')
            try:
                played_at_p = datetime.fromisoformat(item.get('played_at')[:-1]).replace(tzinfo = timezone.utc)
            except:
                played_at_p = np.nan
            try:
                track_album_release_date = datetime.strptime(track_album.get('release_date'), '%Y-%m-%d')
            except:
                track_album_release_date = np.nan
            
            recently_played_data = {
                'sync_date': datetime.now(timezone.utc).replace(microsecond = 0), 
                'played_at': played_at_p, 
                'rank': rank, 
                'track_id': track.get('id'), 
                'album_id': track_album.get('id'), 
            }

            recently_played_list.append(recently_played_data)
            rank += 1
    
    recently_played_table = pd.DataFrame(recently_played_list)
    return recently_played_table

def upload_recently_played(recently_played_table, engine):
    Session = sessionmaker(bind = engine)
    session = Session()
    try:
        recently_played_insert = recently_played_table[['sync_date', 'played_at', 'rank', 'track_id', 'album_id']]
        
        # read data in db
        metadata = MetaData()
        metadata.reflect(bind = engine)
        recently_played_db = Table('recently_played', metadata, autoload_with=engine)
        query = session.query(recently_played_db.c.played_at, recently_played_db.c.track_id)
        recently_played_exist = pd.read_sql(query.statement, query.session.bind)

        # unify as UTC timezone in second
        recently_played_exist['played_at'] = recently_played_exist['played_at'].dt.floor('S').dt.tz_localize('UTC')
        recently_played_insert['played_at'] = recently_played_insert['played_at'].dt.floor('S')

        # check exist or not by played_at & track_id
        to_insert = recently_played_insert[~recently_played_insert[['played_at', 'track_id']].apply(
            lambda row: ((recently_played_exist['played_at'] == row['played_at']) & 
                        (recently_played_exist['track_id'] == row['track_id'])).any(), axis = 1
        )]

        # new data into db
        if not to_insert.empty:
            to_insert.to_sql('recently_played', con = engine, if_exists = 'append', index = False)
        session.commit()
    except Exception as e:
        print(f"Error: {e}")
    
    session.close()

def process_following_atrists(following_atrists):
    following_atrists_list = []
    if isinstance(following_atrists, list): 
        for artist in following_atrists:
            following_atrists_data = {
                'sync_date': datetime.now(timezone.utc).replace(microsecond = 0), 
                'artist_id': artist.get('id'), 
                'artist_name': artist.get('name'), 
                'artist_popularity': int(artist.get('popularity')), 
                'artist_followers_total': int(artist.get('followers').get('total')), 
            }
            following_atrists_list.append(following_atrists_data)

    following_atrists_table = pd.DataFrame(following_atrists_list)
    return following_atrists_table

def upload_following_atrists(following_atrists_table, engine):
    Session = sessionmaker(bind = engine)
    session = Session()
    try:
        following_atrists_table.to_sql('following_atrists', con = engine, if_exists = 'replace', index = False)
        session.commit()
    except Exception as e:
        print(f"Error: {e}")
    
    session.close()