import requests
import json
import os
import boto3
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from sqlalchemy import create_engine, Table, MetaData, text
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

def get_tracks_to_update(engine, table_name):
    Session = sessionmaker(bind=engine)
    session = Session()
    # check if exist in other table or not
    try:
        if table_name in ["track_artists", "track_feature", "track_info"]:
            # all track_id in recently_played table
            all_tracks = {row[0] for row in session.execute(text("SELECT track_id FROM recently_played"))}
            exist_tracks = {row[0] for row in session.execute(text(f"SELECT track_id FROM {table_name}"))}
            tracks_to_update = list(all_tracks - exist_tracks)
        session.close()

        return tracks_to_update
    except Exception as e:
        print(f"Error: {e}. Return empty list!")
        return []

def get_track_artists(headers, track_list, max_idn = 100):
    track_artists_list = []
    for i in range(len(track_list)//max_idn + 1):
        sub_track_list = track_list[i*max_idn: i*max_idn+max_idn]
        base_url = 'https://api.spotify.com/v1/tracks?ids=' + ','.join(sub_track_list)
        response = requests.get(base_url, headers = headers)
        data = response.json()
        track_info_json = data.get('tracks')

        if isinstance(track_info_json, list): 
            for track in track_info_json:
                for artist in track.get('artists'):
                    track_artists_data = {
                        'sync_date': datetime.now(timezone.utc).replace(microsecond = 0), 
                        'track_id': track.get('id'), 
                        'artist_id': artist.get('id'), 
                    }
                    track_artists_list.append(track_artists_data)

    track_artists_table = pd.DataFrame(track_artists_list)
    return track_artists_table

def get_track_info(headers, track_list, max_idn = 100):
    track_info_list = []
    for i in range(len(track_list)//max_idn + 1):
        sub_track_list = track_list[i*max_idn: i*max_idn+max_idn]
        base_url = 'https://api.spotify.com/v1/tracks?ids=' + ','.join(sub_track_list)
        response = requests.get(base_url, headers = headers)
        data = response.json()
        track_info_json = data.get('tracks')

        if isinstance(track_info_json, list): 
            for track in track_info_json:
                track_info_data = {
                    'sync_date': datetime.now(timezone.utc).replace(microsecond = 0), 
                    'track_duration_ms': int(track.get('duration_ms')), 
                    'track_id': track.get('id'), 
                    'track_name': track.get('name'), 
                }
                track_info_list.append(track_info_data)

    track_info_table = pd.DataFrame(track_info_list)
    return track_info_table

def get_track_feature(headers, track_list, max_idn = 100):
    track_feature_list = []
    for i in range(len(track_list)//max_idn + 1):
        sub_track_list = track_list[i*max_idn: i*max_idn+max_idn]
        base_url = 'https://api.spotify.com/v1/audio-features?ids=' + ','.join(sub_track_list)
        response = requests.get(base_url, headers = headers)
        data = response.json()
        track_feature_json = data.get('audio_features')

        if isinstance(track_feature_json, list): 
            for track in track_feature_json:
                track_feature_data = {
                    'sync_date': datetime.now(timezone.utc).replace(microsecond = 0), 
                    'track_id': track.get('id'), 
                    'track_danceability': float(track.get('danceability')), 
                    'track_energy': float(track.get('energy')), 
                    'track_key': int(track.get('key')), 
                    'track_loudness': float(track.get('loudness')), 
                    'track_mode': int(track.get('mode')), 
                    'track_speechiness': float(track.get('speechiness')), 
                    'track_acousticness': float(track.get('acousticness')), 
                    'track_instrumentalness': float(track.get('instrumentalness')), 
                    'track_liveness': float(track.get('liveness')), 
                    'track_valence': float(track.get('valence')), 
                    'track_tempo': float(track.get('tempo')), 
                    'track_type': track.get('type'), 
                    'track_time_signature': int(track.get('time_signature')), 
                }
                track_feature_list.append(track_feature_data)

    track_feature_table = pd.DataFrame(track_feature_list)
    return track_feature_table

def upload_track_artists(track_artists_table, engine):
    Session = sessionmaker(bind = engine)
    session = Session()
    try:
        track_artists_insert = track_artists_table[['sync_date', 'track_id', 'artist_id']].drop_duplicates()
        metadata = MetaData()
        metadata.reflect(bind = engine)
        track_artists_db = Table('track_artists', metadata, autoload_with = engine)
        # read db table
        query = session.query(track_artists_db)
        track_artists_exist = pd.read_sql(query.statement, query.session.bind)
        # insert
        to_insert = track_artists_insert[[track not in list(track_artists_exist['track_id'].unique()) for track in track_artist_insert['track_id']]]
        to_insert.to_sql('track_artists', con = engine, if_exists = 'append', index = False)
    except Exception as e:
        print(f"Error: {e}")
    
    session.close()

def upload_track_info(track_info_table, engine):
    Session = sessionmaker(bind = engine)
    session = Session()
    try:
        track_info_insert = track_info_table[['sync_date', 'track_duration_ms', 'track_id', 'track_name']].drop_duplicates()
        metadata = MetaData()
        metadata.reflect(bind = engine)
        track_info_db = Table('track_info', metadata, autoload_with = engine)
        # read db table
        query = session.query(track_info_db)
        track_info_exist = pd.read_sql(query.statement, query.session.bind)
        # insert
        to_insert = track_info_insert[[track not in list(track_info_exist['track_id'].unique()) for track in track_info_insert['track_id']]]
        to_insert.to_sql('track_info', con = engine, if_exists = 'append', index = False)
    except Exception as e:
        print(f"Error: {e}")

    session.close()

def upload_track_feature(track_feature_table, engine):
    Session = sessionmaker(bind = engine)
    session = Session()
    try:
        track_feature_insert = track_feature_table[[
            'sync_date', 'track_id', 'track_danceability', 'track_energy', 'track_key', 'track_loudness', 
            'track_mode', 'track_speechiness', 'track_acousticness', 'track_instrumentalness', 'track_liveness',
            'track_valence', 'track_tempo', 'track_type', 'track_time_signature'
        ]].drop_duplicates()
        metadata = MetaData()
        metadata.reflect(bind = engine)
        track_feature_db = Table('track_feature', metadata, autoload_with = engine)
        # read db table
        query = session.query(track_feature_db)
        track_feature_exist = pd.read_sql(query.statement, query.session.bind)
        # insert
        to_insert = track_feature_insert[[track not in list(track_feature_exist['track_id'].unique()) for track in track_feature_insert['track_id']]]
        to_insert.to_sql('track_feature', con = engine, if_exists = 'append', index = False)
    except Exception as e:
        print(f"Error: {e}")

    session.close()