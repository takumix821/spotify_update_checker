from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
import requests
import json
import os
from mysql import connector
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker



def refresh_access_token():
    token_file_path = '/home/ubuntu/tokens.json'
    if os.path.exists(token_file_path):
        with open(token_file_path, 'r') as token_file:
            exist_token_data = json.load(token_file)

    # 配置
    client_id = 'a75ddc29587045ac916c74e96dc22959'
    client_secret = '1ccbd3309fc74ab9b55dd3ff6ac71958'
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

    # 更新和保存新的访问令牌
    access_token = token_data['access_token']
    if 'refresh_token' in token_data:
        refresh_token = token_data['refresh_token']

    with open('/home/ubuntu/tokens.json', 'w') as token_file:
        json.dump(token_data, token_file)

# by playlist -> get tracks
def playlist_get_tracks(headers, playlist_id = 'me'):
    if playlist_id == 'me':
        # return my saved tracks
        base_url = 'https://api.spotify.com/v1/me/tracks'
    else:
        base_url = 'https://api.spotify.com/v1/playlists/' + playlist_id + '/tracks'
    
    # get json data
    next_url = base_url
    playlist_get_tracks_json = []
    while next_url:
        response = requests.get(next_url, headers = headers)
        data = response.json()
        # current items
        curr_page = data.get('items')
        playlist_get_tracks_json.extend(curr_page)
        # next page if exist
        next_url = data.get('next')
    
    # playlist_get_tracks, album_info
    playlist_get_tracks_list = []
    album_info_list = []
    if isinstance(playlist_get_tracks_json, list): 
        rank = 1
        for item in playlist_get_tracks_json:
            track_album = item.get('track').get('album')
            track = item.get('track')
            try:
                added_at_p = datetime.fromisoformat(item.get('added_at')[:-1]).replace(tzinfo = timezone.utc)
            except:
                added_at_p = np.nan
            try:
                track_album_release_date = datetime.strptime(track_album.get('release_date'), '%Y-%m-%d')
            except:
                track_album_release_date = np.nan
            
            playlist_get_tracks_data = {
                'sync_date': datetime.now(timezone.utc).replace(microsecond = 0), 
                'added_at': added_at_p, 
                'rank': rank, 
                'track_id': track.get('id'), 
                'album_id': track_album.get('id'), 
            }

            album_info_data = {
                'sync_date': datetime.now(timezone.utc).replace(microsecond = 0), 
                'album_id': track_album.get('id'), 
                'album_name': track_album.get('name'), 
                'album_release_date': track_album_release_date,  
                #'album_release_date_precision': track_album.get('release_date_precision'), 
            }

            playlist_get_tracks_list.append(playlist_get_tracks_data)
            album_info_list.append(album_info_data)
            rank += 1

    playlist_get_tracks_table = pd.DataFrame(playlist_get_tracks_list)
    album_info_table = pd.DataFrame(album_info_list)

    # track_artists
    track_artists_list = []
    if isinstance(playlist_get_tracks_json, list): 
        for item in playlist_get_tracks_json:
            track = item.get('track')
            # id
            track_id = track.get('id')

            if isinstance(track.get('artists'), list): 
                for artist in track.get('artists'):
                    # artist.id
                    track_artists_id = artist.get('id')
                    track_data = {
                        'sync_date': datetime.now(timezone.utc).replace(microsecond = 0),  
                        'track_id': track_id, 
                        'artist_id': track_artists_id, 
                    }
                    track_artists_list.append(track_data)

    track_artists_table = pd.DataFrame(track_artists_list)

    return playlist_get_tracks_table, album_info_table, track_artists_table

# get followed artists  
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
    
    following_atrists_list = []
    if isinstance(following_atrists_json, list): 
        for artist in following_atrists_json:
            following_atrists_data = {
                'sync_date': datetime.now(timezone.utc).replace(microsecond = 0), 
                'artist_id': artist.get('id'), 
                'artist_name': artist.get('name'), 
                'artist_popularity': int(artist.get('popularity')), 
                #'artist_type': artist.get('type'), 
                'artist_followers_total': int(artist.get('followers').get('total')), 
            }
            following_atrists_list.append(following_atrists_data)

    following_atrists_table = pd.DataFrame(following_atrists_list)
    return following_atrists_table

# get track info
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
                    'duration_ms': int(track.get('duration_ms')), 
                    'id': track.get('id'), 
                    'name': track.get('name'), 
                    'popularity': int(track.get('popularity')), 
                }
                track_info_list.append(track_info_data)

    track_info_table = pd.DataFrame(track_info_list)
    return track_info_table

# my_recently_played
def my_recently_played(headers):
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
    
    # all_records, album_info
    all_records_list = []
    album_info_list = []
    if isinstance(all_records, list): 
        rank = 1
        for item in all_records:
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
            
            all_records_data = {
                'sync_date': datetime.now(timezone.utc).replace(microsecond = 0), 
                'played_at': played_at_p, 
                'rank': rank, 
                'track_id': track.get('id'), 
                'album_id': track_album.get('id'), 
            }

            album_info_data = {
                'sync_date': datetime.now(timezone.utc).replace(microsecond = 0), 
                'album_id': track_album.get('id'), 
                'album_name': track_album.get('name'), 
                'album_release_date': track_album_release_date,  
                #'album_release_date_precision': track_album.get('release_date_precision'), 
            }

            all_records_list.append(all_records_data)
            album_info_list.append(album_info_data)
            rank += 1

    all_records_table = pd.DataFrame(all_records_list)
    album_info_table = pd.DataFrame(album_info_list)

    # track_artists
    track_artists_list = []
    if isinstance(all_records, list): 
        for item in all_records:
            track = item.get('track')
            # id
            track_id = track.get('id')

            if isinstance(track.get('artists'), list): 
                for artist in track.get('artists'):
                    # artist.id
                    track_artists_id = artist.get('id')
                    track_data = {
                        'sync_date': datetime.now(timezone.utc).replace(microsecond = 0),  
                        'track_id': track_id, 
                        'artist_id': track_artists_id, 
                    }
                    track_artists_list.append(track_data)

    track_artists_table = pd.DataFrame(track_artists_list)

    return all_records_table, album_info_table, track_artists_table

# get track feature
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
                    'id': track.get('id'), 
                    'danceability': float(track.get('danceability')), 
                    'energy': float(track.get('energy')), 
                    'key': int(track.get('key')), 
                    'loudness': float(track.get('loudness')), 
                    'mode': int(track.get('mode')), 
                    'speechiness': float(track.get('speechiness')), 
                    'acousticness': float(track.get('acousticness')), 
                    'instrumentalness': float(track.get('instrumentalness')), 
                    'liveness': float(track.get('liveness')), 
                    'valence': float(track.get('valence')), 
                    'tempo': float(track.get('tempo')), 
                    'type': track.get('type'), 
                    'time_signature': int(track.get('time_signature')), 
                }
                track_feature_list.append(track_feature_data)

    track_feature_table = pd.DataFrame(track_feature_list)
    return track_feature_table


# ----- refresh_access_token ----- 
def task_1(**kwargs):
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

# ----- JP top 50 ----- 
def task_2(**kwargs):
    # Create a SQLAlchemy engine to connect to the MySQL database
    engine = create_engine('mysql+mysqlconnector://admin:Vivian_0821@database-test1.c5qk2uos632g.ap-southeast-2.rds.amazonaws.com/spotify_project')

    headers = kwargs['ti'].xcom_pull(key='headers', task_ids='refresh_access_token')
    playlist_id_jptop50 = '37i9dQZEVXbKXQ4mDTEBXq'
    jptop50_table, jptop50_album_info, jptop50_track_artists = playlist_get_tracks(headers, playlist_id_jptop50)

    # 建立 Session
    Session = sessionmaker(bind = engine)
    session = Session()

    ## jptop50_table: update to DB jptop50_daily directly
    jptop50_table_insert = jptop50_table[['sync_date', 'added_at', 'rank', 'track_id', 'album_id']]
    jptop50_table_insert.to_sql('jptop50_daily', con = engine, if_exists = 'append', index = False)

    ## jptop50_album_info: update to DB album_info_update if album_id not exist
    jptop50_album_info_insert = jptop50_album_info[['sync_date', 'album_id', 'album_name', 'album_release_date']].drop_duplicates()
    # album_info_update table
    metadata = MetaData()
    metadata.reflect(bind = engine)
    album_info_db = Table('album_info_update', metadata, autoload_with = engine)
    # read db table
    query_album_info_exist = session.query(album_info_db)
    album_info_exist = pd.read_sql(query_album_info_exist.statement, query_album_info_exist.session.bind)
    # campare db & new data
    merged_album_info = pd.merge(jptop50_album_info_insert, album_info_exist, on = 'album_id', suffixes = ('_new', '_db'), how = 'outer', indicator = True)
    # update
    to_update = merged_album_info[(merged_album_info['_merge'] == 'both') & (
        (merged_album_info['album_name_new'] != merged_album_info['album_name_db']) |
        (merged_album_info['album_release_date_new'] != merged_album_info['album_release_date_db'])
    )]
    for _, row in to_update.iterrows():
        session.query(album_info_db).filter_by(album_id = row['album_id']).update({
            'album_name': row['album_name_new'],
            'album_release_date': row['album_release_date_new'],
            'sync_date': row['sync_date_new']
        })
    # insert
    to_insert = merged_album_info[merged_album_info['_merge'] == 'left_only']
    to_insert = to_insert[['sync_date_new', 'album_id', 'album_name_new', 'album_release_date_new']]
    to_insert.columns = ['sync_date', 'album_id', 'album_name', 'album_release_date']
    to_insert.to_sql('album_info_update', con = engine, if_exists = 'append', index = False)

    ## jptop50_track_artists: update to DB if new track_id
    jptop50_track_artists_insert = jptop50_track_artists[['sync_date', 'track_id', 'artist_id']]
    # track_artists_update table
    metadata = MetaData()
    metadata.reflect(bind = engine)
    track_artists_db = Table('track_artists_update', metadata, autoload_with = engine)
    # read db table
    query_track_artists_exist = session.query(track_artists_db)
    track_artists_exist = pd.read_sql(query_track_artists_exist.statement, query_track_artists_exist.session.bind)

    # insert
    to_insert = jptop50_track_artists_insert[[track not in list(track_artists_exist['track_id'].unique()) for track in jptop50_track_artists_insert['track_id']]]
    to_insert.to_sql('track_artists_update', con = engine, if_exists = 'append', index = False)

    # 提交更新
    session.commit()

    # 關閉 Session
    session.close()

# ----- Global top 50 ----- 
def task_3(**kwargs):
    # Create a SQLAlchemy engine to connect to the MySQL database
    engine = create_engine('mysql+mysqlconnector://admin:Vivian_0821@database-test1.c5qk2uos632g.ap-southeast-2.rds.amazonaws.com/spotify_project')

    headers = kwargs['ti'].xcom_pull(key='headers', task_ids='refresh_access_token')
    playlist_id_glbtop50 = '37i9dQZEVXbNG2KDcFcKOF'
    glbtop50_table, glbtop50_album_info, glbtop50_track_artists = playlist_get_tracks(headers, playlist_id_glbtop50)

    # 建立 Session
    Session = sessionmaker(bind = engine)
    session = Session()


    ## glbtop50_table: update to DB glbtop50_daily directly
    glbtop50_table_insert = glbtop50_table[['sync_date', 'added_at', 'rank', 'track_id', 'album_id']]
    glbtop50_table_insert.to_sql('glbtop50_daily', con = engine, if_exists = 'append', index = False)


    ## glbtop50_album_info: update to DB album_info_update if album_id not exist
    glbtop50_album_info_insert = glbtop50_album_info[['sync_date', 'album_id', 'album_name', 'album_release_date']].drop_duplicates()
    # album_info_update table
    metadata = MetaData()
    metadata.reflect(bind = engine)
    album_info_db = Table('album_info_update', metadata, autoload_with = engine)
    # read db table
    query_album_info_exist = session.query(album_info_db)
    album_info_exist = pd.read_sql(query_album_info_exist.statement, query_album_info_exist.session.bind)
    # campare db & new data
    merged_album_info = pd.merge(glbtop50_album_info_insert, album_info_exist, on = 'album_id', suffixes = ('_new', '_db'), how = 'outer', indicator = True)
    # update
    to_update = merged_album_info[(merged_album_info['_merge'] == 'both') & (
        (merged_album_info['album_name_new'] != merged_album_info['album_name_db']) |
        (merged_album_info['album_release_date_new'] != merged_album_info['album_release_date_db'])
    )]
    for _, row in to_update.iterrows():
        session.query(album_info_db).filter_by(album_id = row['album_id']).update({
            'album_name': row['album_name_new'],
            'album_release_date': row['album_release_date_new'],
            'sync_date': row['sync_date_new']
        })
    # insert
    to_insert = merged_album_info[merged_album_info['_merge'] == 'left_only']
    to_insert = to_insert[['sync_date_new', 'album_id', 'album_name_new', 'album_release_date_new']]
    to_insert.columns = ['sync_date', 'album_id', 'album_name', 'album_release_date']
    to_insert.to_sql('album_info_update', con = engine, if_exists = 'append', index = False)


    ## glbtop50_track_artists: update to DB if new track_id
    glbtop50_track_artists_insert = glbtop50_track_artists[['sync_date', 'track_id', 'artist_id']]
    # track_artists_update table
    metadata = MetaData()
    metadata.reflect(bind = engine)
    track_artists_db = Table('track_artists_update', metadata, autoload_with = engine)
    # read db table
    query_track_artists_exist = session.query(track_artists_db)
    track_artists_exist = pd.read_sql(query_track_artists_exist.statement, query_track_artists_exist.session.bind)

    # insert
    to_insert = glbtop50_track_artists_insert[[track not in list(track_artists_exist['track_id'].unique()) for track in glbtop50_track_artists_insert['track_id']]]
    to_insert.to_sql('track_artists_update', con = engine, if_exists = 'append', index = False)


    # 提交更新
    session.commit()

    # 關閉 Session
    session.close()

# ----- get_following_artist ----- 
def task_4(**kwargs):
    # Create a SQLAlchemy engine to connect to the MySQL database
    engine = create_engine('mysql+mysqlconnector://admin:Vivian_0821@database-test1.c5qk2uos632g.ap-southeast-2.rds.amazonaws.com/spotify_project')

    headers = kwargs['ti'].xcom_pull(key='headers', task_ids='refresh_access_token')
    following_atrists_table = get_following_artist(headers)

    # 建立 Session
    Session = sessionmaker(bind = engine)
    session = Session()

    following_atrists_table.to_sql('following_atrists_daily', con = engine, if_exists = 'append', index = False)

    # 提交更新
    session.commit()

    # 關閉 Session
    session.close()

# ----- my save tracks ----- 
def task_5(**kwargs):
    # Create a SQLAlchemy engine to connect to the MySQL database
    engine = create_engine('mysql+mysqlconnector://admin:Vivian_0821@database-test1.c5qk2uos632g.ap-southeast-2.rds.amazonaws.com/spotify_project')

    headers = kwargs['ti'].xcom_pull(key='headers', task_ids='refresh_access_token')
    mysave_table, mysave_album_info, mysave_track_artists = playlist_get_tracks(headers)

    # 建立 Session
    Session = sessionmaker(bind = engine)
    session = Session()


    ## mysave_table: update to DB mysave_daily directly
    mysave_table_insert = mysave_table[['sync_date', 'added_at', 'rank', 'track_id', 'album_id']]
    mysave_table_insert.to_sql('mysave_daily', con = engine, if_exists = 'append', index = False)

    ## mysave_album_info: update to DB album_info_update if album_id not exist
    mysave_album_info_insert = mysave_album_info[['sync_date', 'album_id', 'album_name', 'album_release_date']].drop_duplicates()
    # album_info_update table
    metadata = MetaData()
    metadata.reflect(bind = engine)
    album_info_db = Table('album_info_update', metadata, autoload_with = engine)
    # read db table
    query_album_info_exist = session.query(album_info_db)
    album_info_exist = pd.read_sql(query_album_info_exist.statement, query_album_info_exist.session.bind)
    # campare db & new data
    merged_album_info = pd.merge(mysave_album_info_insert, album_info_exist, on = 'album_id', suffixes = ('_new', '_db'), how = 'outer', indicator = True)
    # update
    to_update = merged_album_info[(merged_album_info['_merge'] == 'both') & (
        (merged_album_info['album_name_new'] != merged_album_info['album_name_db']) |
        (merged_album_info['album_release_date_new'] != merged_album_info['album_release_date_db'])
    )]
    for _, row in to_update.iterrows():
        session.query(album_info_db).filter_by(album_id = row['album_id']).update({
            'album_name': row['album_name_new'],
            'album_release_date': row['album_release_date_new'],
            'sync_date': row['sync_date_new']
        })
    # insert
    to_insert = merged_album_info[merged_album_info['_merge'] == 'left_only']
    to_insert = to_insert[['sync_date_new', 'album_id', 'album_name_new', 'album_release_date_new']]
    to_insert.columns = ['sync_date', 'album_id', 'album_name', 'album_release_date']
    to_insert.to_sql('album_info_update', con = engine, if_exists = 'append', index = False)

    ## mysave_track_artists: update to DB if new track_id
    mysave_track_artists_insert = mysave_track_artists[['sync_date', 'track_id', 'artist_id']]
    # track_artists_update table
    metadata = MetaData()
    metadata.reflect(bind = engine)
    track_artists_db = Table('track_artists_update', metadata, autoload_with = engine)
    # read db table
    query_track_artists_exist = session.query(track_artists_db)
    track_artists_exist = pd.read_sql(query_track_artists_exist.statement, query_track_artists_exist.session.bind)
    # insert
    to_insert = mysave_track_artists_insert[[track not in list(track_artists_exist['track_id'].unique()) for track in mysave_track_artists_insert['track_id']]]
    to_insert.to_sql('track_artists_update', con = engine, if_exists = 'append', index = False)


    # 提交更新
    session.commit()

    # 關閉 Session
    session.close()

# ----- my recently played ----- 
def task_6(**kwargs):
    # Create a SQLAlchemy engine to connect to the MySQL database
    engine = create_engine('mysql+mysqlconnector://admin:Vivian_0821@database-test1.c5qk2uos632g.ap-southeast-2.rds.amazonaws.com/spotify_project')

    headers = kwargs['ti'].xcom_pull(key='headers', task_ids='refresh_access_token')
    my_recently_table, my_recently_album_info, my_recently_track_artists = my_recently_played(headers)

    # 建立 Session
    Session = sessionmaker(bind = engine)
    session = Session()

    ## my_recently_table: update to DB my_recently_played if data not exist
    my_recently_table_insert = my_recently_table[['sync_date', 'played_at', 'rank', 'track_id', 'album_id']]

    # my_recently_played table
    metadata = MetaData()
    metadata.reflect(bind = engine)
    my_recently_played_db = Table('my_recently_played', metadata, autoload_with = engine)
    # read db table
    query_my_recently_played_exist = session.query(my_recently_played_db)
    my_recently_played_exist = pd.read_sql(query_my_recently_played_exist.statement, query_my_recently_played_exist.session.bind)
    # campare db & new data
    my_recently_played_exist['played_at'] = pd.to_datetime(my_recently_played_exist['played_at'], errors='coerce')
    my_recently_played_exist['played_at'] = my_recently_played_exist['played_at'].dt.floor('S').dt.tz_localize('UTC') # set data from DB as UTC timezone
    my_recently_table_insert['played_at'] = my_recently_table_insert['played_at'].dt.floor('S')
    # insert
    if len(my_recently_played_exist['played_at']) > 0:
        to_insert = my_recently_table_insert[my_recently_table_insert['played_at'] > max(my_recently_played_exist['played_at'])]
    else:
        to_insert = my_recently_table_insert
    to_insert.to_sql('my_recently_played', con = engine, if_exists = 'append', index = False)


    ## my_recently_album_info: update to DB album_info_update if album_id not exist
    my_recently_album_info_insert = my_recently_album_info[['sync_date', 'album_id', 'album_name', 'album_release_date']].drop_duplicates()
    # album_info_update table
    metadata = MetaData()
    metadata.reflect(bind = engine)
    album_info_db = Table('album_info_update', metadata, autoload_with = engine)
    # read db table
    query_album_info_exist = session.query(album_info_db)
    album_info_exist = pd.read_sql(query_album_info_exist.statement, query_album_info_exist.session.bind)
    # campare db & new data
    merged_album_info = pd.merge(my_recently_album_info_insert, album_info_exist, on = 'album_id', suffixes = ('_new', '_db'), how = 'outer', indicator = True)
    # update
    to_update = merged_album_info[(merged_album_info['_merge'] == 'both') & (
        (merged_album_info['album_name_new'] != merged_album_info['album_name_db']) |
        (merged_album_info['album_release_date_new'] != merged_album_info['album_release_date_db'])
    )]
    for _, row in to_update.iterrows():
        session.query(album_info_db).filter_by(album_id = row['album_id']).update({
            'album_name': row['album_name_new'],
            'album_release_date': row['album_release_date_new'],
            'sync_date': row['sync_date_new']
        })
    # insert
    to_insert = merged_album_info[merged_album_info['_merge'] == 'left_only']
    to_insert = to_insert[['sync_date_new', 'album_id', 'album_name_new', 'album_release_date_new']]
    to_insert.columns = ['sync_date', 'album_id', 'album_name', 'album_release_date']
    to_insert.to_sql('album_info_update', con = engine, if_exists = 'append', index = False)


    ## my_recently_track_artists: update to DB if new track_id
    my_recently_track_artists_insert = my_recently_track_artists[['sync_date', 'track_id', 'artist_id']]
    # track_artists_update table
    metadata = MetaData()
    metadata.reflect(bind = engine)
    track_artists_db = Table('track_artists_update', metadata, autoload_with = engine)
    # read db table
    query_track_artists_exist = session.query(track_artists_db)
    track_artists_exist = pd.read_sql(query_track_artists_exist.statement, query_track_artists_exist.session.bind)

    # insert
    to_insert = my_recently_track_artists_insert[[track not in list(track_artists_exist['track_id'].unique()) for track in my_recently_track_artists_insert['track_id']]]
    to_insert.to_sql('track_artists_update', con = engine, if_exists = 'append', index = False)


    # 提交更新
    session.commit()

    # 關閉 Session
    session.close()

# ----- Track Info ----- 
def task_7(**kwargs):
    # Create a SQLAlchemy engine to connect to the MySQL database
    engine = create_engine('mysql+mysqlconnector://admin:Vivian_0821@database-test1.c5qk2uos632g.ap-southeast-2.rds.amazonaws.com/spotify_project')

    headers = kwargs['ti'].xcom_pull(key='headers', task_ids='refresh_access_token')
    # 建立 Session
    Session = sessionmaker(bind = engine)
    session = Session()

    # current mysave_daily table in DB
    metadata = MetaData()
    metadata.reflect(bind = engine)
    mysave_daily = Table('mysave_daily', metadata, autoload_with = engine)
    # read db table
    mysave_daily_exist = pd.read_sql(session.query(mysave_daily).statement, session.query(mysave_daily).session.bind)

    # current mysave_daily table in DB
    metadata = MetaData()
    metadata.reflect(bind = engine)
    jptop50_daily = Table('jptop50_daily', metadata, autoload_with = engine)
    # read db table
    jptop50_daily_exist = pd.read_sql(session.query(jptop50_daily).statement, session.query(jptop50_daily).session.bind)

    # current mysave_daily table in DB
    metadata = MetaData()
    metadata.reflect(bind = engine)
    glbtop50_daily = Table('glbtop50_daily', metadata, autoload_with = engine)
    # read db table
    glbtop50_daily_exist = pd.read_sql(session.query(glbtop50_daily).statement, session.query(glbtop50_daily).session.bind)

    # current my_recently_played table in DB
    metadata = MetaData()
    metadata.reflect(bind = engine)
    my_recently_played = Table('my_recently_played', metadata, autoload_with = engine)
    # read db table
    my_recently_played_exist = pd.read_sql(session.query(my_recently_played).statement, session.query(my_recently_played).session.bind)

    # current track_info_update in DB
    metadata = MetaData()
    metadata.reflect(bind = engine)
    track_info_db = Table('track_info_update', metadata, autoload_with = engine)
    # read db table
    query_track_info_exist = session.query(track_info_db)
    track_info_exist = pd.read_sql(query_track_info_exist.statement, query_track_info_exist.session.bind)

    # ---------------------------------------------------------------------------------------------------------------- #

    # filter the track not in track_info_update table
    update_track_list = []
    for track in list(mysave_daily_exist['track_id']) + list(jptop50_daily_exist['track_id']) + list(my_recently_played_exist['track_id']) + list(glbtop50_daily_exist['track_id']):
        if track not in list(track_info_exist['track_id']):
            update_track_list.append(track)
    unique_update_track_list = list(set(update_track_list))

    # insert 
    to_insert = get_track_info(headers, unique_update_track_list, max_idn = 50)
    to_insert.columns = ['sync_date', 'track_duration_ms', 'track_id', 'track_name', 'track_popularity']
    to_insert.to_sql('track_info_update', con = engine, if_exists = 'append', index = False)


    # 提交更新
    session.commit()

    # 關閉 Session
    session.close()

# ----- Track Feature ----- 
def task_8(**kwargs):
    # Create a SQLAlchemy engine to connect to the MySQL database
    engine = create_engine('mysql+mysqlconnector://admin:Vivian_0821@database-test1.c5qk2uos632g.ap-southeast-2.rds.amazonaws.com/spotify_project')

    headers = kwargs['ti'].xcom_pull(key='headers', task_ids='refresh_access_token')
    # 建立 Session
    Session = sessionmaker(bind = engine)
    session = Session()

    # current mysave_daily table in DB
    metadata = MetaData()
    metadata.reflect(bind = engine)
    mysave_daily = Table('mysave_daily', metadata, autoload_with = engine)
    # read db table
    mysave_daily_exist = pd.read_sql(session.query(mysave_daily).statement, session.query(mysave_daily).session.bind)

    # current mysave_daily table in DB
    metadata = MetaData()
    metadata.reflect(bind = engine)
    jptop50_daily = Table('jptop50_daily', metadata, autoload_with = engine)
    # read db table
    jptop50_daily_exist = pd.read_sql(session.query(jptop50_daily).statement, session.query(jptop50_daily).session.bind)

    # current mysave_daily table in DB
    metadata = MetaData()
    metadata.reflect(bind = engine)
    glbtop50_daily = Table('glbtop50_daily', metadata, autoload_with = engine)
    # read db table
    glbtop50_daily_exist = pd.read_sql(session.query(glbtop50_daily).statement, session.query(glbtop50_daily).session.bind)

    # current my_recently_played table in DB
    metadata = MetaData()
    metadata.reflect(bind = engine)
    my_recently_played = Table('my_recently_played', metadata, autoload_with = engine)
    # read db table
    my_recently_played_exist = pd.read_sql(session.query(my_recently_played).statement, session.query(my_recently_played).session.bind)

    # current track_feature_update in DB
    metadata = MetaData()
    metadata.reflect(bind = engine)
    track_feature_db = Table('track_feature_update', metadata, autoload_with = engine)
    # read db table
    query_track_feature_exist = session.query(track_feature_db)
    track_feature_exist = pd.read_sql(query_track_feature_exist.statement, query_track_feature_exist.session.bind)

    # ---------------------------------------------------------------------------------------------------------------- #

    # filter the track not in track_feature_update table
    update_track_list = []
    for track in list(mysave_daily_exist['track_id']) + list(jptop50_daily_exist['track_id']) + list(my_recently_played_exist['track_id']) + list(glbtop50_daily_exist['track_id']):
        if track not in list(track_feature_exist['track_id']):
            update_track_list.append(track)
    unique_update_track_list = list(set(update_track_list))

    # insert 
    to_insert = get_track_feature(headers, unique_update_track_list, max_idn = 50)
    to_insert.columns = [
        'sync_date', 'track_id', 'track_danceability', 'track_energy', 'track_key', 'track_loudness', 
        'track_mode', 'track_speechiness', 'track_acousticness', 'track_instrumentalness', 'track_liveness',
        'track_valence', 'track_tempo', 'track_type', 'track_time_signature'
        ]
    to_insert.to_sql('track_feature_update', con = engine, if_exists = 'append', index = False)

    # 提交更新
    session.commit()

    # 關閉 Session
    session.close()



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'spotify_data_pipeline',
    default_args = default_args,
    description = 'A simple data pipeline for spotify data',
    schedule_interval = '@daily',
    start_date = datetime.now() - timedelta(days=1),
    catchup = False,
)

task1 = PythonOperator(
    task_id = 'refresh_access_token',
    python_callable = task_1,
    provide_context = True, 
    dag = dag,
)

task2 = PythonOperator(
    task_id = 'jptop50',
    python_callable = task_2,
    provide_context = True, 
    dag = dag,
)

task3 = PythonOperator(
    task_id = 'glbtop50',
    python_callable = task_3,
    provide_context = True, 
    dag = dag,
)

task4 = PythonOperator(
    task_id = 'following_atrists',
    python_callable = task_4,
    provide_context = True, 
    dag = dag,
)

task5 = PythonOperator(
    task_id = 'my_save_tracks',
    python_callable = task_5,
    provide_context = True, 
    dag = dag,
)

task6 = PythonOperator(
    task_id = 'my_recently_played',
    python_callable = task_6,
    provide_context = True, 
    dag = dag,
)

task7 = PythonOperator(
    task_id = 'track_info',
    python_callable = task_7,
    provide_context = True, 
    dag = dag,
)

task8 = PythonOperator(
    task_id = 'track_feature',
    python_callable = task_8,
    provide_context = True, 
    dag = dag,
)

task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7 >> task8

task2.trigger_rule = 'all_done'
task3.trigger_rule = 'all_done'
task4.trigger_rule = 'all_done'
task5.trigger_rule = 'all_done'
task6.trigger_rule = 'all_done'
task7.trigger_rule = 'all_done'
task8.trigger_rule = 'all_done'

