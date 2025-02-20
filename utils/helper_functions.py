import requests
import json
import os
import boto3


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

def read_aws_keys(file_path):
    aws_access_keys = {}
    with open(file_path, "r") as f:
        for line in f:
            key, value = line.strip().split(" = ")
            aws_access_keys[key] = value
    return aws_access_keys

def upload_s3(bucket_name, s3_key, upload_json):
    # 初始化 S3 client
    aws_access_keys = read_aws_keys("/home/ubuntu/aws_access_key.txt") # read key file
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_keys["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=aws_access_keys["AWS_SECRET_ACCESS_KEY"],
        region_name=aws_access_keys["AWS_DEFAULT_REGION"]
    )

    json_data = json.dumps(upload_json, indent = 4)
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json_data,
        ContentType="application/json"
    )
    
    print("File {s3_key} upload successfully!")