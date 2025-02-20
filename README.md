# spotify_update_checker - Spotify Data Pipeline

## Introduction
This project aims to leverage Spotify playlists data to calculate track similarity based on textual and musical attributes, using personal playlists to find globally popular music with high similarity. 
Spotify playlists data is collected by using Airflow and SQLAlchemy to create a data pipeline that fetches, updating data from user's Spotify playlists and storing it in a MySQL database. 

## Prerequisites
- Python 3.x
- Apache Airflow
- SQLAlchemy
- MySQL database
- Spotify API credentials

## Installation

1. **Clone the repository:**
    ```bash
    git clone https://github.com/takumix821/spotify_update_checker.git
    cd spotify_update_checker
    ```

2. **Install required Python packages:**

3. **Set up Airflow:**
    Place `test_run_spotify_dag.py` in Airflow's `dags` folder and update the configuration as needed.

4. **Configure MySQL database:**
    Ensure your MySQL database is running and update the connection string in the code.

5. **Spotify API credentials:**
    Obtain and configure your Spotify API credentials.

## Usage

### Start Airflow
1. Start the Airflow scheduler:
    ```bash
    airflow scheduler
    ```

2. Start the Airflow web server:
    ```bash
    airflow webserver
    ```

3. Access the Airflow UI at `http://localhost:8080` and trigger the `spotify_data_pipeline` DAG.

## DAG Tasks Overview

1. **Refresh Access Token**
2. **Fetch and Update Japan Top 50 Playlist**
3. **Fetch and Update Global Top 50 Playlist**
4. **Fetch and Update Followed Artists**
5. **Fetch and Update Saved Tracks**
6. **Fetch and Update Recently Played Tracks**
7. **Update Track Information**
8. **Update Track Features**

The Jupyter notebook `text_embedding_test.ipynb` in `test` folder is a sample of calculating track similarity for finding globally popular music with high similarity. 

