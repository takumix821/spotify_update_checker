# spotify_update_checker - Spotify Data Pipeline

# 🎵 Spotify Data Pipeline

## 📌 專案簡介
這個專案的目的是記錄使用者在 Spotify 上的音樂聆聽行為，並作為未來推薦歌曲的參考依據。包含以下功能：
1. **記錄最近播放的歌曲**：由於 Spotify API 限制每次最多只能獲取 50 首歌曲，因此需要頻繁串接 API 來記錄完整的聆聽歷史。
2. **每日記錄使用者關注的演奏者**：確保最新的演奏者清單同步到資料庫。
3. **歌曲資訊記錄與去重**：歌曲資訊（如曲目特性、發表時間、演奏者等）不會立即儲存，而是等待累積足夠數量後一次性去重儲存。

---

## 🔄 **資料流架構**
專案使用 **Apache Airflow** 來管理資料管道，並透過 **Spotify API** 獲取數據，最後將處理後的數據存入 **AWS S3 與 MySQL**。資料流如下：

### **📌 每兩小時執行 (`spotify_bihourly_download_dag.py`)**
1. **刷新 Spotify Access Token** → 透過 `refresh_access_token()` 獲取新的存取權杖。
2. **取得最近播放的歌曲 (`get_recently_played()`)**  
   - 取得後的 JSON 檔案會上傳至 **AWS S3** (`recently_played/YYYYMMDDHH.json`)。

---

### **📌 每日執行 (`spotify_daily_download_dag.py`)**
1. **刷新 Spotify Access Token**
2. **取得使用者關注的演奏者 (`get_following_artist()`)**  
   - 獲取完整的追蹤清單，並上傳至 **AWS S3** (`following_artists/YYYYMMDDHH.json`)。

---

### **📌 每日數據處理 (`spotify_daily_data_pipeline.py`)**
1. **處理最近播放的歌曲 (`process_recently_played()`)**
   - 讀取 `recently_played/YYYYMMDDHH.json`，解析後存入 **MySQL**。
   - 確保不重複存入相同的歌曲紀錄。
2. **處理使用者關注的演奏者 (`process_following_artists()`)**
   - 讀取 `following_artists/YYYYMMDDHH.json`，解析後存入 **MySQL**。

---

## ⚙️ **使用方式**

### 1️⃣ 安裝依賴套件
```bash
pip install -r requirements.txt
```

### 2️⃣ 設置 DAG 檔案
請將以下檔案移動到 `dags/` 資料夾：
```
spotify_bihourly_download_dag.py
spotify_daily_data_pipeline.py
spotify_daily_download_dag.py
utils/
```

### 3️⃣ 啟動 Airflow
```bash
airflow scheduler
airflow webserver -p 8080
```

### 4️⃣ 設定連線資訊
請根據你的環境配置以下連線資訊：
- **AWS S3 存取資訊**：`aws_access_key.txt`
- **MySQL 資料庫連線資訊**：`db_access_key.txt`
- **Spotify API 認證資訊**：
  - `spotify_client_info.txt`
  - `tokens.json`

DAG 的詳細功能與流程可參考 `dags/` 內的 Python 檔案。

---

# 🎵 Spotify Data Pipeline (English Version)

## 📌 Project Overview
This project is designed to track users' listening behavior on Spotify and use it for future song recommendations. The key features include:
1. **Tracking recently played songs**: Since the Spotify API only allows fetching the last 50 played songs, the system needs to fetch data frequently.
2. **Daily tracking of followed artists**: Ensures that the user's followed artist list is up-to-date.
3. **Recording and deduplicating song metadata**: Instead of storing song metadata immediately, the system waits until a sufficient amount of data is collected before processing and avoiding duplicates.

---

## 🔄 **Data Flow Architecture**
The project uses **Apache Airflow** to manage the data pipeline. Data is fetched from **Spotify API**, stored in **AWS S3**, and processed into **MySQL**. The workflow is as follows:

### **📌 Every 2 Hours (`spotify_bihourly_download_dag.py`)**
1. **Refresh Spotify Access Token** → Uses `refresh_access_token()` to obtain a new token.
2. **Fetch Recently Played Songs (`get_recently_played()`)**
   - The data is uploaded to **AWS S3** (`recently_played/YYYYMMDDHH.json`).

---

### **📌 Daily Data Collection (`spotify_daily_download_dag.py`)**
1. **Refresh Spotify Access Token**
2. **Fetch Followed Artists (`get_following_artist()`)**
   - The data is uploaded to **AWS S3** (`following_artists/YYYYMMDDHH.json`).

---

### **📌 Daily Data Processing (`spotify_daily_data_pipeline.py`)**
1. **Process Recently Played Songs (`process_recently_played()`)**
   - Reads `recently_played/YYYYMMDDHH.json`, processes the data, and inserts it into **MySQL**.
   - Ensures that duplicate records are not stored.
2. **Process Followed Artists (`process_following_artists()`)**
   - Reads `following_artists/YYYYMMDDHH.json`, processes the data, and inserts it into **MySQL**.

---

## ⚙️ **Usage**

### 1️⃣ Install Dependencies
```bash
pip install -r requirements.txt
```

### 2️⃣ Move DAG Files to the `dags/` Directory:
```
spotify_bihourly_download_dag.py
spotify_daily_data_pipeline.py
spotify_daily_download_dag.py
utils/
```

### 3️⃣ Start Airflow
```bash
airflow scheduler
airflow webserver -p 8080
```

### 4️⃣ Configure Connection Credentials:
- **AWS S3 access**: `aws_access_key.txt`
- **MySQL database credentials**: `db_access_key.txt`
- **Spotify API credentials**:
  - `spotify_client_info.txt`
  - `tokens.json`

For more details on DAG functionalities, refer to the Python files in the `dags/` folder.
