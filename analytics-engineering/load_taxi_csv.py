import os
import sys
import urllib.request
import base64
import json
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from itertools import product

# Third-party imports
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
from dotenv import load_dotenv

# --- PATH & ENV SETUP ---
SCRIPT_DIR = Path(__file__).parent 
ENV_PATH = SCRIPT_DIR / ".env_encoded"
load_dotenv(dotenv_path=ENV_PATH)

# --- CONSTANTS ---
BUCKET_NAME = "kestra-zoomcamp-julian11"
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month}.csv.gz"
COLORS = ["green", "yellow"]
YEARS = [2019, 2020]
MONTHS = [f"{i:02d}" for i in range(1, 13)]
DOWNLOAD_DIR = SCRIPT_DIR / "data"
CHUNK_SIZE = 8 * 1024 * 1024

def get_gcs_client():
    """Initializes the GCS client by decoding the Base64 service account key."""
    encoded_creds = os.getenv("SECRET_GCP_SERVICE_ACCOUNT")
    if encoded_creds:
        try:
            decoded_creds = base64.b64decode(encoded_creds).decode("utf-8")
            creds_info = json.loads(decoded_creds)
            return storage.Client.from_service_account_info(creds_info)
        except Exception as e:
            print(f"❌ Failed to decode credentials: {e}")
            sys.exit(1)
    
    print("⚠️ SECRET_GCP_SERVICE_ACCOUNT not found. Falling back to gcs.json...")
    return storage.Client.from_service_account_json("gcs.json")

# Initialize Client and Bucket
client = get_gcs_client()
bucket = client.bucket(BUCKET_NAME)

def download_file(params):
    """Downloads csv.gz files from GitHub releases."""
    color, year, month = params
    url = BASE_URL.format(color=color, year=year, month=month)
    filename = f"{color}_tripdata_{year}-{month}.csv.gz"
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    
    try:
        print(f"⬇️ Downloading {filename}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"✅ Downloaded: {filename}")
        return file_path
    except Exception as e:
        print(f"❌ Failed to download {url}: {e}")
        return None

def create_bucket_if_not_exists(bucket_name):
    """Checks for bucket existence and ownership once."""
    try:
        client.get_bucket(bucket_name)
        print(f"✔️ Bucket '{bucket_name}' verified.")
    except NotFound:
        print(f"🚀 Creating bucket '{bucket_name}'...")
        client.create_bucket(bucket_name)
    except Forbidden:
        print(f"🚫 Bucket name '{bucket_name}' is taken or inaccessible.")
        sys.exit(1)

def upload_to_gcs(file_path, max_retries=3):
    """Uploads a single file to GCS with retry logic."""
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE
    
    for attempt in range(max_retries):
        try:
            print(f"⬆️ Uploading {blob_name} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            if blob.exists(client):
                print(f"⭐ Verification successful for {blob_name}")
                # Optional: os.remove(file_path)  # Delete local file after upload
                return
        except Exception as e:
            print(f"⚠️ Attempt {attempt + 1} failed for {blob_name}: {e}")
            time.sleep(5)
    
    print(f"❌ Giving up on {blob_name} after {max_retries} attempts.")

if __name__ == "__main__":
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    create_bucket_if_not_exists(BUCKET_NAME)
    
    # Generate all combinations: 2 colors × 2 years × 12 months = 48 files
    all_params = list(product(COLORS, YEARS, MONTHS))
    
    print(f"--- Starting Parallel Download ({len(all_params)} files) ---")
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, all_params))
    
    valid_files = [f for f in file_paths if f]
    print(f"\n✅ Successfully downloaded {len(valid_files)}/{len(all_params)} files")
    
    print(f"\n--- Starting Parallel Upload ---")
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, valid_files)
    
    print("🏁 All processes completed.")