import os
from pathlib import Path
import pandas as pd
from minio import Minio
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


# Environment variables
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
MINIO_API_ENDPOINT = os.getenv("MINIO_API_ENDPOINT")
CSV_DIRECTORY = Path("csv/")

if not all([ACCESS_KEY, SECRET_KEY, MINIO_API_ENDPOINT]):
    raise EnvironmentError("Please set ACCESS_KEY, SECRET_KEY, and MINIO_API_ENDPOINT in your kube configmap file.")

def ingest_csv():
    """
    Lists all CSV files in the CSV_DIRECTORY
    """
    csv_files = [f for f in CSV_DIRECTORY.glob("*.csv") if f.is_file()]
    if not csv_files:
        print(f"No CSV files found in {CSV_DIRECTORY}")
    return csv_files

def convert_csv_files_to_parquet_files(csv_files):
    """
    Converts CSV files to Parquet and returns the list of Parquet files
    """
    parquet_files = []
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            parquet_file = csv_file.with_suffix(".parquet")
            df.to_parquet(parquet_file, index=False)
            parquet_files.append(parquet_file)
            print(f"Converted {csv_file} → {parquet_file}")
        except Exception as e:
            print(f"Error converting {csv_file} to Parquet: {e}")
    return parquet_files

def upload_parquet_to_minio(parquet_files):
    """
    Uploads Parquet files to MinIO bucket
    """
    minio_client = Minio(
        MINIO_API_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False
    )

    bucket_name = "csv"
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")

    for parquet_file in parquet_files:
        object_name = parquet_file.name
        try:
            minio_client.fput_object(bucket_name, object_name, str(parquet_file))
            print(f"Uploaded {parquet_file} → {bucket_name}/{object_name}")
        except Exception as e:
            print(f"Failed to upload {parquet_file}: {e}")

def main():
    csv_files = ingest_csv()
    if not csv_files:
        return

    parquet_files = convert_csv_files_to_parquet_files(csv_files)
    if not parquet_files:
        print("No Parquet files were created.")
        return

    upload_parquet_to_minio(parquet_files)

if __name__ == "__main__":
    main()
