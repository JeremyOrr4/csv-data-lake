import os
import pandas as pandas
import pyarrow as pyarrow

from minio import Minio
from dotenv import load_dotenv


csv_directory = "csv/"

def main():

    def ingest_csv():
        csv_files = [
            os.path.join(csv_directory, f)
            for f in os.listdir(csv_directory)
            if os.path.isfile(os.path.join(csv_directory, f))
        ]

        return csv_files        
        
    
    def convert_csv_files_to_parquet_files(csv_files):
        parquet_files = []

        for csv in csv_files:
            dataframe = pandas.read_csv(csv)
            
            name, _ = os.path.splitext(csv)
            parquet_file = f"{name}.parquet"
            dataframe.to_parquet(parquet_file, index=False)

            parquet_files.append(parquet_file)

        return parquet_files
    
    # def upload_parquet_to_csv(parquet_files):
            
    #     load_dotenv()
    #     ACCESS_KEY = os.environ.get('ACCESS_KEY')
    #     SECRET_KEY = os.environ.get('SECRET_KEY')
    #     MINIO_API_ENDPOINT = os.environ.get('MINIO_API_ENDPOINT')

    #     MINIO_CLIENT = Minio(MINIO_API_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)

    #     found = MINIO_CLIENT.bucket_exists("csv")
    #     if not found:
    #         MINIO_CLIENT.make_bucket("csv")
    #     else:
    #         print("Bucket already exists")    
    #         MINIO_CLIENT.fput_object("<bucketname>", "<pic.jpg>", LOCAL_FILE_PATH,)    
    #         print("It is successfully uploaded to bucket")


    # def upload_parquet_to_minio(parquet_files):
        
    #     load_dotenv()






    #     # ingest_csv(file_path):
    #     #     → validate_csv(file_path)
    #     #     → parquet_path = convert_to_parquet(file_path)
    #     #     → s3_uri = upload_to_minio(parquet_path)
    #     #     → register_iceberg_table(table_name, schema, s3_uri)
    #     #     → return success

    

    csv_files = ingest_csv()
    # validate_csv_files()
    parquet_files = convert_csv_files_to_parquet_files(csv_files)
    upload_parquet_to_csv(parquet_files)



if __name__ == "__main__":
    main()



# ingest_csv(file_path):
#     → validate_csv(file_path)
#     → parquet_path = convert_to_parquet(file_path)
#     → s3_uri = upload_to_minio(parquet_path)
#     → register_iceberg_table(table_name, schema, s3_uri)
#     → return success







