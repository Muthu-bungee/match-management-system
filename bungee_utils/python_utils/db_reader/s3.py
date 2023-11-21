import boto3
import awswrangler as wr
import datetime  as dt

import pandas as pd

class S3Reader:
    def __init__(self):
        pass

    def create_dataframe(self, file_format: str, s3_bucket=None, s3_key=None, s3_path=None)-> pd.DataFrame:
        if s3_bucket is not None and s3_key is not None:
            s3_path = f's3://{s3_bucket}/{s3_key}'
        
        if file_format == 'parquet':
            df = wr.s3.read_parquet(path=s3_path)
            return df
        elif file_format == 'csv':
            df = wr.s3.read_csv(path=s3_path)
            return df
    
    def get_latest_s3_object(self, s3_bucket: str, prefix: str)-> str:
        print(s3_bucket, prefix)
        YEAR, MONTH, DAY = 1998, 1, 1
        latest_s3_object_key = None
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(s3_bucket)  
        for file in bucket.objects.filter(Prefix = prefix):
            if '$folder$' not in file.key and (file.last_modified).replace(tzinfo = None) > dt.datetime(YEAR, MONTH, DAY, tzinfo = None):
                latest_s3_object_key = file.key
                YEAR, MONTH, DAY = file.last_modified.year, file.last_modified.month, file.last_modified.day
        return latest_s3_object_key

    def generate_s3_key_from_latest_partition(self, s3_bucket: str, prefix: str, last_partition: str) -> str:
        latest_s3_object_key = self.get_latest_s3_object(s3_bucket, prefix)
        all_dir = str.split(latest_s3_object_key, '/')
        key = ''
        for dir in all_dir:
            key = key + dir + '/'
            if last_partition in dir:
                break
        if key.endswith('/'):
            key = key[:-1]
        return key
    
    def create_dataframe_from_latest_partition(self, s3_bucket, prefix, suffix, last_partition, file_format)-> pd.DataFrame:
        s3_key = self.generate_s3_key_from_latest_partition(s3_bucket, prefix, last_partition)
        print(s3_key, suffix)
        if suffix is not None:
            s3_key = f'{s3_key}/{suffix}'
        print(s3_key)
        return self.create_dataframe(file_format = file_format, s3_bucket = s3_bucket, s3_key = s3_key )
