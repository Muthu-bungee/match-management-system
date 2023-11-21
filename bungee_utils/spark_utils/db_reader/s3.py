from awsglue.context import GlueContext
from pyspark.sql import DataFrame
import boto3
import datetime  as dt
from typing import List

#  database reader start
class S3Reader:
    def __init__(self, glueContext: GlueContext):
        self.glue_context = glueContext

    def create_dataframe(self, file_format: str, s3_bucket=None, s3_key=None, s3_path=None)-> DataFrame:
        if s3_bucket is not None and s3_key is not None:
            s3_path = f's3://{s3_bucket}/{s3_key}'
        
        glue_df = self.glue_context.create_dynamic_frame_from_options(
            connection_type="s3",
            format = file_format,
            connection_options={
                "paths": [s3_path]
            },
            format_options={
                "withHeader": True,
            })
        
        spark_df = glue_df.toDF()
        return spark_df

    def get_latest_s3_object(self, s3_bucket: str, prefix: str)-> str:
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
        return key
    
    def create_dataframe_from_latest_partition(self, s3_bucket, prefix, suffix, last_partition, file_format)-> DataFrame:
        s3_key = self.generate_s3_key_from_latest_partition(s3_bucket, prefix, last_partition)
        if suffix is not None:
            s3_key = f'{s3_key}/{suffix}'
        return self.create_dataframe(file_format = file_format, s3_bucket = s3_bucket, s3_key = s3_key )
    
    def read_multiple_partitions(self, file_format: str, s3_path_list: List[str]) :
        glue_df = self.glue_context.create_dynamic_frame.from_options(
            connection_type='s3',
            connection_options={
                'paths': s3_path_list
            },
            format = file_format,
            format_options={
                "withHeader": True
            }
        )
        spark_df = glue_df.toDF()
        return spark_df
    
    def read_multiple_partitionsWithprefix(self, s3_bucket_with_prefix:str, paths:List[str]) :
        full_paths = [f'{s3_bucket_with_prefix}/{path}' for path in paths]
        print(full_paths)
        dynamicframe = self.glue_context.create_dynamic_frame.from_options(
            connection_type='s3',
            connection_options={
                'paths': full_paths
            },
            format='parquet',
            format_options={
                "withHeader": True
            }
        )
        return dynamicframe
    
    def read_single_file(self, ftype,path:str) :
        fpath=path+'.'+ftype
        print(fpath)
        dynamicframe = self.glue_context.read.\
           format(ftype).\
           option("header", "true").\
           load(fpath)
        return dynamicframe
    
