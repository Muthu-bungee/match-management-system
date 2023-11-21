from awsglue.context import GlueContext
from pyspark.sql import DataFrame
import boto3
import datetime  as dt
 
class AthenaReader:
    def __init__(self, glueContext: GlueContext):
        self.glue_context = glueContext
    
    def get_latest_s3_object_key(self, s3_bucket: str, prefix: str)-> str:
        YEAR, MONTH, DAY = 1998, 1, 1
        latest_s3_object_key = None
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(s3_bucket)  
        for file in bucket.objects.filter(Prefix = prefix):
            if '$folder$' not in file.key and (file.last_modified).replace(tzinfo = None) > dt.datetime(YEAR, MONTH, DAY, tzinfo = None):
                latest_s3_object_key = file.key
                YEAR, MONTH, DAY = file.last_modified.year, file.last_modified.month, file.last_modified.day
        return latest_s3_object_key
    
    def generate_athena_push_down_predicate_from_s3_key(self, s3_bucket: str, s3_key: str, last_partition: str):
        s3_key = self.get_latest_s3_object_key(s3_bucket, s3_key)
        push_down_predicate = ''
        for part in str.split(s3_key, '/'):
            if '=' in part:
                key = str.split(part, '=')[0]
                value = str.split(part, '=')[-1]
                push_down_predicate = push_down_predicate + f" {key} = '{value}' "
                if last_partition in part :
                    break
                else :
                    push_down_predicate = push_down_predicate + 'and'
        
        return push_down_predicate

    def create_dataframe_from_partition(self, database: str, table_name: str, s3_bucket: str, s3_key: str, last_partition: str) -> DataFrame:
        push_down_predicate = self.generate_athena_push_down_predicate_from_s3_key(s3_bucket, s3_key, last_partition)
        pairs_dyf = self.glue_context.create_dynamic_frame.from_catalog(
                        database=database,
                        table_name=table_name,
                        push_down_predicate=push_down_predicate)
                    
        pairs_df = pairs_dyf.toDF()
        return pairs_df
        
    def create_dataframe(self, database: str, table_name: str, push_down_predicate: str=None) -> DataFrame:
        if push_down_predicate is None:
            pairs_dyf = self.glue_context.create_dynamic_frame.from_catalog(
                        database=database,
                        table_name=table_name)
        else:
            pairs_dyf = self.glue_context.create_dynamic_frame.from_catalog(
                        database=database,
                        table_name=table_name,
                        push_down_predicate=push_down_predicate)
                    
        pairs_df = pairs_dyf.toDF()
        return pairs_df         

