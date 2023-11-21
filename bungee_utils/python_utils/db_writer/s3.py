import pandas as pd
import awswrangler as wr
import boto3

class S3Writer:
    def __init__(self, s3_bucket: str):
        self.s3_bucket = s3_bucket

    def save_dataframe_to_s3(self, key:str, df: pd.DataFrame):
        s3_path = f's3://{self.s3_bucket}/{key}/output.parquet'
        wr.s3.to_parquet(df, s3_path, index=False)
        