from pyspark.sql import DataFrame

class S3Writer:
    def __init__(self, s3_bucket: str):
        self.s3_bucket = s3_bucket

    def save_dataframe_to_s3(self, key:str, df: DataFrame):
        s3_path = f's3a://{self.s3_bucket}/{key}/'
        print(f'saving df to {s3_path}')
        df.write.format("parquet").mode("overwrite").save(s3_path,header = 'true')