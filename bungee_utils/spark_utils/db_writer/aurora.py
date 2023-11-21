from pyspark.sql import DataFrame

import boto3
import json

class AuroraWriter:
    def __init__(self, glueContext):
        self.glue_context = glueContext

    def get_secret(self, secret_id):
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name ="us-east-1"
        )
        try:
            get_secret_value_response = client.get_secret_value( SecretId = secret_id )
            str_obj = get_secret_value_response['SecretString']
            db_info = json.loads(str_obj)
            return db_info
        except Exception as e:
            raise e

    def save_dataframe(self, database: str, table: str, secret_id: str,  df: DataFrame, mode: str, query: str):
        db_info = self.get_secret(secret_id)
        try:
            if query is None:
                jdbcUrl = f"jdbc:postgresql://{db_info['host']}:{db_info['port']}/{database}"
                connectionProperties = {
                    "user": db_info['username'],
                    "password": db_info['password'],
                    "stringtype": "unspecified"
                }
                df.write.jdbc(url=jdbcUrl, table=table, mode = mode, properties=connectionProperties)
            else :
                jdbcUrl = f"jdbc:postgresql://{db_info['host']}:{db_info['port']}/{database}"
                connectionProperties = {
                    "user": db_info['username'],
                    "password": db_info['password'],
                    "where": query
                }
                df.write.jdbc(url=jdbcUrl, table=table, mode = mode, properties=connectionProperties)
        except Exception as e:
            raise e
