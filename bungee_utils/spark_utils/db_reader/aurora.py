from awsglue.context import GlueContext
from pyspark.sql import DataFrame
import boto3
import json

class AuroraReader:
    def __init__(self, glueContext: GlueContext):
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
    
    def get_data(self, database: str, secret_id: str, query: str ) -> DataFrame:
        print(query)
        db_info = self.get_secret(secret_id)
        db_url = f"jdbc:postgresql://{db_info['host']}:{db_info['port']}/{database}"
        try:
            dynamic_frame = self.glue_context.read.format("jdbc") \
                            .option("url", db_url)\
                            .option("dbtable", f"({query}) AS tmp") \
                            .option("user", db_info['username'])\
                            .option("password", db_info['password'])\
                            .load()
            return dynamic_frame
        except Exception as e:
            raise e
