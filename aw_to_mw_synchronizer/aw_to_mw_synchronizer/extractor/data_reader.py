from datetime import datetime
from pyspark.sql.types import DoubleType
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from bungee_utils.spark_utils.db_reader.aurora import AuroraReader

class DataFetcher:
    def __init__(self, args, spark:SparkSession, env , glue_context) -> None:
        self.args = args
        self.spark = spark
        self.env = env
        self.glue_context = glue_context
        self.time_stamp = self.args["last_stack_run_date"]
        self.time_stamp_integer = int(datetime.strptime(self.time_stamp, '%Y-%m-%d').timestamp())
        empty_schema = StructType([])
        # Create an empty DataFrame
        self.empty_df = self.spark.createDataFrame([], schema=empty_schema)
        
        
    def fetch_customer_audit_library(self) -> DataFrame:
        if self.env == 'dev':
            match_suggestion = self.spark.read.option("header", "true").csv("/home/preacher/Bungee/CodeRepo/match-management-system/data/1st_run.csv")
            return match_suggestion
        try:
            database = self.args["customer_audit_library"]["database"]
            access_key = self.args["customer_audit_library"]["access_key"]
            table = self.args["customer_audit_library"]["table"]
            
            query = f"""
                    SELECT * FROM {table}
                    WHERE manager_verified_date >= '{self.time_stamp}'
                    AND customer_verified_date  >= '{self.time_stamp}'
                    AND match_date >= {self.time_stamp_integer}
                    """
            match_suggestion = AuroraReader(self.glue_context).get_data(database, access_key, query)
            if self.env != 'prod':
                match_suggestion.show()
            return match_suggestion
        except Exception as e:
            raise e
        
    def fetch_bungee_audit_library(self) -> DataFrame:
        if self.env == 'dev':
            match_suggestion = self.spark.read.option("header", "true").csv("/home/preacher/Bungee/CodeRepo/match-management-system/data/1st_run.csv")
            return match_suggestion
        try:
            time_stamp = self.args["last_stack_run_date"]
            self.database = self.args["bungee_audit_library"]["database"]
            self.table = self.args["bungee_audit_library"]["table"]
            query = f"""
                SELECT * FROM `{self.database}`.`{self.table}` WHERE match_date >= '{self.time_stamp_integer}'
            """
            match_suggestion = self.spark.sql(f'SELECT * FROM `{self.database}`.`{self.table}` WHERE')
            if self.env != 'prod':
                match_suggestion.show()
            return match_suggestion
        except Exception as e:
            raise e
    
    def fetch_mdw(self) -> DataFrame:
        if self.env == 'dev':
            mw_df = self.spark.read.option("header", "true").parquet("/home/preacher/Bungee/CodeRepo/match-management-system/data/match_warehouse.parquet")
            return mw_df
        try:
            mw_database = self.args["match_warehouse"]["database"]
            mw_table = self.args["match_warehouse"]["table"]
            mdw = self.spark.sql(f"""SELECT * FROM `{mw_database}`.`{mw_table}`""")
            if self.env != 'prod':
                mdw.show()
            return mdw
        except Exception as e:
            raise e
        
    
"""
SELECT base_sku, comp_sku, score, match_date, customer_review_state, bungee_review_state, match_status, base_source_store, comp_source_store FROM "match_library"."match_library_snapshot" limit 10;
SELECT sku_uuid_a, sku_uuid_b, base_source_store, comp_source_store, segment, inserted_date, answer, score FROM "ml_internal_uat"."fastlane_dump" limit 10;
"""