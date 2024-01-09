from datetime import datetime
from pyspark.sql.types import DoubleType
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from bungee_utils.spark_utils.db_reader.aurora import AuroraReader
from pyspark.sql.functions import *
from datetime import datetime, timedelta
from mdp_common_utils.schema import *
from mdp_common_utils.constants import *
class DataFetcher:
    def __init__(self, args, spark:SparkSession, env , glue_context) -> None:
        print(env)
        self.args = args
        self.spark = spark
        self.env = env
        self.glue_context = glue_context
        self.time_stamp = self.args["last_stack_run_date"]
        self.time_stamp_integer = int(datetime.strptime(self.time_stamp, '%Y-%m-%d').timestamp())
        empty_schema = StructType([])
        # Create an empty DataFrame
        self.empty_df = self.spark.createDataFrame([], schema=empty_schema)
        self.fastlane_input_cols = ['score', 'completed_date', 'comp_source_store','base_source_store', 'base_sku', 'comp_sku', 'answer', 'company_code']

        
    def fetch_customer_audit_library(self) -> DataFrame:
        match_suggestion
        try:
            if self.env == 'dev':
                match_suggestion = self.spark.read.option("header", "true").csv("/home/preacher/Bungee/CodeRepo/match-management-system/data/fastlane_successful.csv")
            elif self.env == 'ut':
                file_path = self.args['match_library_path']
                match_suggestion= self.spark.read.option('header','true').csv(file_path)
                match_suggestion = match_suggestion.withColumn("score", col("score").cast("double"))
                match_suggestion.show()
            elif self.env == 'prod':
                database = self.args["customer_audit_library"]["database"]
                access_key = self.args["customer_audit_library"]["access_key"]
                table = self.args["customer_audit_library"]["table"]
                
                query = f"""
                        SELECT * FROM {table}
                        WHERE (manager_verified_date IS NOT NULL AND manager_verified_date >= '{self.time_stamp})'
                        OR (customer_verified_date IS NOT NULL AND customer_verified_date  >= '{self.time_stamp})'
                        OR (match_date IS NOT NULL AND match_date >= {self.time_stamp_integer})
                        """
                print(query)
                match_suggestion = AuroraReader(self.glue_context).get_data(database, access_key, query)
                
            match_suggestion.printSchema()    
            if self.env != 'prod':
                match_suggestion.show()
            return match_suggestion
        except Exception as e:
            raise e
        
    def fetch_successful_bungee_audit_library(self) -> DataFrame:
        fastlane_df = None
        try:
            if self.env == 'dev':
                fastlane_df = self.spark.read.option("header", "true").csv("/home/preacher/Bungee/CodeRepo/match-management-system/data/fastlane_successful.csv")
            elif self.env == 'ut':
                file_path = self.args['fastlane_success_path']
                fastlane_df= self.spark.read.option('header','true').csv(file_path)
                fastlane_df = fastlane_df.withColumn("score", col("score").cast("double"))
                fastlane_df.show()
            elif self.env == 'prod':
                push_down_predicate = ''
                start = self.args['audited_request']["fastlane"]['start']
                end = self.args['audited_request']["fastlane"]['end']
                self.database = self.args["successful_bungee_audit_library"]['database']
                self.table = self.args["successful_bungee_audit_library"]['table']
                push_down_predicate = self._generate_file_paths(start, end)
                print('push_down_predicate = ',push_down_predicate)
                fastlane_dyf = self.glueContext.create_dynamic_frame.from_catalog(
                                     database=self.database,
                                     table_name=self.table,
                                     push_down_predicate=push_down_predicate)
                fastlane_dyf = fastlane_dyf.resolveChoice(specs = [('score','cast:Double'), ("match_date", "cast:int"), ("comp_size", "cast:String"), ("base_size", "cast:String"), ('base_sku','cast:String'), ('comp_sku','cast:String'), ('base_upc','cast:String'), ('comp_upc','cast:String')])
                fastlane_df = fastlane_dyf.toDF()
                fastlane_df = fastlane_df.select(self.fastlane_input_cols)
            
            fastlane_df.printSchema()
            if self.env != 'prod':
                fastlane_df.show()
            return fastlane_df
        except Exception as e:
            raise e
        
        # error = utils.validate_dataframe(fastlane_df, self.fastlane_input_schema)
        # if error != None:
        #     raise ValueError(error)
        fastlane_df = fastlane_df.select(self.fastlane_input_cols)
        return fastlane_df
    
    def fetch_unsuccessful_bungee_audit_library(self) -> DataFrame:
        fastlane_df = None
        try:
            if self.env == 'dev':
                fastlane_df = self.spark.read.option("header", "true").csv("/home/preacher/Bungee/CodeRepo/match-management-system/data/fastlane_successful.csv")
            elif self.env == 'ut':
                file_path = self.args['fastlane_unsuccess_path']
                fastlane_df= self.spark.read.option('header','true').csv(file_path)
                fastlane_df = fastlane_df.withColumn("score", col("score").cast("double"))
                fastlane_df.show()
            elif self.env == 'prod':
                push_down_predicate = ''
                start = self.args['audited_request']["fastlane"]['start']
                end = self.args['audited_request']["fastlane"]['end']
                self.database = self.args["unsuccessful_bungee_audit_library"]['database']
                self.table = self.args["unsuccessful_bungee_audit_library"]['table']
                push_down_predicate = self._generate_file_paths(start, end)
                print('push_down_predicate = ',push_down_predicate)
                fastlane_dyf = self.glueContext.create_dynamic_frame.from_catalog(
                                     database=self.database,
                                     table_name=self.table,
                                     push_down_predicate=push_down_predicate)
                fastlane_dyf = fastlane_dyf.resolveChoice(specs = [('score','cast:Double'), ("match_date", "cast:int"), ("comp_size", "cast:String"), ("base_size", "cast:String"), ('base_sku','cast:String'), ('comp_sku','cast:String'), ('base_upc','cast:String'), ('comp_upc','cast:String')])
                fastlane_df = fastlane_dyf.toDF()
                fastlane_df = fastlane_df.select(self.fastlane_input_cols)
                
            fastlane_df.printSchema()
            if self.env != 'prod':
                fastlane_df.show()
            return fastlane_df
        except Exception as e:
            raise e
        
        # error = utils.validate_dataframe(fastlane_df, self.fastlane_input_schema)
        # if error != None:
        #     raise ValueError(error)
        fastlane_df = fastlane_df.select(self.fastlane_input_cols)
    
    def fetch_audited_matches_from_mw(self) -> DataFrame:
        audited_matches = None
        try:
            if self.env == 'dev':
                audited_matches = self.spark.read.option("header", "true").parquet("/home/preacher/Bungee/CodeRepo/match-management-system/data/match_warehouse.parquet")
            elif self.env == 'ut':
                file_path = self.args['unaudited_match_path_path']
                audited_matches= self.spark.read.option('header','true').csv(file_path)
                audited_matches.show()
            elif self.env == 'prod':
                mw_database = self.args["match_warehouse"]["database"]
                mw_table = self.args["match_warehouse"]["table"]
                matched_audit_status = f"'{BUNGEE_AUDIT_STATUS.SIMILAR_MATCH}','{BUNGEE_AUDIT_STATUS.EXACT_MATCH}','{BUNGEE_AUDIT_STATUS.NOT_MATCH}'"
                query = f"""SELECT * FROM `{mw_database}`.`{mw_table}` WHERE {MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME} in ({matched_audit_status})"""
                print("Running query  =", query)
                audited_matches = self.spark.sql(query)
                
            audited_matches.printSchema() 
            if self.env != 'prod':
                audited_matches.show()
            return audited_matches
        except Exception as e:
            raise e
        
    def fetch_unaudited_matches_from_mw_based_on_source_store(self, base_source_store_list List[str]) -> DataFrame:
        unaudited_matches = None
        try:
            if self.env == 'dev':
                unaudited_matches = self.spark.read.option("header", "true").parquet("/home/preacher/Bungee/CodeRepo/match-management-system/data/match_warehouse.parquet")
            elif self.env == 'ut':
                file_path = self.args['unaudited_match_path_path']
                unaudited_matches= self.spark.read.option('header','true').csv(file_path)
            elif self.env == 'prod':
                base_source_store_str = ", ".join([ f"'{source}'" for source in base_source_store_list])
                mw_database = self.args["match_warehouse"]["database"]
                mw_table = self.args["match_warehouse"]["table"]
                query = f"""SELECT * FROM `{mw_database}`.`{mw_table}` 
                WHERE {MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME} == '{BUNGEE_AUDIT_STATUS.UNAUDITED}'
                AND base_source_store in ({base_source_store_str})"""
                print("Running query  =", query)
                unaudited_matches = self.spark.sql(query)
            
            unaudited_matches.printSchema()   
            if self.env != 'prod':
                unaudited_matches.show()
            return unaudited_matches
        except Exception as e:
            raise e
        
        
    def _generate_file_paths(self, start_date_str, end_date_str):
        # Function to get all dates between two given dates
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        
        current_date = start_date
        s3_path_list = []
    
        while current_date <= end_date:
            year = current_date.strftime('%Y')
            month = current_date.strftime("%m")
            day = current_date.strftime("%d")
            s3_key = f"""(year = '{year}' and month = '{month}' and day = '{day}')"""
            s3_path_list.append(s3_key)
            current_date += timedelta(days=1)
            
        push_down_predicate = " or ".join(s3_path_list)
        return push_down_predicate
        
    
"""
SELECT base_sku, comp_sku, score, match_date, customer_review_state, bungee_review_state, match_status, base_source_store, comp_source_store FROM "match_library"."match_library_snapshot" limit 10;
SELECT sku_uuid_a, sku_uuid_b, base_source_store, comp_source_store, segment, inserted_date, answer, score FROM "ml_internal_uat"."fastlane_dump" limit 10;
"""

# we need to explore more on this 
# pair_id, uuid_a, uuid_b, status, created_date , updated_date-> match_warehouse
# abc_def     abc     def     audited     29-12-2023  02-01-2024

# pair_id, uuid_a, uuid_b, status, created_date , updated_date  -> new data 
# def_ghi     def     ghi     audited     01-01-2024  02-01-2024  -> new -data
# abc_def     abc     def     audited     01-01-2024  02-01-2024  -> old -daat