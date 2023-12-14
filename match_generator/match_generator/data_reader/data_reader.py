from pyspark.sql.types import DoubleType
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession

class DataFetcher:
    def __init__(self, args, spark:SparkSession, env ) -> None:
        self.args = args
        self.spark = spark
        self.env = env
        empty_schema = StructType([])
        # Create an empty DataFrame
        self.empty_df = self.spark.createDataFrame([], schema=empty_schema)
        
        
    def fetch_match_suggestion(self) -> DataFrame:
        if self.env == 'dev':
            match_suggestion = self.spark.read.option("header", "true").csv("/home/preacher/Bungee/CodeRepo/match-management-system/data/1st_run.csv")
            return match_suggestion
        try:
            self.database = self.args["match_suggestion"]["database"]
            self.table = self.args["match_suggestion"]["table"]
            match_suggestion = self.spark.sql(f'select * from `{self.database}`.`{self.table}`')
            if self.env != 'prod':
                match_suggestion.show()
            return match_suggestion
        except Exception as e:
            raise e
    
    def fetch_upc_matches(self) -> DataFrame:
        try: 
            start = self.args["upc"]["start"]
            end = self.args["upc"]["end"]
            upc_match_suggestion = self.spark.sql(f"select * from {self.database}.{self.table} where match_source = 'UPC' and creation_date between {start} and {end}")
            return upc_match_suggestion
        except Exception as e:
            return None
    
    def fetch_ml_matches(self) -> DataFrame:
        try: 
            start = self.args["ml"]["start"]
            end = self.args["ml"]["end"]
            ml_match_suggestion = self.spark.sql(f"select * from {self.database}.{self.table} where match_source = 'ML' and creation_date between {start} and {end}")
            return ml_match_suggestion
        except Exception as e:
            return None
    
    def fetch_transitive_matches(self) -> DataFrame:
        try: 
            start = self.args["transitive"]["start"]
            end = self.args["transitive"]["end"]
            transitive_match_suggestion = self.spark.sql(f"select * from {self.database}.{self.table} where match_source = 'TRANSITIVE' and creation_date between {start} and {end}")
            return transitive_match_suggestion
        except Exception as e:
            return None
    
    def fetch_mpn_matches(self) -> DataFrame:
        try: 
            start = self.args["mpn"]["start"]
            end = self.args["mpn"]["end"]
            mpn_match_suggestion = self.spark.sql(f"select * from {self.database}.{self.table} where match_source = 'MPN' and creation_date between {start} and {end}")
            return mpn_match_suggestion
        except Exception as e:
            return None
    
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
        
    
# class DataReader:
#     def __init__(self,glue_context):
#         self.glue_context=glue_context
#         self.s3reader=S3Reader(glue_context)
#         self.athena=AthenaReader(glue_context)

#     def getDeltaMatches(self):
#         matches = self.spark.sql
#         return df
    
#     def getExistingMatches(self,delta_pairs):
#         s3reader=S3Reader(self.glue_context)
#         #df=s3reader.read_single_file('csv','s3://mdp-ut.east1/match/mdp_match_agg')
#         df=self.athena.create_dataframe('mdp','match')
#         print('*********before',df.count())
#         df=df.filter(df.pair_id.isin(delta_pairs))
#         print('*********after',df.count())

#         #df = df.withColumn("score", df["score"].cast(DoubleType()))
#         return df
        
        