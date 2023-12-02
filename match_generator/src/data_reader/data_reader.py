from bungee_utils.spark_utils.db_reader.s3 import S3Reader
from bungee_utils.spark_utils.db_reader.aurora import AuroraReader
from bungee_utils.spark_utils.db_reader.athena import AthenaReader
from pyspark.sql.types import DoubleType
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession

class DataFetcher:
    def __init__(self, args, spark:SparkSession ) -> None:
        self.args = args
        self.spark = spark
        empty_schema = StructType([])
        # Create an empty DataFrame
        self.empty_df = self.spark.createDataFrame([], schema=empty_schema)
        pass
    
    def _fetch_upc_matches(self) -> DataFrame:
        upc_match_suggestion = self.empty_df
        return upc_match_suggestion
    
    def fetch_ml_matches(self) -> DataFrame:
        ml_match_suggestion = self.empty_df
        return ml_match_suggestion
    
    def fetch_transitive_matches(self) -> DataFrame:
        transitive_match_suggestion = self.empty_df
        return transitive_match_suggestion
    
    def fetch_mpn_matches(self) -> DataFrame:
        mpn_match_suggestion = self.empty_df
        return mpn_match_suggestion
    
    def fetch_mdw(self) -> DataFrame:
        mdw = self.spark.sql("""SELECT * FROM (
                                    SELECT * FROM {mdw_database}.{mdw_table} A
                                    JOIN
                                    SELECT * FROM {temp_match_database}.{temp_match_table} B
                                    ON A.pair_id == B.pair_id
                                )""")
        
        return mdw
    
class DataReader:
    def __init__(self,glue_context):
        self.glue_context=glue_context
        self.s3reader=S3Reader(glue_context)
        self.athena=AthenaReader(glue_context)

    def getDeltaMatches(self):
        s3reader=S3Reader(self.glue_context)
        #df=s3reader.read_single_file('csv','s3://mdp-ut.east1/suggestion/mdp_sugg_ag')
        #df = df.withColumn("score", df["score"].cast(DoubleType()))
        df=self.athena.create_dataframe('mdp','delta')
        return df
    
    def getExistingMatches(self,delta_pairs):
        s3reader=S3Reader(self.glue_context)
        #df=s3reader.read_single_file('csv','s3://mdp-ut.east1/match/mdp_match_agg')
        df=self.athena.create_dataframe('mdp','match')
        print('*********before',df.count())
        df=df.filter(df.pair_id.isin(delta_pairs))
        print('*********after',df.count())

        #df = df.withColumn("score", df["score"].cast(DoubleType()))
        return df
        
        