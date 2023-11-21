from bungee_utils.spark_utils.db_reader.s3 import S3Reader
from bungee_utils.spark_utils.db_reader.aurora import AuroraReader
from bungee_utils.spark_utils.db_reader.athena import AthenaReader
from pyspark.sql.types import DoubleType

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
        
        