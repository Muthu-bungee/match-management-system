import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from match_generator.runner import MatchGenerator

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer').config('spark.sql.hive.convertMetastoreParquet', 'false').config("spark.sql.autoBroadcastJoinThreshold", -1).config("spark.sql.optimizer.pushedFilters", "true").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session
spark.sparkContext.setLogLevel('ERROR')
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()


if __name__ == "__main__":
    print("running in environment")
    MatchGenerator( spark, args, "ut").run()
    print('Program run successfully')
    
    
