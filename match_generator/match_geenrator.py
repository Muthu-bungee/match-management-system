import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from match_generator.src.runner import MatchGenerator

args = getResolvedOptions(sys.argv, ['JOB_NAME', "env"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.sparkContext.setLogLevel('WARN')
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

if __name__ == "__main__":
    print("running in environment" ,args['env'])
    MatchGenerator(glueContext, spark).run(None)
    print('Program run successfully')