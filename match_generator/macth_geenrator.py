from pyspark.context import SparkContext
from awsglue.context import GlueContext
import sys
sys.path.insert(0,'/home/glue_user/workspace')
from match_generator.src.runner import MatchGenerator
if __name__=='__main__':
    spark=SparkContext.getOrCreate()
    spark.setLogLevel("WARN")
    glue_context=GlueContext(spark)
    run_config={
        "env":"ut"
    }
generator=MatchGenerator(glue_context)
generator.run(None)
