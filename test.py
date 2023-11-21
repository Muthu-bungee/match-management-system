from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import StructType,StructField,StringType,ArrayType,IntegerType,DoubleType
from pyspark.sql.functions import col,expr,when,lit,desc
spark_context=SparkContext()
print('spark_context',spark_context)
spark=SparkSession(spark_context)
print('spark_session',spark)


mschema=StructType(
    [
        StructField('id',IntegerType(),True),
        StructField('pair_id',StringType(),True),
        StructField('uuid_a',StringType(),True),
        StructField('uuid_b',StringType(),True),
        StructField('score', DoubleType(),True),
        StructField('source_type',StringType(),True),
        StructField('product_Segment',StringType(),True),
        StructField('seller_flag',DoubleType(),True)
    ]
)



df=spark.read.format('csv').option('header',True).schema(mschema).load('mdp_sugg.csv')
#df.show()
df.printSchema()
#df.select('score').show(2)
mdf=df.select('score')
#mdf.select(expr('score as pairscore')).show()
#mdf.selectExpr('score as mainscore').show()
#mdf.selectExpr('avg(score)').show()
#df.selectExpr('*','(score>0) as highscore').show(3)
#df.withColumn('SCORE_TYPE',when(expr('score>0.8'),'HIGHSCORE').otherwise('LOWSCORE')).show()
#for col in df.columns:
 #   df=df.withColumnRenamed(col,'match_'+col)
#df.show()
df=df.drop('id')
df.show()

#df.withColumn('score',col('score').cast('long')).show()
#df.where(col('score')>0.5).where(col('score')<0.8).show()
#df.select('score').distinct().show()
#select all columns in df, but distinct on single collumn . HOW ????????????????????????????????
#df.sort(col('score').desc()).show()

df.where(col('score')>0.5).select(col('pair_id')).show()



