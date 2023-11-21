from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType
class Aggregator:
    def __init__(self,glue_context) -> None:
        self.glue_context=glue_context
        
    
    def aggregate(self,delta_match_df,existing_match_df):
         existing_match_df.cache()
         delta_match_df.cache()
         existing_match_df.show()
         delta_match_df.show()
         print('---------------count of existing b4 explode is',existing_match_df.count())
         
         existing_match_df=existing_match_df.withColumn("type_score_list", explode(existing_match_df.type_score_list))
         existing_match_df=existing_match_df.select(col('*'),col('type_score_list')[0].alias('source_type'),col('type_score_list')[1].alias('score'))
    
         for column in existing_match_df.columns:
            existing_match_df = existing_match_df.withColumnRenamed(column, 'match_' + column)

         existing_match_df.show()
         print('---------------count of existing is',existing_match_df.count())
         existing_match_df.printSchema()
         print('-----------------match------------------',existing_match_df.count())
         delta_match_df.printSchema()
         print('------------delta-----------------------',delta_match_df.count())
         
         full_df = delta_match_df.join(existing_match_df, (delta_match_df["pair_id"] == existing_match_df["match_pair_id"]) & (delta_match_df["source_type"] == existing_match_df["match_source_type"]),how='full_outer')
         full_df.cache()
         full_df=full_df.withColumn('final_product_segment',when(isnull(col('product_Segment')) , col('match_product_segment')).otherwise(col('product_Segment')))
         full_df=full_df.withColumn('final_pair_id',when(isnull(col('pair_id')) , col('match_pair_id')).otherwise(col('pair_id')))
         full_df=full_df.withColumn('final_type',when(isnull(col('source_type')),col('match_source_type')).otherwise(col('source_type')))
         full_df=full_df.withColumn('final_score',when(isnull(col('score')),col('match_score')).otherwise(col('score')))
         full_df.printSchema()
         print('------------ful',full_df.count())
         full_df.show()
         full_df.write.format("parquet").mode("overwrite").save('s3://mdp-ut.east1/aggregated/',header = 'true')
         return full_df
