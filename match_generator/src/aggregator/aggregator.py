from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType
from pyspark.sql import DataFrame
import bungee_utils.spark_utils.function.dataframe_util as utils

# class Aggregator:
#     def __init__(self,glue_context) -> None:
#         self.glue_context=glue_context
        
    
#     def aggregate(self,delta_match_df,existing_match_df):
#         existing_match_df.cache()
#         delta_match_df.cache()
#         existing_match_df.show()
#         delta_match_df.show()
#         print('---------------count of existing b4 explode is',existing_match_df.count())
        
#         existing_match_df=existing_match_df.withColumn("type_score_list", explode(existing_match_df.type_score_list))
#         existing_match_df=existing_match_df.select(col('*'),col('type_score_list')[0].alias('source_type'),col('type_score_list')[1].alias('score'))

#         for column in existing_match_df.columns:
#             existing_match_df = existing_match_df.withColumnRenamed(column, 'match_' + column)

#         existing_match_df.show()
#         print('---------------count of existing is',existing_match_df.count())
#         existing_match_df.printSchema()
#         print('-----------------match------------------',existing_match_df.count())
#         delta_match_df.printSchema()
#         print('------------delta-----------------------',delta_match_df.count())
         
#         #  we have tpo add 3 and 4 here 
#         full_df = delta_match_df.join(existing_match_df, (delta_match_df["pair_id"] == existing_match_df["match_pair_id"]) & (delta_match_df["source_type"] == existing_match_df["match_source_type"]),how='full_outer')
#         full_df.cache()
#         full_df=full_df.withColumn('final_product_segment',when(isnull(col('product_Segment')) , col('match_product_segment')).otherwise(col('product_Segment')))
#         full_df=full_df.withColumn('final_pair_id',when(isnull(col('pair_id')) , col('match_pair_id')).otherwise(col('pair_id')))
#         full_df=full_df.withColumn('final_type',when(isnull(col('source_type')),col('match_source_type')).otherwise(col('source_type')))
#         full_df=full_df.withColumn('final_score',when(isnull(col('score')),col('match_score')).otherwise(col('score')))
#         full_df.printSchema()
#         print('------------ful',full_df.count())
#         full_df.show()
#         full_df.write.format("parquet").mode("overwrite").save('s3://mdp-ut.east1/aggregated/',header = 'true')
#         return full_df
    

class Aggregator:
    def __init__(self, glue_context: DataFrame, ml_match_suggestion: DataFrame, upc_match_suggestion: DataFrame, mpn_match_suggestion: DataFrame, transitive_match_suggestion: DataFrame, mdw: DataFrame) -> None:
        self.ml_match_suggestion = ml_match_suggestion
        self.upc_match_suggestion = upc_match_suggestion
        self.mpn_match_suggestion = mpn_match_suggestion
        self.transitive_match_suggestion = transitive_match_suggestion
        
        match_suggestion_list = [ml_match_suggestion, upc_match_suggestion, mpn_match_suggestion, transitive_match_suggestion]
        match_suggestion_list = [item for item in match_suggestion_list if item is not None]
        merged_match_suggestion = utils.combine_dfs(match_suggestion_list)
        self.merged_match_suggestion = utils.add_prefix_to_column_name(merged_match_suggestion, "new_", cols_to_rename)
        
        cols_to_rename = []
        self.mdw = mdw
        if mdw is not None:
            self.prefixed_mdw = utils.add_prefix_to_column_name(mdw, "old_", cols_to_rename)
        
        self.glue_context = glue_context
        
    def aggregate_match(self, mdw: DataFrame, match_suggestion: DataFrame):
        # cols = set(mdw.columns).difference(set(["source_score_map"]))
        mdw = mdw.select( "*", explode(col("source_score_map"))).drop("source_score_map")
        
        mdw.cache()
        self.merged_match_suggestion.cache()
        diff_column = col("old_score") - col("new_score")
        
        if self.mdw is not None:
            merged_matches = self.merged_match_suggestion.join(self.prefixed_mdw, ["pair_id", "match_source"], "full_outer")
        else:
            merged_matches = self.merged_match_suggestion.withColumn("old_score", lit(None))
            
        merged_matches = merged_matches.withColumn( "updated_score", 
                                                    when( col("old_score").isNull(), col("new_score") )\
                                                    .when( col("new_score").isNull(), col("old_score") )\
                                                    .when( (diff_column.between(-0.05, 0.05)) , col("old_score") )\
                                                    .otherwise( col("new_score") ) 
                                                )
        merged_matches = merged_matches.withColumn("match_status", when(col("old_score").isNull(), lit("updated"))\
                                                                    .when( col("old_score").isNotNull() & (col("old_score") != col("updated_score") ), lit("updated") )\
                                                                    .otherwise( lit("not_updated") ) 
                                                    )
        
        merged_matches = merged_matches.withColumnRenamed("updated_score", "score")
        
        merged_matches = merged_matches.groupBy("pair_id", "base_sku_uuid", "comp_sku_uuid", "base_source_store", "comp_source_store").\
                                        agg( map_from_entries( collect_list( struct("match_source","score"))).alias("source_score_map"), 
                                            collect_set("match_status").alias("match_status") , sum(col("score")).alias("aggregated_score") )
                                        
                                        
        updated_matches = merged_matches.filter(array_contains(col("match_status"), "updated"))
        
        updated_matches = updated_matches.withColumn("status", lit("unaudited"))
        
        
        # we need to remove audited matches but should we update the score as well
        # 
        
    # def aggregate(self,delta_match_df,existing_match_df):
    #     existing_match_df.cache()
    #     delta_match_df.cache()
    #     existing_match_df.show()
    #     delta_match_df.show()
    #     print('---------------count of existing b4 explode is',existing_match_df.count())
        
    #     existing_match_df=existing_match_df.withColumn("type_score_list", explode(existing_match_df.type_score_list))
    #     existing_match_df=existing_match_df.select(col('*'),col('type_score_list')[0].alias('source_type'),col('type_score_list')[1].alias('score'))

    #     for column in existing_match_df.columns:
    #         existing_match_df = existing_match_df.withColumnRenamed(column, 'match_' + column)

    #     existing_match_df.show()
    #     print('---------------count of existing is',existing_match_df.count())
    #     existing_match_df.printSchema()
    #     print('-----------------match------------------',existing_match_df.count())
    #     delta_match_df.printSchema()
    #     print('------------delta-----------------------',delta_match_df.count())
         
    #     #  we have tpo add 3 and 4 here 
    #     full_df = delta_match_df.join(existing_match_df, (delta_match_df["pair_id"] == existing_match_df["match_pair_id"]) & (delta_match_df["source_type"] == existing_match_df["match_source_type"]),how='full_outer')
    #     full_df.cache()
    #     full_df=full_df.withColumn('final_product_segment',when(isnull(col('product_Segment')) , col('match_product_segment')).otherwise(col('product_Segment')))
    #     full_df=full_df.withColumn('final_pair_id',when(isnull(col('pair_id')) , col('match_pair_id')).otherwise(col('pair_id')))
    #     full_df=full_df.withColumn('final_type',when(isnull(col('source_type')),col('match_source_type')).otherwise(col('source_type')))
    #     full_df=full_df.withColumn('final_score',when(isnull(col('score')),col('match_score')).otherwise(col('score')))
    #     full_df.printSchema()
    #     print('------------ful',full_df.count())
    #     full_df.show()
    #     full_df.write.format("parquet").mode("overwrite").save('s3://mdp-ut.east1/aggregated/',header = 'true')
    #     return full_df


# ----- exploded to map
# import pyspark.sql.functions as F

# df1 = df.groupby("id", "cat").count()
# df2 = df1.groupby("id")\
#          .agg(F.map_from_entries(F.collect_list(F.struct("cat","count"))).alias("cat"))


# ------ map to exploded
# df.select(df.name,explode(df.properties)).show()

#  union dont drop duplicates

