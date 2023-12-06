from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
import bungee_utils.spark_utils.function.dataframe_util as utils


schema = StructType([
    StructField("pair_id", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("base_sku_uuid", StringType(), True),
    StructField("comp_sku_uuid", StringType(), True),
    StructField("base_source_store", StringType(), True),
    StructField("comp_source_store", StringType(), True),
    StructField("match_source_score_map", MapType(StringType(), DoubleType()), True),
    StructField("aggregated_score", DoubleType(), True),
    StructField("bungee_audit_status", StringType(), True), # un_AUDITED, PRUNED, exact, similar, not_a_match
    StructField("misc_info", StringType(), True),
    StructField("bungee_auditor", StringType(), True), # job_name (pruned during offline) or auditor_name also pruning online
    StructField("bungee_audit_date", TimestampType(), True),
    StructField("bungee_auditor_comment", StringType(), True),
    StructField("seller_type", StringType(), True),
    StructField("client_audit_status_l1", StringType(), True),
    StructField("client_auditor_l1", StringType(), True),
    StructField("client_audit_date_l1", TimestampType(), True),
    StructField("client_auditor_l1_comment", StringType(), True),
    StructField("client_audit_status_l2", StringType(), True),
    StructField("client_auditor_l2", StringType(), True),
    StructField("client_audit_date_l2", TimestampType(), True),
    StructField("client_auditor_l2_comment", StringType(), True),
    StructField("created_date", TimestampType(), True),
    StructField("created_by", StringType(), True),
    StructField("updated_date", TimestampType(), True),
    StructField("updated_by", StringType(), True),
])

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
    

def add_prefix_to_column_name( df: DataFrame, prefix: str, columns_to_prefix: list):
    df_prefixed = df.select( *[col(column).alias(prefix + column) if column in columns_to_prefix else col(column) for column in df.columns])
    return df_prefixed
class Aggregator:
    def __init__(self, ml_match_suggestion: DataFrame, upc_match_suggestion: DataFrame, mpn_match_suggestion: DataFrame, transitive_match_suggestion: DataFrame, mdw: DataFrame) -> None:
        self.cols_to_rename = ["score", "segment", "seller_flag"]
        self.ml_match_suggestion = ml_match_suggestion
        self.upc_match_suggestion = upc_match_suggestion
        self.mpn_match_suggestion = mpn_match_suggestion
        self.transitive_match_suggestion = transitive_match_suggestion
        
        match_suggestion_list = [ml_match_suggestion, upc_match_suggestion, mpn_match_suggestion, transitive_match_suggestion]
        match_suggestion_list = [item for item in match_suggestion_list if item is not None]
        merged_match_suggestion = utils.combine_dfs(match_suggestion_list)
        self.merged_match_suggestion = add_prefix_to_column_name(merged_match_suggestion, "new_", self.cols_to_rename)
        self.mdw = mdw

    def aggregate_match_suggestion_and_mdw(self, mdw: DataFrame, merged_match_suggestion: DataFrame):
        # cols = set(mdw.columns).difference(set(["source_score_map"]))
        merged_matches = None
        if mdw is not None:
            prefixed_mdw = add_prefix_to_column_name(mdw, "old_",  self.cols_to_rename)
            mdw = prefixed_mdw.select( "*", explode(col("match_source")).alias("source", "score")).drop("match_source")
            mdw.show()
            merged_matches = merged_match_suggestion.join(mdw, ["pair_id", "source"], "full_outer")
        else:
            merged_matches = merged_match_suggestion.withColumn("old_score", lit(None))
        
        mdw.cache()
        merged_match_suggestion.cache()
        score_diff = col("old_score") - col("new_score")
            
        merged_matches = merged_matches.withColumn( "score", 
                                                    when( col("old_score").isNull(), col("new_score") )\
                                                    .when( col("new_score").isNull(), col("old_score") )\
                                                    .when( (score_diff.between(-0.05, 0.05)) , col("old_score") )\
                                                    .otherwise( col("new_score") ) 
                                                )\
                                        .withColumn( "segment", 
                                                    when( col("old_segment").isNull(), col("new_segment") )\
                                                    .otherwise(  col("old_segment") )
                                                ).drop("old_segment", "new_segment")\
                                        .withColumn( "seller_flag", 
                                                    when( col("old_seller_flag").isNull(), col("new_seller_flag") )\
                                                    .otherwise(  col("old_seller_flag") )
                                                ).drop("old_seller_flag", "new_seller_flag")
        merged_matches = merged_matches.withColumn("match_status", when(col("old_score").isNull(), lit("new"))\
                                                                    .when( col("old_score").isNotNull() & (col("old_score") != col("score") ), lit("updated") )\
                                                                    .otherwise( lit("not_updated") ) 
                                                    )
        return merged_matches
    
    def fetch_updated_and_new_matches_suggestions(self, merged_match_suggestion: DataFrame):
        
        merged_match_suggestion = merged_match_suggestion.groupBy("pair_id", "segment", "base_sku_uuid", "comp_sku_uuid", "base_source_store", "comp_source_store", "seller_type").\
                                        agg( map_from_entries( collect_list( struct("source","score"))).alias("match_source"), 
                                            collect_set("match_status").alias("match_status") , sum(col("score")).alias("score"), collect_set("match_status_col_name").alias("match_status_col_name"))
        
        unaudited_match_suggestion = merged_match_suggestion.filter(~(array_contains(col("match_status_col_name"), "audited"))) # remove audited match
        new_matches_suggestion = unaudited_match_suggestion.filter( (size(col("match_status")) == 1) & array_contains(col("match_status"), "new") ) # remove non updated match                                                             
        updated_matches_suggestion = unaudited_match_suggestion.filter(array_contains(col("match_status"), "updated") | (array_contains(col("match_status"), "not_updated") & array_contains(col("match_status"), "new") )) # remove non updated match  
        
        return updated_matches_suggestion, new_matches_suggestion
    
    def convert_schema_for_updated_match_suggestion(self, updated_matches_suggestion:DataFrame, mdw: DataFrame):
        updated_matches_suggestion_cols = ["score", "match_source", "updated_date", "updated_by", "match_status_col_name"]
        mdw_cols = set(mdw.columns).difference(updated_matches_suggestion_cols)
        
        updated_matches_suggestion = updated_matches_suggestion.withColumn( "match_status_col_name", lit("unaudited") )\
                                                                .withColumn( "updated_date", current_timestamp() )\
                                                                .withColumn( "updated_by", lit("match_management_system") )
                                                                
        
        updated_matches_suggestion = updated_matches_suggestion.select(["pair_id"] + updated_matches_suggestion_cols)
        mdw = mdw.select(mdw_cols)
        updated_matches_suggestion = updated_matches_suggestion.join(mdw, "pair_id", "inner")
        return updated_matches_suggestion
                                                                
    def convert_schema_for_new_match_suggestion(self, new_matches_suggestion:DataFrame):
        new_matches_suggestion = new_matches_suggestion.select("")
        new_matches_suggestion = new_matches_suggestion.withColumn( "match_status_col_name", lit("unaudited") )\
                                                        .withColumn( "created_date", current_timestamp() )\
                                                        .withColumn( "created_by", lit("match_management_system") )\
                                                        .withColumn( "updated_date", current_timestamp() )\
                                                        .withColumn( "updated_by", lit("match_management_system") )\
                                                        .withColumn( "match_id", lit("") )\
                                                        .withColumn("bungee_audit_status", lit(""))\
                                                        .withColumn("bungee_auditor", lit(""))\
                                                        .withColumn("bungee_audit_date", lit(""))\
                                                        .withColumn("bungee_auditor_comment", lit(""))\
                                                        .withColumn("client_audit_status_l1", lit(""))\
                                                        .withColumn("client_auditor_l1", lit(""))\
                                                        .withColumn("client_audit_date_l1", lit(""))\
                                                        .withColumn("client_auditor_l1_comment", lit(""))\
                                                        .withColumn("client_audit_status_l2", lit(""))\
                                                        .withColumn("client_auditor_l2", lit(""))\
                                                        .withColumn("client_audit_date_l2", lit(""))\
                                                        .withColumn("client_auditor_l2_comment", lit(""))\
                                                        .withColumn("misc_info", lit(""))\
                                                        .withColumn("eviction_type", lit(""))\
                                                        .withColumn("eviction_reason", lit(""))\
                                                        .withColumn("year", lit(""))\
                                                        .withColumn("month", lit(""))\
                                                        .withColumn("date", lit(""))\
                                                        .withColumn("active_status", lit(""))
        return new_matches_suggestion

    def aggregate_matches(self):
        merged_match_suggestion = self.aggregate_match_suggestion_and_mdw(self.mdw, self.merged_match_suggestion)
        merged_match_suggestion.cache()
        updated_matches_suggestion, new_matches_suggestion = self.fetch_updated_and_new_matches_suggestions(merged_match_suggestion)
        updated_matches_suggestion.cache()
        new_matches_suggestion.cache()
        updated_matches_suggestion = self.convert_schema_for_updated_match_suggestion(updated_matches_suggestion)
        new_matches_suggestion = self.convert_schema_for_new_match_suggestion(new_matches_suggestion)
        updated_matches_suggestion.cache()
        new_matches_suggestion.cache()


# ----- exploded to map
# import pyspark.sql.functions as F

# df1 = df.groupby("id", "cat").count()
# df2 = df1.groupby("id")\
#          .agg(F.map_from_entries(F.collect_list(F.struct("cat","count"))).alias("cat"))


# ------ map to exploded
# df.select(df.name,explode(df.properties)).show()

#  union dont drop duplicates

