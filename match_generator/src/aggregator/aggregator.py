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
    StructField("bungee_audit_status", StringType(), True), # un_AUDITED, PRUNED, exact, similar, not_a_match, unsure, conflict, deactivated
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


def add_prefix_to_column_name( df: DataFrame, prefix: str, columns_to_prefix: list):
    df_prefixed = df.select( *[col(column).alias(prefix + column) if column in columns_to_prefix else col(column) for column in df.columns])
    return df_prefixed
class Aggregator:
    def __init__(self, ml_match_suggestion: DataFrame, upc_match_suggestion: DataFrame, mpn_match_suggestion: DataFrame, transitive_match_suggestion: DataFrame, mdw: DataFrame) -> None:
        self.cols_to_rename = ["score", "segment", "seller_type", "base_source_store","comp_source_store"]
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
            mdw = mdw.select( "*", explode(col("match_source_score_map")).alias("source", "score")).drop("match_source")
            prefixed_mdw = add_prefix_to_column_name(mdw, "old_",  self.cols_to_rename)
            merged_matches = merged_match_suggestion.join(prefixed_mdw, ["pair_id", "source"], "full_outer")
        else:
            merged_matches = merged_match_suggestion.withColumn("old_score", lit(None))
        merged_matches.filter(col("pair_id") == "00003463186541<>100341666<>homedepot<>homedepot_0003463186541<>109589<>hdsupplysolutions<>hdsupplysolutions").show()
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
                                        .withColumn( "seller_type", 
                                                    when( col("old_seller_type").isNull(), col("new_seller_type") )\
                                                    .otherwise(  col("old_seller_type") )
                                                ).drop("old_seller_type", "new_seller_type")\
                                        .withColumn( "base_sku_uuid", 
                                                    when( col("base_sku_uuid").isNull(), col("sku_uuid_a") )\
                                                    .otherwise(  col("base_sku_uuid") )
                                                ).drop("sku_uuid_a")\
                                        .withColumn( "comp_sku_uuid", 
                                                    when( col("comp_sku_uuid").isNull(), col("sku_uuid_b") )\
                                                    .otherwise(  col("comp_sku_uuid") )
                                                ).drop("sku_uuid_b")\
                                        .withColumn( "base_source_store", 
                                                    when( col("old_base_source_store").isNull(), col("new_base_source_store") )\
                                                    .otherwise(  col("old_base_source_store") )
                                                ).drop("old_base_source_store","new_base_source_store")\
                                        .withColumn( "comp_source_store", 
                                                    when( col("old_comp_source_store").isNull(), col("new_comp_source_store") )\
                                                    .otherwise(  col("old_comp_source_store") )
                                                ).drop("old_comp_source_store","new_comp_source_store")
                                        
        merged_matches = merged_matches.withColumn("match_status", when(col("old_score").isNull(), lit("new"))\
                                                                    .when( col("old_score").isNotNull() & (col("old_score") != col("score") ), lit("updated") )\
                                                                    .otherwise( lit("not_updated") ) 
                                                    )
        return merged_matches
    
    def fetch_updated_and_new_matches_suggestions(self, merged_match_suggestion: DataFrame):
        # "created_date", "created_by"
        merged_match_suggestion = merged_match_suggestion.groupBy("pair_id", "segment", "base_sku_uuid", "comp_sku_uuid", "base_source_store", "comp_source_store", "seller_type" ).\
                                        agg( map_from_entries( collect_list( struct("source","score"))).alias("match_source_score_map"), 
                                            collect_set("match_status").alias("match_status") , sum(col("score")).alias("aggregated_score"))
        
        unaudited_match_suggestion = merged_match_suggestion.filter(~(array_contains(col("match_status"), "audited"))) # remove audited match
        new_matches_suggestion = unaudited_match_suggestion.filter( (size(col("match_status")) == 1) & array_contains(col("match_status"), "new") ) # remove non updated match                                                             
        updated_matches_suggestion = unaudited_match_suggestion.filter(array_contains(col("match_status"), "updated") | (array_contains(col("match_status"), "not_updated") & array_contains(col("match_status"), "new") )) # remove non updated match  
        
        return updated_matches_suggestion, new_matches_suggestion
    
    def convert_schema_for_updated_match_suggestion(self, updated_matches_suggestion:DataFrame, mdw: DataFrame):
        updated_matches_suggestion = updated_matches_suggestion.select("pair_id","segment","base_sku_uuid","comp_sku_uuid","match_source_score_map","aggregated_score", "created_date", "created_by")
        updated_matches_suggestion = updated_matches_suggestion.withColumn("updated_date", current_timestamp() )\
                                                        .withColumn("updated_by", lit("match_management_system") )\
                                                        .withColumn("bungee_audit_status", lit("un_audited"))\
                                                        .withColumn("bungee_auditor", lit(None))\
                                                        .withColumn("bungee_audit_date", lit(None))\
                                                        .withColumn("bungee_auditor_comment", lit(None))\
                                                        .withColumn("client_audit_status_l1", lit(None))\
                                                        .withColumn("client_auditor_l1", lit(None))\
                                                        .withColumn("client_audit_date_l1", lit(None))\
                                                        .withColumn("client_auditor_l1_comment", lit(None))\
                                                        .withColumn("client_audit_status_l2", lit(None))\
                                                        .withColumn("client_auditor_l2", lit(None))\
                                                        .withColumn("client_audit_date_l2", lit(None))\
                                                        .withColumn("client_auditor_l2_comment", lit(None))\
                                                        .withColumn("misc_info", lit(None))
                                                        
        return updated_matches_suggestion
                                                                
    def convert_schema_for_new_match_suggestion(self, new_matches_suggestion:DataFrame):
        # "base_source_store","comp_source_store",
        new_matches_suggestion = new_matches_suggestion.select("pair_id","segment","base_sku_uuid","comp_sku_uuid","match_source_score_map","aggregated_score")
        new_matches_suggestion = new_matches_suggestion.withColumn("created_date", current_timestamp() )\
                                                        .withColumn("created_by", lit("match_management_system") )\
                                                        .withColumn("updated_date", lit(None) )\
                                                        .withColumn("updated_by", lit(None) )\
                                                        .withColumn("bungee_audit_status", lit("un_audited"))\
                                                        .withColumn("bungee_auditor", lit(None))\
                                                        .withColumn("bungee_audit_date", lit(None))\
                                                        .withColumn("bungee_auditor_comment", lit(None))\
                                                        .withColumn("client_audit_status_l1", lit(None))\
                                                        .withColumn("client_auditor_l1", lit(None))\
                                                        .withColumn("client_audit_date_l1", lit(None))\
                                                        .withColumn("client_auditor_l1_comment", lit(None))\
                                                        .withColumn("client_audit_status_l2", lit(None))\
                                                        .withColumn("client_auditor_l2", lit(None))\
                                                        .withColumn("client_audit_date_l2", lit(None))\
                                                        .withColumn("client_auditor_l2_comment", lit(None))\
                                                        .withColumn("misc_info", lit(None))
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

