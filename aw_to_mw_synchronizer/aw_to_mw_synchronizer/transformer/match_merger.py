from pyspark.sql.dataframe import DataFrame
from bungee_utils.spark_utils.function.dataframe_util import *
from pyspark.sql.window import Window
from mdp_common_utils.schema import * 
from mdp_common_utils.constants import *

class MatchMerger:
    def __init__(self, env: str, args: dict, bungee_audit_matches : DataFrame, customer_audit_matches: DataFrame, match_warehouse : DataFrame ) -> None:
        self.args = args
        self.bungee_audit_matches = bungee_audit_matches
        self.customer_audit_matches = customer_audit_matches
        self.match_warehouse = match_warehouse
        self.env = env
    
    def _remove_non_updated_matches(self, match_warehouse: DataFrame, merged_audited_matches: DataFrame):
        
        merged_audited_matches = merged_audited_matches.select("*", explode(col(MATCH_WAREHOUSE.MATCH_SOURCE_SCORE_MAP.NAME)).alias("source", "score"))
        match_warehouse = match_warehouse.select("*", explode(col(MATCH_WAREHOUSE.MATCH_SOURCE_SCORE_MAP.NAME)).alias("source", "score"))

        all_col_list = get_column_list(MATCH_WAREHOUSE) + ["source", "score"]
        non_essential_column = [MATCH_WAREHOUSE.CREATED_DATE.NAME, MATCH_WAREHOUSE.CREATED_BY.NAME, MATCH_WAREHOUSE.UPDATED_DATE.NAME, MATCH_WAREHOUSE.UPDATED_BY.NAME, MATCH_WAREHOUSE.MATCH_SOURCE_SCORE_MAP.NAME]
        essential_column = list(set(all_col_list).difference(set(non_essential_column)))
        
        updated_merged_audited_matches = merged_audited_matches.join(match_warehouse, essential_column, "left_anti")
        updated_merged_audited_matches = updated_merged_audited_matches.drop_duplicates(["pair_id"]).drop("source", "score")
        return updated_merged_audited_matches
      
    def _convert_to_direct_pair_matches(self, undirected_matches:DataFrame):
        print("converting undirected to directed pairs")
        match_source_to_dest = undirected_matches
        match_dest_to_source = undirected_matches.withColumnRenamed(MATCH_WAREHOUSE.BASE_SKU_UUID.NAME, "temp_sku_uuid")\
                                                .withColumnRenamed(MATCH_WAREHOUSE.COMP_SKU_UUID.NAME, MATCH_WAREHOUSE.BASE_SKU_UUID.NAME)\
                                                .withColumnRenamed("temp_sku_uuid", MATCH_WAREHOUSE.COMP_SKU_UUID.NAME)\
                                                .withColumnRenamed(MATCH_WAREHOUSE.BASE_SOURCE_STORE.NAME, "temp_source_store")\
                                                .withColumnRenamed(MATCH_WAREHOUSE.COMP_SOURCE_STORE.NAME, MATCH_WAREHOUSE.BASE_SOURCE_STORE.NAME)\
                                                .withColumnRenamed("temp_source_store", MATCH_WAREHOUSE.COMP_SOURCE_STORE.NAME)\
                                                .withColumn(MATCH_WAREHOUSE.PAIR_ID.NAME, concat_ws("_", col("base_sku_uuid"), col("comp_sku_uuid")))\
                                                .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L1.NAME, lit(None) ) \
                                                .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L1.NAME, lit(None) ) \
                                                .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_DATE_L1.NAME, lit(None) ) \
                                                .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L2.NAME, lit(None) ) \
                                                .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L2.NAME, lit(None) ) \
                                                .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_DATE_L2.NAME, lit(None) ) \
                                                .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L2_COMMENT.NAME, lit(None))                                                
        directed_matches = combine_dfs([match_source_to_dest, match_dest_to_source]).dropDuplicates(["pair_id"])
        
        if self.env != "prod":
            print("undirected matches count ", undirected_matches.count())
            undirected_matches.show(truncate = False, n = 100)
            print("directed matches count ", directed_matches.count())
            directed_matches.show(truncate = False, n = 100)
        print("Converted undirected to directed pairs")
        return directed_matches

    def merge_matches(self):
        # so we need to convert undirected to directed 
        merged_audited_matches = combine_dfs([self.bungee_audit_matches, self.customer_audit_matches])
        window_spec = Window.partitionBy(MATCH_WAREHOUSE.PAIR_ID.NAME).orderBy("priority")
        merged_audited_matches = merged_audited_matches.select("*", \
                                             collect_set(col(MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME)).over(window_spec).alias("audit_status_list"), \
                                             row_number().over(window_spec).alias("row_num") )
        
        #  checking for multiple bungee audit status
        merged_audited_matches = merged_audited_matches.filter(col("row_num") == 1).drop("row_num")
        merged_audited_matches = self._convert_to_direct_pair_matches(merged_audited_matches)
        merged_audited_matches = merged_audited_matches.withColumn( MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME, \
                                                                        when( size(col("audit_status_list")) > 1, lit(BUNGEE_AUDIT_STATUS.CONFLICT)) \
                                                                        .otherwise( col("audit_status_list").getItem(0) ) )\
                                                                        .drop("audit_status_list")
        
        #  checking for multiple audit status eg bungee and customer
        merged_audited_matches = merged_audited_matches.withColumn( "audit_status_list", array_distinct(array(MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME, MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L1.NAME, MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L2.NAME)) )
        merged_audited_matches = merged_audited_matches.withColumn( "audit_status_list", expr("FILTER(audit_status_list, value -> value IS NOT NULL)"))
        merged_audited_matches = merged_audited_matches.withColumn( "audit_status_list_length", size(col("audit_status_list")) )
        
        merged_audited_matches = merged_audited_matches.withColumn( MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME, \
                                                                        when( col("audit_status_list_length") > 1, lit(BUNGEE_AUDIT_STATUS.CONFLICT)) \
                                                                        .otherwise( col(MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME) ) )\
                                                                        .drop("audit_status_list", "audit_status_list_length")
        # ask senthil if this can be considered as conflict
        # conflict_condition = ( ( col(MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME).isNotNull() & col(MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L1.NAME).isNotNull() & (col(MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME) != col(MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L1.NAME)) ) | 
        #                       ( col(MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME).isNotNull() & col(MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L2.NAME).isNotNull() & (col(MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME) != col(MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L2.NAME)) ) |
        #                       ( col(MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L1.NAME).isNotNull() & col(MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L2.NAME).isNotNull() & (col(MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L1.NAME) != col(MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L2.NAME)) ) )
        
        # merged_audited_matches = merged_audited_matches.withColumn( MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME, \
        #                                                                 when( conflict_condition, lit(BUNGEE_AUDIT_STATUS.CONFLICT)) \
        #                                                                 .otherwise( col(MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME) ) )
        updated_merged_audited_matches = self._remove_non_updated_matches(self.match_warehouse, merged_audited_matches)
        
        return updated_merged_audited_matches
    
