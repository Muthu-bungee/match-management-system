from pyspark.sql.dataframe import DataFrame
from bungee_utils.spark_utils.function.dataframe_util import *
from pyspark.sql.window import Window
from mdp_common_utils.schema import * 
from mdp_common_utils.constants import *

class MatchMerger:
    def __init__(self, args: dict, bungee_audit_matches : DataFrame, customer_audit_matches: DataFrame ) -> None:
        self.args = args
        self.bungee_audit_matches = bungee_audit_matches
        self.customer_audit_matches = customer_audit_matches  

    def merge_matches(self, bungee_audit_matches: DataFrame, customer_audit_matches: DataFrame):
        # so we need to convert undirected to directed 
        merged_audited_matches = combine_dfs([bungee_audit_matches, customer_audit_matches])
        window_spec = Window.partitionBy(MATCH_WAREHOUSE.PAIR_ID.NAME).orderBy("priority")
        merged_audited_matches = merged_audited_matches.select("*", \
                                             collect_set(col(MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME)).over(window_spec).alias("audit_status_list"), \
                                             row_number().over(window_spec).alias("row_num") )
        
        merged_audited_matches = merged_audited_matches.filter(col("row_num") == 1).drop("row_num")
        merged_audited_matches = merged_audited_matches.withColumn( MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME, \
                                                                        when( size(col("audit_status_list")) > 1, lit(BUNGEE_AUDIT_STATUS.CONFLICT)) \
                                                                        .otherwise( col("audit_status_list").getItem(0) )
                                                                   )
        return merged_audited_matches
