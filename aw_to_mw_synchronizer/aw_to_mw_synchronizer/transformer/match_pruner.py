from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql import DataFrame
from bungee_utils.spark_utils.function.dataframe_util import *
from pyspark.sql.window import Window
from mdp_common_utils.schema import *


AUDITED_MATCH_SELLER_TYPE = "audited_match_seller_type"
AUDITED_MATCH_SELLER_TYPE_LIST = "audited_match_seller_type_list"

class MatchPruner:
    def __init__(self, client_config:dict, unaudited_matches: DataFrame, audited_matches: DataFrame) -> None:
        self.client_config = client_config
        self.unaudited_matches = unaudited_matches
        self.audited_matches = audited_matches
        self.join_columns = [MATCH_WAREHOUSE.BASE_SKU_UUID.NAME, MATCH_WAREHOUSE.COMP_SOURCE_STORE.NAME]
        
    def prune_matches(self):
        similar_matches, exact_matches, unaudited_matches = self._separate_audited_and_unaudited_matches(self.unaudited_matches, self.audited_matches)

    def _separate_audited_and_unaudited_matches(self, unaudited_matches:DataFrame, audited_matches:DataFrame):
        print("separate_audited_and_unaudited_matches")
        window_spec = Window.partitionBy("base_sku_uuid", "comp_source_store")
        # fetching similar matches from audited matches
        similar_matches = audited_matches.filter(col('bungee_audit_status').isin(['equivalent', 'similar']))
        similar_matches = similar_matches.withColumn(AUDITED_MATCH_SELLER_TYPE_LIST, concat_ws( "_", array_sort( collect_set(col("seller_type")).over(window_spec) ) ) )
        similar_matches = similar_matches.withColumn("misc_info", concat(lit("similar_match_"), col("pair_id")) ).\
                        select("base_sku_uuid", "comp_source_store", "audited_match_seller_type_list", "misc_info", col("seller_type").alias(AUDITED_MATCH_SELLER_TYPE))
        
        # fetching exact matches from audited matches      
        exact_matches = audited_matches.filter(col('bungee_audit_status').isin(['exact']))
        exact_matches = exact_matches.withColumn("audited_match_seller_type_list", concat_ws("_", array_sort( collect_set(col("seller_type")).over(window_spec) ) ) )
        exact_matches = exact_matches.withColumn("misc_info", concat(lit("exact_match_"), col("pair_id")) ).\
                        select("base_sku_uuid", "comp_source_store", "audited_match_seller_type_list", "misc_info", col("seller_type").alias(AUDITED_MATCH_SELLER_TYPE))
        
        # remove all audited matches from unaudited_matches
        unaudited_matches = unaudited_matches.join(audited_matches.select("pair_id"), 'pair_id', 'left_anti').drop("misc_info")
        
        return similar_matches, exact_matches, unaudited_matches
        
    def _prune_match_based_on_client_config(self, similar_matches:DataFrame , exact_matches:DataFrame , unaudited_match:DataFrame ):
        print("Match Pruning Started")
        pruned_suggestion = None
        if self.config['match_type'] == 'exact' :
            pruned_suggestion = self._prune_for_exact_match_config(exact_matches, unaudited_match) 
            
        elif self.config['match_type'] == 'exact_or_similar':
            exact_and_similar_match = exact_matches.union(similar_matches)
            pruned_suggestion = self._prune_for_exact_similar_match(exact_and_similar_match, unaudited_match)
            
        elif self.config['match_type'] == 'exact_over_similar':
            exact_and_similar_match = exact_matches.union(similar_matches)
            pruned_suggestion = self._prune_for_exact_over_similar_match(exact_and_similar_match, unaudited_match)
            
        if pruned_suggestion is not None:
            pruned_suggestion = pruned_suggestion.withColumn("bungee_audit_status", lit("PRUNED"))
            pruned_suggestion = pruned_suggestion.withColumn("bungee_auditor", lit("match_management_system"))
            pruned_suggestion = pruned_suggestion.withColumn("updated_by", lit("match_management_system"))
            pruned_suggestion = pruned_suggestion.withColumn("updated_date", current_timestamp().cast("date"))
        
        return pruned_suggestion
    
    def _prune_for_exact_match_config(self, exact_matches: DataFrame, unaudited_matches:DataFrame):
        
        if self.client_config["seller_type"] == "1p":
            if self.client_config["cardinality"] == "1":
                pruned_suggestion = unaudited_matches.join( exact_matches.filter( col(AUDITED_MATCH_SELLER_TYPE) == '1p'), self.join_columns, "inner" )
                return pruned_suggestion 
            elif self.client_config["cardinality"] == "n":
                pruned_suggestion = unaudited_matches.join( exact_matches.filter( col(AUDITED_MATCH_SELLER_TYPE) == '1p'), self.join_columns, "inner" )
                return pruned_suggestion 
        elif self.client_config["seller_type"] == "3p":
            if self.client_config["cardinality"] == "1":
                pruned_suggestion = unaudited_matches.join( exact_matches.filter( col(AUDITED_MATCH_SELLER_TYPE) == '3p'), self.join_columns, "inner" )
                return pruned_suggestion 
            elif self.client_config["cardinality"] == "n":
                pruned_suggestion = None
                return pruned_suggestion
        elif self.client_config["seller_type"] == "1p_or_3p":
            if self.client_config["cardinality"] == "1":
                pruned_suggestion = unaudited_matches.join( exact_matches, self.join_columns, "inner") 
                return pruned_suggestion
            elif self.client_config["cardinality"] == "n":
                pruned_suggestion = unaudited_matches.join( exact_matches.filter( col(AUDITED_MATCH_SELLER_TYPE) == '1p'), self.join_columns, "inner" )
                return pruned_suggestion
        elif self.client_config["seller_type"] == "1p_over_3p":
            if self.client_config["cardinality"] == "1":
                pruned_suggestion = unaudited_matches.join( exact_matches, self.join_columns, "inner" )
                pruned_suggestion = pruned_suggestion.filter( (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == col(AUDITED_MATCH_SELLER_TYPE)) | ( (col(AUDITED_MATCH_SELLER_TYPE)=='1p') & (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME)=='3p')  ) ) 
                return pruned_suggestion
            elif self.client_config["cardinality"] == "n":
                pruned_suggestion = unaudited_matches.join( exact_matches.filter( col(AUDITED_MATCH_SELLER_TYPE) == '1p'), self.join_columns, "inner" )
                return pruned_suggestion

    def _prune_for_exact_similar_match(self, exact_and_similar_match: DataFrame, unaudited_matches:DataFrame):
        if self.client_config["seller_type"] == "1p":
            if self.client_config["cardinality"] == "1":
                pruned_suggestion = unaudited_matches.join( exact_and_similar_match.filter( col(AUDITED_MATCH_SELLER_TYPE) == '1p'), self.join_columns, "inner" )
                return pruned_suggestion 
            elif self.client_config["cardinality"] == "n":
                pruned_suggestion = None
                return pruned_suggestion 
        elif self.client_config["seller_type"] == "3p":
            if self.client_config["cardinality"] == "1":
                pruned_suggestion = unaudited_matches.join( exact_and_similar_match.filter( col(AUDITED_MATCH_SELLER_TYPE) == '3p'), self.join_columns, "inner" )
                return pruned_suggestion 
            elif self.client_config["cardinality"] == "n":
                pruned_suggestion = None
                return pruned_suggestion
        elif self.client_config["seller_type"] == "1p_or_3p":
            if self.client_config["cardinality"] == "1":
                pruned_suggestion = unaudited_matches.join( exact_and_similar_match, self.join_columns, "inner" ) 
                return pruned_suggestion
            elif self.client_config["cardinality"] == "n":
                pruned_suggestion = None
                return pruned_suggestion
        elif self.client_config["seller_type"] == "1p_over_3p":
            if self.client_config["cardinality"] == "1":
                pruned_suggestion = unaudited_matches.join( exact_and_similar_match, self.join_columns, "inner" )
                pruned_suggestion = pruned_suggestion.filter( (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == col(AUDITED_MATCH_SELLER_TYPE)) | ( (col(AUDITED_MATCH_SELLER_TYPE)=='1p') & (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME)=='3p')  ) ) 
                return pruned_suggestion
            elif self.client_config["cardinality"] == "n":
                pruned_suggestion = None 
                return pruned_suggestion

    def _prune_for_exact_over_similar_match(self, exact_and_similar_match: DataFrame, audited_base_product_unaudited_matches:DataFrame):
        if self.client_config["seller_type"] == "1p":
            if self.client_config["cardinality"] == "1":
                return
            elif self.client_config["cardinality"] == "n":
                return
        elif self.client_config["seller_type"] == "3p":
            if self.client_config["cardinality"] == "1":
                return
            elif self.client_config["cardinality"] == "n":
                return
        elif self.client_config["seller_type"] == "1p_or_3p":
            if self.client_config["cardinality"] == "1":
                return
            elif self.client_config["cardinality"] == "n":
                return
        elif self.client_config["seller_type"] == "1p_over_3p":
            if self.client_config["cardinality"] == "1":
                return
            elif self.client_config["cardinality"] == "n":
                return 

   
   
   