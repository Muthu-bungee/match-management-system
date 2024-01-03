from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql import DataFrame
from bungee_utils.spark_utils.function.dataframe_util import *
from pyspark.sql.window import Window
from mdp_common_utils.schema import *
from mdp_common_utils.constants import *

AUDITED_MATCH_SELLER_TYPE_LIST = "audited_match_seller_type_list"
AUDITED_MATCH_SELLER_TYPE = "audited_match_seller_type"
SELLER_TYPE = 'seller_type'
CARDINALITY = 'cardinality'
MATCH_TYPE = 'match_type'

class MatchPruner:
    def __init__(self, mdw: DataFrame, non_pruned_match_suggestion: DataFrame, config: dict, env: str) -> None:
        self.mdw = mdw
        self.non_pruned_match_suggestion = non_pruned_match_suggestion
        self.config = config
        self.env = env
        self.join_columns = [MATCH_WAREHOUSE.BASE_SKU_UUID.NAME, MATCH_WAREHOUSE.COMP_SOURCE_STORE.NAME]

    def prune_matches(self):
        self.mdw, non_pruned_match_suggestion = self.preprocessing_matches(self.mdw, self.non_pruned_match_suggestion)
        similar_matches, exact_matches, unaudited_matches = self.separate_audited_and_unaudited_matches(non_pruned_match_suggestion, self.mdw)
        if self.env != "prod":
            print("similar_matches")
            similar_matches.show(truncate = False)
            print("exact_matches")
            exact_matches.show(truncate = False)
            print("unaudited_matches")
            unaudited_matches.show(truncate = False)
        self.pruned_suggestion, self.match_suggestion = self.prune_match_based_on_client_config(similar_matches, exact_matches, unaudited_matches)
        updated_match_suggestion = combine_dfs([self.pruned_suggestion, self.match_suggestion]).drop(AUDITED_MATCH_SELLER_TYPE_LIST, AUDITED_MATCH_SELLER_TYPE)
        updated_match_suggestion.cache()
        
        if self.env != "prod":
            if updated_match_suggestion.count() == unaudited_matches.count():
                print("matches are not missing")
            elif updated_match_suggestion.count() < unaudited_matches.count(): 
                print("matches are missing")
            else :
                print("duplicate matches are present")
            if self.pruned_suggestion is not None:
                print("pruned_suggestion")
                self.pruned_suggestion.show(truncate = False)
            if self.match_suggestion is not None:
                print("match_suggestion")
                self.match_suggestion.show(truncate = False)
        return updated_match_suggestion
    
    def fetch_pruned_matches(self):
        return self.pruned_suggestion

    def fetch_match_suggestions(self):
        return self.match_suggestion

    def preprocessing_matches(self, mdw: DataFrame, match_suggestion: DataFrame):
        match_suggestion = match_suggestion.withColumn(MATCH_WAREHOUSE.SELLER_TYPE.NAME, lower(col(MATCH_WAREHOUSE.SELLER_TYPE.NAME)))
        mdw = mdw.withColumn(MATCH_WAREHOUSE.SELLER_TYPE.NAME, lower(col(MATCH_WAREHOUSE.SELLER_TYPE.NAME)))\
            .filter( col( MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME).isin([BUNGEE_AUDIT_STATUS.EXACT_MATCH, BUNGEE_AUDIT_STATUS.SIMILAR_MATCH, BUNGEE_AUDIT_STATUS.NOT_MATCH])  )\
            .withColumn( MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME, lower(col(MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME)) )

        return mdw, match_suggestion
    
    def separate_audited_and_unaudited_matches(self, match_suggestion:DataFrame, mdw:DataFrame):
        print("separate_audited_and_unaudited_matches")
        window_spec = Window.partitionBy(MATCH_WAREHOUSE.BASE_SKU_UUID.NAME, MATCH_WAREHOUSE.COMP_SOURCE_STORE.NAME)
        similar_matches = mdw.filter(col(MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME).isin([BUNGEE_AUDIT_STATUS.SIMILAR_MATCH]))
        similar_matches = similar_matches.withColumn(AUDITED_MATCH_SELLER_TYPE_LIST, concat_ws( "_", array_sort( collect_set(col(MATCH_WAREHOUSE.SELLER_TYPE.NAME)).over(window_spec) ) ) )
        similar_matches = similar_matches.withColumn(MATCH_WAREHOUSE.MISC_INFO.NAME, concat(lit("similar_match_"), col(MATCH_WAREHOUSE.PAIR_ID.NAME)) ).\
                        select(MATCH_WAREHOUSE.BASE_SKU_UUID.NAME, MATCH_WAREHOUSE.COMP_SOURCE_STORE.NAME, AUDITED_MATCH_SELLER_TYPE_LIST, MATCH_WAREHOUSE.MISC_INFO.NAME, col(MATCH_WAREHOUSE.SELLER_TYPE.NAME).alias(AUDITED_MATCH_SELLER_TYPE))
               
        exact_matches = mdw.filter(col(MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME).isin([BUNGEE_AUDIT_STATUS.EXACT_MATCH]))
        exact_matches = exact_matches.withColumn(AUDITED_MATCH_SELLER_TYPE_LIST, concat_ws("_", array_sort( collect_set(col(MATCH_WAREHOUSE.SELLER_TYPE.NAME)).over(window_spec) ) ) )
        exact_matches = exact_matches.withColumn(MATCH_WAREHOUSE.MISC_INFO.NAME, concat(lit("exact_match_"), col(MATCH_WAREHOUSE.PAIR_ID.NAME)) ).\
                        select(MATCH_WAREHOUSE.BASE_SKU_UUID.NAME, MATCH_WAREHOUSE.COMP_SOURCE_STORE.NAME, AUDITED_MATCH_SELLER_TYPE_LIST, MATCH_WAREHOUSE.MISC_INFO.NAME, col(MATCH_WAREHOUSE.SELLER_TYPE.NAME).alias(AUDITED_MATCH_SELLER_TYPE))
        
        unaudited_matches = match_suggestion.join(mdw.select(MATCH_WAREHOUSE.PAIR_ID.NAME), MATCH_WAREHOUSE.PAIR_ID.NAME, 'left_anti').drop(MATCH_WAREHOUSE.MISC_INFO.NAME)
        
        return similar_matches, exact_matches, unaudited_matches

    def prune_match_based_on_client_config(self, similar_matches:DataFrame , exact_matches:DataFrame , audited_base_product_unaudited_matches:DataFrame ):
        print("Match Pruning Started")
        pruned_suggestion = None
        match_suggestion = None
        if self.config[MATCH_TYPE] == CUSTOMER_MATCH_TYPE_CONFIG.EXACT_MATCH or self.config[MATCH_TYPE] == CUSTOMER_MATCH_TYPE_CONFIG.EXACT_OVER_SIMILAR_MATCH:
            pruned_suggestion, match_suggestion = self.prune_for_exact_match_config(exact_matches, audited_base_product_unaudited_matches) 

        elif self.config[MATCH_TYPE] == CUSTOMER_MATCH_TYPE_CONFIG.SIMILAR_MATCH or self.config[MATCH_TYPE] == CUSTOMER_MATCH_TYPE_CONFIG.EXACT_OR_SIMILAR_MATCH:
            exact_and_similar_match = exact_matches.union(similar_matches)
            pruned_suggestion, match_suggestion = self.prune_for_exact_similar_match(exact_and_similar_match, audited_base_product_unaudited_matches)
            
        if pruned_suggestion is not None:
            pruned_suggestion = pruned_suggestion.withColumn(MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME, lit(BUNGEE_AUDIT_STATUS.PRUNED))
            pruned_suggestion = pruned_suggestion.withColumn(MATCH_WAREHOUSE.BUNGEE_AUDITOR.NAME, lit("match_management_system"))
            pruned_suggestion = pruned_suggestion.withColumn(MATCH_WAREHOUSE.UPDATED_BY.NAME, lit("match_management_system"))
            pruned_suggestion = pruned_suggestion.withColumn(MATCH_WAREHOUSE.UPDATED_DATE.NAME, current_timestamp().cast("date"))
            
        return pruned_suggestion, match_suggestion

    def prune_for_exact_similar_match(self, exact_and_similar_match: DataFrame, unaudited_matches: DataFrame, ):
        print("Pruning Exact Sim Matches")
        if self.config[SELLER_TYPE] == CUSTOMER_SELLER_TYPE_CONFIG._1P:
            audited_base_product_unaudited_matches = unaudited_matches.join(exact_and_similar_match.filter(col(AUDITED_MATCH_SELLER_TYPE) == '1p'), self.join_columns, "left")
            audited_base_product_unaudited_matches.show(truncate =False)
            if self.config[CARDINALITY] == '1':
                pruned_suggestion = audited_base_product_unaudited_matches.filter( col(AUDITED_MATCH_SELLER_TYPE_LIST).isNotNull() | (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '3p')).\
                                    withColumn(MATCH_WAREHOUSE.MISC_INFO.NAME, when( col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '3p', lit("seller_type_config_1p")).otherwise(col(MATCH_WAREHOUSE.MISC_INFO.NAME)) )
                match_suggestion = audited_base_product_unaudited_matches.filter( col(MATCH_WAREHOUSE.MISC_INFO.NAME).isNull() & (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '1p') )
            if self.config[CARDINALITY] == 'n':
                pruned_suggestion = audited_base_product_unaudited_matches.filter( (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '3p')).\
                                    withColumn(MATCH_WAREHOUSE.MISC_INFO.NAME, when( col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '3p', lit("seller_type_config_3p")).otherwise(col(MATCH_WAREHOUSE.MISC_INFO.NAME)) )
                match_suggestion = audited_base_product_unaudited_matches.filter( (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '1p'))
                
        elif self.config[SELLER_TYPE] == CUSTOMER_SELLER_TYPE_CONFIG._3P:
            audited_base_product_unaudited_matches = unaudited_matches.join(exact_and_similar_match.filter(col(AUDITED_MATCH_SELLER_TYPE) == '3p'), self.join_columns, "left")
            if self.config[CARDINALITY] == CUSTOMER_CARDINALITY_CONFIG._1:
                pruned_suggestion = audited_base_product_unaudited_matches.filter( col(AUDITED_MATCH_SELLER_TYPE_LIST).isNotNull() | (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '1p')).\
                                    withColumn(MATCH_WAREHOUSE.MISC_INFO.NAME, when( col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '1p', lit("seller_type_config_1p")).otherwise(col(MATCH_WAREHOUSE.MISC_INFO.NAME)) )
                match_suggestion = audited_base_product_unaudited_matches.filter( col(MATCH_WAREHOUSE.MISC_INFO.NAME).isNull() & (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '3p') )
            if self.config[CARDINALITY] == CUSTOMER_CARDINALITY_CONFIG._N:
                pruned_suggestion = audited_base_product_unaudited_matches.filter( (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '1p')).\
                                    withColumn(MATCH_WAREHOUSE.MISC_INFO.NAME, when( col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '1p', lit("seller_type_config_3p")).otherwise(col(MATCH_WAREHOUSE.MISC_INFO.NAME)) )
                match_suggestion = audited_base_product_unaudited_matches.filter( (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '3p'))
                
        elif self.config[SELLER_TYPE] == CUSTOMER_SELLER_TYPE_CONFIG._1P_OR_3P:
            exact_and_similar_match = exact_and_similar_match.withColumn(AUDITED_MATCH_SELLER_TYPE_LIST, when( col(AUDITED_MATCH_SELLER_TYPE_LIST) == "1p_3p", "1p").otherwise(col(AUDITED_MATCH_SELLER_TYPE_LIST)) ).\
                                            filter( col(AUDITED_MATCH_SELLER_TYPE_LIST) == col(AUDITED_MATCH_SELLER_TYPE))
            audited_base_product_unaudited_matches = unaudited_matches.join(exact_and_similar_match, self.join_columns, "left")
            if self.config[CARDINALITY] == CUSTOMER_CARDINALITY_CONFIG._1:
                pruned_suggestion = audited_base_product_unaudited_matches.filter(col(AUDITED_MATCH_SELLER_TYPE_LIST).isNotNull() )
                match_suggestion = audited_base_product_unaudited_matches.filter(col(AUDITED_MATCH_SELLER_TYPE_LIST).isNull() )
            if self.config[CARDINALITY] == CUSTOMER_CARDINALITY_CONFIG._N:
                pruned_suggestion = None
                match_suggestion = audited_base_product_unaudited_matches
                
        elif self.config[SELLER_TYPE] == CUSTOMER_SELLER_TYPE_CONFIG._1P_OVER_3P:
            exact_and_similar_match = exact_and_similar_match.withColumn(AUDITED_MATCH_SELLER_TYPE_LIST, when( col(AUDITED_MATCH_SELLER_TYPE_LIST) == "1p_3p", "1p").otherwise(col(AUDITED_MATCH_SELLER_TYPE_LIST)) ).\
                                            filter( col(AUDITED_MATCH_SELLER_TYPE_LIST) == col(AUDITED_MATCH_SELLER_TYPE))
            audited_base_product_unaudited_matches = unaudited_matches.join(exact_and_similar_match, self.join_columns, "left")
            audited_base_product_unaudited_matches.printSchema()
            if self.config[CARDINALITY] == CUSTOMER_CARDINALITY_CONFIG._1:
                pruned_suggestion = audited_base_product_unaudited_matches.filter( ~(col(AUDITED_MATCH_SELLER_TYPE_LIST).isNull() | ((col(AUDITED_MATCH_SELLER_TYPE_LIST) == "3p") & ( col(SELLER_TYPE) == '1p' )) ) )
                match_suggestion = audited_base_product_unaudited_matches.filter( col(AUDITED_MATCH_SELLER_TYPE_LIST).isNull() | ( (col(AUDITED_MATCH_SELLER_TYPE_LIST) == "3p") & (col(SELLER_TYPE) == '1p') )  )
            if self.config[CARDINALITY] == CUSTOMER_CARDINALITY_CONFIG._N:
                pruned_suggestion = None
                match_suggestion = audited_base_product_unaudited_matches
                
        return pruned_suggestion,match_suggestion

    def prune_for_exact_match_config(self, exact_matches: DataFrame, unaudited_match: DataFrame):
        print("Pruning Exact Matches")
        if self.config[SELLER_TYPE] == CUSTOMER_SELLER_TYPE_CONFIG._1P:
            audited_base_product_unaudited_matches = unaudited_match.join(exact_matches.filter(col(AUDITED_MATCH_SELLER_TYPE) == '1p'), self.join_columns, "left")
            pruned_suggestion = audited_base_product_unaudited_matches.filter( col(AUDITED_MATCH_SELLER_TYPE_LIST).isNotNull() | (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '3p')).\
                                    withColumn(MATCH_WAREHOUSE.MISC_INFO.NAME, when( col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '3p', lit("seller_type_config_1p")).otherwise(col(MATCH_WAREHOUSE.MISC_INFO.NAME)) )
            match_suggestion = audited_base_product_unaudited_matches.filter( col(MATCH_WAREHOUSE.MISC_INFO.NAME).isNull() & (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '1p') )
            
        elif self.config[SELLER_TYPE] == CUSTOMER_SELLER_TYPE_CONFIG._3P:
            audited_base_product_unaudited_matches = unaudited_match.join(exact_matches.filter(col(AUDITED_MATCH_SELLER_TYPE) == '3p'), self.join_columns, "left")
            if self.config[CARDINALITY] == CUSTOMER_CARDINALITY_CONFIG._1:
                pruned_suggestion = audited_base_product_unaudited_matches.filter(col(AUDITED_MATCH_SELLER_TYPE_LIST).isNotNull() | (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '1p')).\
                                    withColumn(MATCH_WAREHOUSE.MISC_INFO.NAME, when( col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '1p', lit("seller_type_config_3p")).otherwise(col(MATCH_WAREHOUSE.MISC_INFO.NAME)) )
                match_suggestion = audited_base_product_unaudited_matches.filter(col(MATCH_WAREHOUSE.MISC_INFO.NAME).isNull() & (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '3p'))
            if self.config[CARDINALITY] == CUSTOMER_CARDINALITY_CONFIG._N:
                pruned_suggestion = audited_base_product_unaudited_matches.filter(col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '1p').withColumn(MATCH_WAREHOUSE.MISC_INFO.NAME, lit("seller_type_config_3p"))
                match_suggestion = audited_base_product_unaudited_matches.filter(col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == '3p')
                
        elif self.config[SELLER_TYPE] == CUSTOMER_SELLER_TYPE_CONFIG._1P_OR_3P:
            exact_matches = exact_matches.withColumn(AUDITED_MATCH_SELLER_TYPE_LIST, when( col(AUDITED_MATCH_SELLER_TYPE_LIST) == "1p_3p", "1p").otherwise(col(AUDITED_MATCH_SELLER_TYPE_LIST)) ).\
                                            filter( col(AUDITED_MATCH_SELLER_TYPE_LIST) == col(AUDITED_MATCH_SELLER_TYPE))
            audited_base_product_unaudited_matches = unaudited_match.join(exact_matches, self.join_columns, "left")
            if self.config[CARDINALITY] == CUSTOMER_CARDINALITY_CONFIG._1:
                pruned_suggestion = audited_base_product_unaudited_matches.filter(col(AUDITED_MATCH_SELLER_TYPE_LIST).isNotNull() )
                match_suggestion = audited_base_product_unaudited_matches.filter(col(AUDITED_MATCH_SELLER_TYPE_LIST).isNull() )
            if self.config[CARDINALITY] == CUSTOMER_CARDINALITY_CONFIG._N:
                pruned_suggestion = audited_base_product_unaudited_matches.filter( col(AUDITED_MATCH_SELLER_TYPE_LIST).isNotNull() & (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == "1p" )  & (col(AUDITED_MATCH_SELLER_TYPE) == "1p" )) 
                match_suggestion = audited_base_product_unaudited_matches.filter( col(AUDITED_MATCH_SELLER_TYPE_LIST).isNull() | (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) != "1p" )  | (col(AUDITED_MATCH_SELLER_TYPE) != "1p" ) )
                
        elif self.config[SELLER_TYPE] == CUSTOMER_SELLER_TYPE_CONFIG._1P_OVER_3P:
            exact_matches = exact_matches.withColumn(AUDITED_MATCH_SELLER_TYPE_LIST, when( col(AUDITED_MATCH_SELLER_TYPE_LIST) == "1p_3p", "1p").otherwise(col(AUDITED_MATCH_SELLER_TYPE_LIST)) ).\
                                            filter( col(AUDITED_MATCH_SELLER_TYPE_LIST) == col(AUDITED_MATCH_SELLER_TYPE))
            audited_base_product_unaudited_matches = unaudited_match.join(exact_matches, self.join_columns, "left")
            if self.config[CARDINALITY] == CUSTOMER_CARDINALITY_CONFIG._1:
                pruned_suggestion = audited_base_product_unaudited_matches.filter( ~(col(AUDITED_MATCH_SELLER_TYPE_LIST).isNull() | ((col(AUDITED_MATCH_SELLER_TYPE_LIST) == "3p") & ( col(SELLER_TYPE) == '1p' )) ) )
                match_suggestion = audited_base_product_unaudited_matches.filter( col(AUDITED_MATCH_SELLER_TYPE_LIST).isNull() | ( (col(AUDITED_MATCH_SELLER_TYPE_LIST) == "3p") & (col(SELLER_TYPE) == '1p') )  )
            if self.config[CARDINALITY] == CUSTOMER_CARDINALITY_CONFIG._N:
                pruned_suggestion = audited_base_product_unaudited_matches.filter( col(AUDITED_MATCH_SELLER_TYPE_LIST).isNotNull() & (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) == "1p" )  & (col(AUDITED_MATCH_SELLER_TYPE) == "1p" )) 
                match_suggestion = audited_base_product_unaudited_matches.filter( col(AUDITED_MATCH_SELLER_TYPE_LIST).isNull() | (col(MATCH_WAREHOUSE.SELLER_TYPE.NAME) != "1p" )  | (col(AUDITED_MATCH_SELLER_TYPE) != "1p" ) )
        return pruned_suggestion, match_suggestion
  
    
        
# INPUT
#   what we have aggregated in previous step (list of all match suggestion present with us)
#   complete match data warehouse (list of all match suggestion present in mdw)

# PROCESSING
   