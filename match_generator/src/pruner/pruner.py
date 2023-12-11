from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql import DataFrame
from bungee_utils.spark_utils.function.dataframe_util import *
from pyspark.sql.window import Window

class Match_Pruner:
    def __init__(self, mdw: DataFrame, match_suggestion: DataFrame, config: dict) -> None:
        self.mdw = mdw
        self.match_suggestion = match_suggestion
        self.config = config

    def prune_matches(self):
        self.mdw, match_suggestion = self.preprocessing_matches(self.mdw, self.match_suggestion)
        similar_matches, exact_matches, unaudited_matches = self.separate_audited_and_unaudited_matches(match_suggestion, self.mdw)
        pruned_suggestion, match_suggestion = self.prune_match_based_on_client_config(similar_matches, exact_matches, unaudited_matches)
            
        if combine_dfs([pruned_suggestion, match_suggestion]).count() == unaudited_matches.count():
            print("matches are not missing")
        elif combine_dfs([pruned_suggestion, match_suggestion]).count() < unaudited_matches.count(): 
            print("matches are missing")
        else :
            print("duplicate matches are present")

        return match_suggestion, pruned_suggestion

    def preprocessing_matches(self, mdw: DataFrame, match_suggestion: DataFrame):
        match_suggestion = match_suggestion.withColumn("seller_type", lower(col("seller_type")))
        mdw = mdw.withColumn("seller_type", lower(col("seller_type")))\
            .withColumn("bungee_audit_status", when( col("bungee_audit_status").isNotNull(), lower(col("bungee_audit_status")) ).otherwise(col("bungee_audit_status")) )

        return mdw, match_suggestion
    
    def separate_audited_and_unaudited_matches(self, match_suggestion:DataFrame, mdw:DataFrame):
        print("separate_audited_and_unaudited_matches")
        window_spec = Window.partitionBy("base_sku_uuid")
        similar_matches = mdw.filter(col('bungee_audit_status').isin(['equivalent', 'similar']))
        similar_matches = similar_matches.withColumn("audited_match_seller_type_list", concat_ws( "_", array_sort( collect_set(col("seller_type")).over(window_spec) ) ) )
        similar_matches = similar_matches.withColumn("misc_info", concat(lit("similar_match_"), col("pair_id")) ).\
            select("base_sku_uuid", "audited_match_seller_type_list", "misc_info", col("seller_type").alias("audited_match_seller_type"))
        similar_matches.show(n=10)                                
               
        exact_matches = mdw.filter(col('bungee_audit_status').isin(['exact']))
        exact_matches = exact_matches.withColumn("audited_match_seller_type_list", concat_ws("_", array_sort( collect_set(col("seller_type")).over(window_spec) ) ) )
        exact_matches = exact_matches.withColumn("misc_info", concat(lit("exact_match_"), col("pair_id")) ).\
                        select("base_sku_uuid", "audited_match_seller_type_list","misc_info", col("seller_type").alias("audited_match_seller_type"))
        unaudited_matches = match_suggestion.join(mdw.select("pair_id"), 'pair_id', 'left_anti')
        return similar_matches, exact_matches, unaudited_matches

    def prune_match_based_on_client_config(self, similar_matches:DataFrame , exact_matches:DataFrame , audited_base_product_unaudited_matches:DataFrame ):
        pruned_suggestion = None
        match_suggestion = None
        if self.config['match_type'] == 'exact' or self.config['match_type'] == 'exact_over_similar':
            pruned_suggestion, match_suggestion = self.prune_for_exact_match_config(exact_matches, audited_base_product_unaudited_matches) 

        elif self.config['match_type'] == 'similar' or self.config['match_type'] == 'exact_or_similar':
            exact_and_similar_match = exact_matches.union(similar_matches)
            pruned_suggestion, match_suggestion = self.prune_for_exact_similar_match(audited_base_product_unaudited_matches, exact_and_similar_match)
            
        if pruned_suggestion is not None:
            pruned_suggestion = pruned_suggestion.withColumn("bungee_audit_status", lit("pruned"))
            pruned_suggestion = pruned_suggestion.withColumn("bungee_auditor", lit("match_management_system"))
            pruned_suggestion = pruned_suggestion.withColumn("updated_by", lit("match_management_system"))
            pruned_suggestion = pruned_suggestion.withColumn("updated_date", current_timestamp())
            
        return pruned_suggestion, match_suggestion

    def prune_for_exact_similar_match(self, audited_base_product_unaudited_matches, exact_and_similar_match):
        if self.config['seller_type'] == '1p':
            audited_base_product_unaudited_matches = audited_base_product_unaudited_matches.join(exact_and_similar_match.filter(col("audited_match_seller_type") == '1p'), "base_sku_uuid", "left")
            if self.config['cardinality'] == '1':
                pruned_suggestion = audited_base_product_unaudited_matches.filter( col("audited_match_seller_type_list").isNotNull() | (col("seller_type") == '3p')).\
                                    withColumn("misc_info", when( col("seller_type") == '3p', lit("seller_type_config_1p")).otherwise(col("misc_info")) )
                match_suggestion = audited_base_product_unaudited_matches.filter( col("misc_info").isNull() & (col("seller_type") == '1p') )
            if self.config['cardinality'] == 'n':
                pruned_suggestion = audited_base_product_unaudited_matches.filter( (col("seller_type") == '3p')).\
                                    withColumn("misc_info", when( col("seller_type") == '3p', lit("seller_type_config_3p")).otherwise(col("misc_info")) )
                match_suggestion = audited_base_product_unaudited_matches.filter( (col("seller_type") == '1p'))
        elif self.config['seller_type'] == '3p':
            audited_base_product_unaudited_matches = audited_base_product_unaudited_matches.join(exact_and_similar_match.filter(col("audited_match_seller_type") == '3p'), "base_sku_uuid", "left")
            if self.config['cardinality'] == '1':
                pruned_suggestion = audited_base_product_unaudited_matches.filter( col("audited_match_seller_type_list").isNotNull() | (col("seller_type") == '1p')).\
                                    withColumn("misc_info", when( col("seller_type") == '1p', lit("seller_type_config_1p")).otherwise(col("misc_info")) )
                match_suggestion = audited_base_product_unaudited_matches.filter( col("misc_info").isNull() & (col("seller_type") == '3p') )
            if self.config['cardinality'] == 'n':
                pruned_suggestion = audited_base_product_unaudited_matches.filter( (col("seller_type") == '1p')).\
                                    withColumn("misc_info", when( col("seller_type") == '1p', lit("seller_type_config_3p")).otherwise(col("misc_info")) )
                match_suggestion = audited_base_product_unaudited_matches.filter( (col("seller_type") == '3p'))
        elif self.config['seller_type'] == '1p_or_3p':
            exact_and_similar_match = exact_and_similar_match.withColumn("audited_match_seller_type_list", when( col("audited_match_seller_type_list") == "1p_3p", "1p").otherwise(col("audited_match_seller_type_list")) ).\
                                            filter( col("audited_match_seller_type_list") == col("audited_match_seller_type"))
            audited_base_product_unaudited_matches = audited_base_product_unaudited_matches.join(exact_and_similar_match, "base_sku_uuid", "left")
            if self.config['cardinality'] == '1':
                pruned_suggestion = audited_base_product_unaudited_matches.filter(col("audited_match_seller_type_list").isNotNull() )
                match_suggestion = audited_base_product_unaudited_matches.filter(col("audited_match_seller_type_list").isNull() )
            if self.config['cardinality'] == 'n':
                pruned_suggestion = None
                match_suggestion = audited_base_product_unaudited_matches
        elif self.config['seller_type'] == '1p_over_3p':
            exact_and_similar_match = exact_and_similar_match.withColumn("audited_match_seller_type_list", when( col("audited_match_seller_type_list") == "1p_3p", "1p").otherwise(col("audited_match_seller_type_list")) ).\
                                            filter( col("audited_match_seller_type_list") == col("audited_match_seller_type"))
            audited_base_product_unaudited_matches = audited_base_product_unaudited_matches.join(exact_and_similar_match, "base_sku_uuid", "left")
            audited_base_product_unaudited_matches.printSchema()
            if self.config['cardinality'] == '1':
                pruned_suggestion = audited_base_product_unaudited_matches.filter( ~(col("audited_match_seller_type_list").isNull() | ((col("audited_match_seller_type_list") == "3p") & ( col('seller_type') == '1p' )) ) )
                match_suggestion = audited_base_product_unaudited_matches.filter( col("audited_match_seller_type_list").isNull() | ( (col("audited_match_seller_type_list") == "3p") & (col('seller_type') == '1p') )  )
            if self.config['cardinality'] == 'n':
                pruned_suggestion = None
                match_suggestion = audited_base_product_unaudited_matches
        return pruned_suggestion,match_suggestion

    def prune_for_exact_match_config(self, exact_matches, audited_base_product_unaudited_matches):
        if self.config['seller_type'] == '1p':
            audited_base_product_unaudited_matches = audited_base_product_unaudited_matches.join(exact_matches.filter(col("audited_match_seller_type") == '1p'), "base_sku_uuid", "left")
            pruned_suggestion = audited_base_product_unaudited_matches.filter( col("audited_match_seller_type_list").isNotNull() | (col("seller_type") == '3p')).\
                                    withColumn("misc_info", when( col("seller_type") == '3p', lit("seller_type_config_1p")).otherwise(col("misc_info")) )
            match_suggestion = audited_base_product_unaudited_matches.filter( col("misc_info").isNull() & (col("seller_type") == '1p') )
        elif self.config['seller_type'] == '3p':
            audited_base_product_unaudited_matches = audited_base_product_unaudited_matches.join(exact_matches.filter(col("audited_match_seller_type") == '3p'), "base_sku_uuid", "left")
            if self.config['cardinality'] == '1':
                pruned_suggestion = audited_base_product_unaudited_matches.filter(col("audited_match_seller_type_list").isNotNull() | (col("seller_type") == '1p')).\
                                    withColumn("misc_info", when( col("seller_type") == '1p', lit("seller_type_config_3p")).otherwise(col("misc_info")) )
                match_suggestion = audited_base_product_unaudited_matches.filter(col("misc_info").isNull() & (col("seller_type") == '3p'))
            if self.config['cardinality'] == 'n':
                pruned_suggestion = audited_base_product_unaudited_matches.filter(col("seller_type") == '1p').withColumn("misc_info", lit("seller_type_config_3p"))
                match_suggestion = audited_base_product_unaudited_matches.filter(col("seller_type") == '3p')
        elif self.config['seller_type'] == '1p_or_3p':
            exact_matches = exact_matches.withColumn("audited_match_seller_type_list", when( col("audited_match_seller_type_list") == "1p_3p", "1p").otherwise(col("audited_match_seller_type_list")) ).\
                                            filter( col("audited_match_seller_type_list") == col("audited_match_seller_type"))
            audited_base_product_unaudited_matches = audited_base_product_unaudited_matches.join(exact_matches, "base_sku_uuid", "left")
            if self.config['cardinality'] == '1':
                pruned_suggestion = audited_base_product_unaudited_matches.filter(col("audited_match_seller_type_list").isNotNull() )
                match_suggestion = audited_base_product_unaudited_matches.filter(col("audited_match_seller_type_list").isNull() )
            if self.config['cardinality'] == 'n':
                pruned_suggestion = audited_base_product_unaudited_matches.filter( col("audited_match_seller_type_list").isNotNull() & (col("seller_type") == "1p" )  & (col("audited_match_seller_type") == "1p" )) 
                match_suggestion = audited_base_product_unaudited_matches.filter( col("audited_match_seller_type_list").isNull() | (col("seller_type") != "1p" )  | (col("audited_match_seller_type") != "1p" ) ) 
        elif self.config['seller_type'] == '1p_over_3p':
            exact_matches = exact_matches.withColumn("audited_match_seller_type_list", when( col("audited_match_seller_type_list") == "1p_3p", "1p").otherwise(col("audited_match_seller_type_list")) ).\
                                            filter( col("audited_match_seller_type_list") == col("audited_match_seller_type"))
            audited_base_product_unaudited_matches = audited_base_product_unaudited_matches.join(exact_matches, "base_sku_uuid", "left")
            if self.config['cardinality'] == '1':
                pruned_suggestion = audited_base_product_unaudited_matches.filter( ~(col("audited_match_seller_type_list").isNull() | ((col("audited_match_seller_type_list") == "3p") & ( col('seller_type') == '1p' )) ) )
                match_suggestion = audited_base_product_unaudited_matches.filter( col("audited_match_seller_type_list").isNull() | ( (col("audited_match_seller_type_list") == "3p") & (col('seller_type') == '1p') )  )
            if self.config['cardinality'] == 'n':
                pruned_suggestion = audited_base_product_unaudited_matches.filter( col("audited_match_seller_type_list").isNotNull() & (col("seller_type") == "1p" )  & (col("audited_match_seller_type") == "1p" )) 
                match_suggestion = audited_base_product_unaudited_matches.filter( col("audited_match_seller_type_list").isNull() | (col("seller_type") != "1p" )  | (col("audited_match_seller_type") != "1p" ) )
        return pruned_suggestion, match_suggestion
  
    
        
# INPUT
#   what we have aggregated in previous step (list of all match suggestion present with us)
#   complete match data warehouse (list of all match suggestion present in mdw)

# PROCESSING
   