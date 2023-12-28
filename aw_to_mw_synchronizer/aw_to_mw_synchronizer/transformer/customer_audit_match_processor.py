from pyspark.sql.types import *
from pyspark.sql.functions import lit
import re
from pyspark.sql import DataFrame
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mdp_common_utils.constants import *
from mdp_common_utils.schema import *

class CustomerAuditMatchProcessor:
    def __init__(self, args: dict, customer_audit_matches: DataFrame ) -> None:
        self.args = args
        self.customer_audit_matches = customer_audit_matches
        self.match_warehouse_cols = get_column_list(MATCH_WAREHOUSE)
        self._get_match_type_classification()
        self._get_match_segment_classification()

    def _get_match_type_classification(self):
        self.exact_matches = ["exact"]
        self.similar_matches = ["similar", "equivalent"]
        self.not_a_match = ["no"]

    def _get_match_segment_classification(self):
        self.segment_company_code_map = {}
        self.segment_company_code_map[SEGMENT.GROCERY] = [item.split("<>")[0] for item in self.args["customers"]["grocery"] ]
        self.segment_company_code_map[SEGMENT.OFFICE_SUPPLIES] = [item.split("<>")[0] for item in self.args["customers"]["office_supplies"] ]
        self.segment_company_code_map[SEGMENT.PETS] = [item.split("<>")[0] for item in self.args["customers"]["pets"] ]
        self.segment_company_code_map[SEGMENT.SPORTS_OUTDOORS] = [item.split("<>")[0] for item in self.args["customers"]["sports_outdoors"] ]
        self.segment_company_code_map[SEGMENT.PETS_CA] = [item.split("<>")[0] for item in self.args["customers"]["petsca"] ]
        print(self.segment_company_code_map)

    def _generate_additional_columns(self, customer_audit_matches: DataFrame)-> DataFrame:
        customer_audit_matches = customer_audit_matches.withColumn(MATCH_WAREHOUSE.BASE_SOURCE_STORE.NAME, regexp_replace(col("base_source_store"), "_", "<>"))\
                                    .withColumn(MATCH_WAREHOUSE.COMP_SOURCE_STORE.NAME, regexp_replace(col("comp_source_store"), "_", "<>"))\
                                    .withColumn(MATCH_WAREHOUSE.BASE_SKU_UUID.NAME, concat_ws("<>", col("base_sku"), col("base_source_store")))\
                                    .withColumn(MATCH_WAREHOUSE.COMP_SKU_UUID.NAME, concat_ws("<>", col("comp_sku"), col("comp_source_store")))\
                                    .withColumn(MATCH_WAREHOUSE.PAIR_ID.NAME, concat_ws("_", col(MATCH_WAREHOUSE.BASE_SKU_UUID.NAME), col(MATCH_WAREHOUSE.COMP_SKU_UUID.NAME)))\
                                    .withColumn(MATCH_WAREHOUSE.AGGREGATED_SCORE.NAME, when( col("score") > 1.0, lit(1.0)).otherwise( col("score") ) )\
                                    .withColumn(MATCH_WAREHOUSE.MATCH_SOURCE_SCORE_MAP.NAME, create_map( col("match_source"), col(MATCH_WAREHOUSE.AGGREGATED_SCORE.NAME) ))\
                                    .withColumn(MATCH_WAREHOUSE.BUNGEE_AUDITOR.NAME, split(col("match"), ",").getItem(
                                                    when(array_contains(split(col("match"), ","), "bungeetech"), 1)
                                                ))\
                                    .withColumn(MATCH_WAREHOUSE.BUNGEE_AUDIT_DATE.NAME, to_date(from_unixtime(col("client_sku_first_seen_date"))))\
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L1.NAME, col("tpvr_worker_choice") )\
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L1.NAME, col("tpvr_worker") )\
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_DATE_L1.NAME, col("customer_verified_date") )\
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L2.NAME, col("tpvr_manager_choice") )\
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L2.NAME, col("tpvr_manager") )\
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_DATE_L2.NAME, col("manager_verified_date") )\
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L2_COMMENT.NAME, col("matcher_comments")) \
                                    .withColumn(MATCH_WAREHOUSE.BUNGEE_AUDITOR_COMMENT.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L1_COMMENT.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.CREATED_DATE.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.CREATED_BY.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.UPDATED_DATE.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.UPDATED_BY.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.MISC_INFO.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.SELLER_TYPE.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.WORKFLOW_NAME.NAME, lit(None)) \
                                    .withColumn("priority", lit(1))
        return customer_audit_matches
    
    def _add_segment_to_customer_audit_matches_based_on_company_code(self, customer_audit_matches: DataFrame):
        customer_audit_matches = customer_audit_matches.withColumn(MATCH_WAREHOUSE.SEGMENT.NAME, lit(None).cast(StringType()) )
        for segment, retailer_list in self.segment_company_code_map.items():
            customer_audit_matches = customer_audit_matches.withColumn(MATCH_WAREHOUSE.SEGMENT.NAME\
                                                , when( col("company_code").isin(retailer_list), lit(segment).cast(StringType()) ) \
                                                .otherwise( col(MATCH_WAREHOUSE.SEGMENT.NAME) )
                                            )
            
        customer_audit_matches = customer_audit_matches.withColumn(MATCH_WAREHOUSE.SEGMENT.NAME\
                                                ,when( col(MATCH_WAREHOUSE.SEGMENT.NAME).isNull(), lit(SEGMENT.UNSEGMENTED))\
                                                .otherwise( col(MATCH_WAREHOUSE.SEGMENT.NAME) ) )
        return customer_audit_matches
    


    def _fetch_active_matches(self, customer_audit_matches: DataFrame):
        matched_df = customer_audit_matches.filter( (customer_audit_matches.deleted_date.isNull()) | (customer_audit_matches.deleted_date == ""))
        return matched_df
    
    def _fetch_deleted_matches(self, customer_audit_matches: DataFrame):
        not_match_df = customer_audit_matches.filter(  ((customer_audit_matches.deleted_date.isNotNull()) & (customer_audit_matches.deleted_date != "")))
        return not_match_df

    def _add_bungee_audit_status_for_active_matches(self, match_df:DataFrame):
        match_df = match_df.withColumn(MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME, \
                                                    when( col("match").isin(self.exact_matches), lit(BUNGEE_AUDIT_STATUS.EXACT_MATCH) )\
                                                    .when( col("match").isin(self.similar_matches), lit(BUNGEE_AUDIT_STATUS.SIMILAR_MATCH) )\
                                                    .when( col("match").isin(self.not_a_match), lit(BUNGEE_AUDIT_STATUS.NOT_MATCH) )\
                                                    .otherwise( lit(BUNGEE_AUDIT_STATUS.UNSURE) )
                                                )
        return match_df
    
    def _add_bungee_audit_status_for_deleted_matches(self, not_match_df: DataFrame):
        not_match_df = not_match_df.withColumn( MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME, lit(BUNGEE_AUDIT_STATUS.INACTIVE) )
        return not_match_df

    def _add_match_source_for_legacy_matches(self, match_df: DataFrame):
        legacy_matches_condition = (col('model_used').isin(['manualinternal','manualexternal','m_l_t', "mlt"]))
        match_df = match_df.withColumn("match_source", when( legacy_matches_condition , lit(MATCH_SOURCE.LEGACY))\
                                        .otherwise(col("match_source")) )
        return match_df
    
    def _add_match_source_for_manual_matches(self, match_df: DataFrame):
        manual_matches_condition = (col('model_used').contains('manual') & ~(col('model_used').isin(['manualinternal','manualexternal','m_l_t']))) 
        match_df = match_df.withColumn("match_source", when( manual_matches_condition , lit(MATCH_SOURCE.MANUAL))\
                                        .otherwise(col("match_source")) )
        return match_df
    
    def _add_match_source_for_legacy_ml_matches(self, match_df: DataFrame):
        legacy_ml_matches_condition =  (col('model_used').isin(['simengineautov1','simenginev1'])) 
        match_df = match_df.withColumn("match_source", when( legacy_ml_matches_condition , lit(MATCH_SOURCE.LEGACY_ML))\
                                        .otherwise(col("match_source")) )
        return match_df
        
    def _add_match_source_for_fastlane_matches(self, match_df: DataFrame):
        fastlane_matches_condition = (  (col('model_used').contains('btfastlane') & col('model_used').contains('ml'))  
                                            | (col('model_used').contains('ml_v')) | (col('model_used') == 'ml') )
        match_df = match_df.withColumn("match_source", when( fastlane_matches_condition , lit(MATCH_SOURCE.FASTLANE ))\
                                        .otherwise(col("match_source")) )
        return match_df
    # TODO: need to revisit this in future during MDP
    
    def _add_match_source_for_upc_matches(self, match_df: DataFrame):
        upc_matches_condition =  ( col('model_used').contains('upc') )
        match_df = match_df.withColumn("match_source", when( upc_matches_condition , lit(MATCH_SOURCE.UPC))\
                                        .otherwise(col("match_source")) )
        return match_df
    
    def _add_match_source_for_transitive_matches(self, match_df: DataFrame):
        transitive_matches_condition =  ( col('model_used').contains('transitive') )
        match_df = match_df.withColumn("match_source", when( transitive_matches_condition , lit(MATCH_SOURCE.TRANSITIVE))\
                                        .otherwise(col("match_source")) )
        return match_df
    
    def _add_match_source_for_mpn_matches(self, match_df: DataFrame):
        transitive_matches_condition =  ( col('model_used').contains('mpn') )
        match_df = match_df.withColumn("match_source", when( transitive_matches_condition , lit(MATCH_SOURCE.MPN))\
                                        .otherwise(col("match_source")) )
        return match_df
        
    
    def _add_match_source(self, match_df: DataFrame):
        match_df = match_df.withColumn( "model_used", lower(col("model_used")))
        match_df = match_df.withColumn( "match_source", lit(None).cast(StringType()) )
        match_df = self._add_match_source_for_legacy_matches(match_df)
        match_df = self._add_match_source_for_manual_matches(match_df)
        match_df = self._add_match_source_for_legacy_ml_matches(match_df)
        match_df = self._add_match_source_for_fastlane_matches(match_df)
        match_df = self._add_match_source_for_upc_matches(match_df)
        match_df = self._add_match_source_for_transitive_matches(match_df)
        match_df = match_df.withColumn("match_source", when(  col( "match_source").isNull() , lit('UNKNOWN') ).otherwise( col("match_source") ) )
       
        return match_df
    
    def process(self):
        print(f"Running customer_audit_matches")
        customer_audit_matches = self._add_match_source(self.customer_audit_matches)
        customer_audit_matches.select("model_used", "match_source").show()
        customer_audit_matches = self._generate_additional_columns(customer_audit_matches)
        customer_audit_matches.show()
        customer_audit_matches = self._add_segment_to_customer_audit_matches_based_on_company_code(customer_audit_matches)
        customer_audit_matches.select("company_code", "segment").show()
        
        active_match_df = self._fetch_active_matches(customer_audit_matches)
        print("active_match_df")
        active_match_df.show()
        deleted_match_df = self._fetch_deleted_matches(customer_audit_matches)
        print("deleted_match_df")
        deleted_match_df.show()
        active_match_df = active_match_df.join(deleted_match_df.select(MATCH_WAREHOUSE.PAIR_ID.NAME), "pair_id", "left_anti")
        
        active_match_df = self._add_bungee_audit_status_for_active_matches(active_match_df)
        print("active_match_df")
        active_match_df.show()
        deleted_match_df = self._add_bungee_audit_status_for_deleted_matches(deleted_match_df)
        print("deleted_match_df")
        deleted_match_df.show()
        
        customer_audit_matches = active_match_df.union(deleted_match_df)
        customer_audit_matches.show()
        customer_audit_matches = customer_audit_matches.select(get_column_list(MATCH_WAREHOUSE) + ["priority"])
        customer_audit_matches.show()
        return customer_audit_matches
    