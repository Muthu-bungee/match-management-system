from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark.sql import DataFrame
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mdp_common_utils.constants import *
from mdp_common_utils.schema import *
from bungee_utils.spark_utils.function.dataframe_util import *

class BungeeAuditMatchProcessor:
    def __init__(self, args: dict, bungee_successful_audit_matches: DataFrame, bungee_unsuccessful_audit_matches: DataFrame ) -> None:
        self.args = args
        self.bungee_successful_audit_matches = bungee_successful_audit_matches
        self.bungee_unsuccessful_audit_matches = bungee_unsuccessful_audit_matches

    def _generate_product_info_columns(self, bungee_audit_matches: DataFrame)-> DataFrame:
        print(f'Running bungee_audit_matches')
        bungee_audit_matches = bungee_audit_matches.withColumn(MATCH_WAREHOUSE.BASE_SOURCE_STORE.NAME, regexp_replace(col("base_source_store"), "_", "<>"))\
                                    .withColumn(MATCH_WAREHOUSE.COMP_SOURCE_STORE.NAME, regexp_replace(col("comp_source_store"), "_", "<>"))\
                                    .withColumn(MATCH_WAREHOUSE.BASE_SKU_UUID.NAME, concat_ws("<>", col("base_sku"), col("base_source_store")))\
                                    .withColumn(MATCH_WAREHOUSE.COMP_SKU_UUID.NAME, concat_ws("<>", col("comp_sku"), col("comp_source_store")))\
                                    .withColumn(MATCH_WAREHOUSE.PAIR_ID.NAME, concat_ws("_", col(MATCH_WAREHOUSE.BASE_SKU_UUID.NAME), col(MATCH_WAREHOUSE.COMP_SKU_UUID.NAME)))
        return bungee_audit_matches
            
    def _remove_unsuccessful_matches(self, bungee_successful_audit_matches: DataFrame, bungee_unsuccessful_audit_matches: DataFrame):
        bungee_successful_audit_matches = bungee_successful_audit_matches.join(bungee_unsuccessful_audit_matches, "pair_id", "left_anti")
        return bungee_successful_audit_matches
    
    def _add_audit_status_for_successful_audit_matches(self, bungee_successful_audit_matches: DataFrame):
        bungee_successful_audit_matches = bungee_successful_audit_matches.withColumn(MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME, 
                                                    when( col("answer").contains("exact"), lit(BUNGEE_AUDIT_STATUS.EXACT_MATCH))\
                                                    .when( col("answer").contains("similar") | col("answer").contains("equivalent"), lit(BUNGEE_AUDIT_STATUS.SIMILAR_MATCH))\
                                                    .otherwise( lit(BUNGEE_AUDIT_STATUS.UNAUDITED) )
                                                )
        return bungee_successful_audit_matches
        
    def _add_audit_status_for_unsuccessful_audit_matches(self, bungee_unsuccessful_audit_matches: DataFrame):
        bungee_unsuccessful_audit_matches = bungee_unsuccessful_audit_matches.withColumn( MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME, lit(BUNGEE_AUDIT_STATUS.NOT_MATCH) )
        return bungee_unsuccessful_audit_matches
    
    def _add_additional_columns(self, bungee_audit_matches: DataFrame):
        print(f'Running bungee_audit_matches')
        bungee_audit_matches = bungee_audit_matches.withColumn(MATCH_WAREHOUSE.AGGREGATED_SCORE.NAME, when( col("score") > 1, 1.0 ).otherwise( col("score") ) )\
                                    .withColumn("match_source", 
                                                    when( col("model_used").contains("ml"), lit(MATCH_SOURCE.ML))\
                                                    .when( col("model_used").contains("upc"), lit(MATCH_SOURCE.UPC))\
                                                    .when( col("model_used").contains("mpn"), lit(MATCH_SOURCE.MPN))\
                                                    .when( col("model_used").contains("manual"), lit(MATCH_SOURCE.MANUAL))\
                                                    .when( col("model_used").contains("transitive"), lit(MATCH_SOURCE.TRANSITIVE))\
                                                    .otherwise( lit(MATCH_SOURCE.LEGACY) )
                                                ) \
                                    .withColumn(MATCH_WAREHOUSE.MATCH_SOURCE_SCORE_MAP.NAME, create_map( col("match_source"), col(MATCH_WAREHOUSE.AGGREGATED_SCORE.NAME) ))\
                                    .withColumn(MATCH_WAREHOUSE.SEGMENT.NAME, col("domain") ) \
                                    .withColumn(MATCH_WAREHOUSE.BUNGEE_AUDIT_DATE.NAME, to_date(col("match_date")) ) \
                                    .withColumn(MATCH_WAREHOUSE.BUNGEE_AUDITOR.NAME, col("matcher") ) \
                                    .withColumn(MATCH_WAREHOUSE.WORKFLOW_NAME.NAME, col("workflow") ) \
                                    .withColumn(MATCH_WAREHOUSE.BUNGEE_AUDITOR_COMMENT.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.MISC_INFO.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.SELLER_TYPE.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L1.NAME, lit(None) ) \
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L1.NAME, lit(None) ) \
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_DATE_L1.NAME, lit(None) ) \
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L2.NAME, lit(None) ) \
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L2.NAME, lit(None) ) \
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_DATE_L2.NAME, lit(None) ) \
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L2_COMMENT.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L1_COMMENT.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.CREATED_DATE.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.CREATED_BY.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.UPDATED_DATE.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.UPDATED_BY.NAME, lit(None)) \
                                    .withColumn("priority", lit(2))
        bungee_audit_matches = bungee_audit_matches.select(get_column_list(MATCH_WAREHOUSE) + ["priority"])
        return bungee_audit_matches
    
    def process(self ):
        bungee_successful_audit_matches = self._generate_product_info_columns(self.bungee_successful_audit_matches)
        bungee_unsuccessful_audit_matches = self._generate_product_info_columns(self.bungee_unsuccessful_audit_matches)
        bungee_successful_audit_matches = self._remove_unsuccessful_matches(bungee_successful_audit_matches, bungee_unsuccessful_audit_matches)
        bungee_successful_audit_matches = self._add_audit_status_for_successful_audit_matches(bungee_successful_audit_matches)
        bungee_unsuccessful_audit_matches = self._add_audit_status_for_unsuccessful_audit_matches(bungee_unsuccessful_audit_matches)
        bungee_audit_matches = combine_dfs( [bungee_successful_audit_matches, bungee_unsuccessful_audit_matches] )
        bungee_audit_matches = self._add_additional_columns(bungee_audit_matches)
        return bungee_audit_matches
