from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark.sql import DataFrame
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mdp_common_utils.constants import *
from mdp_common_utils.schema import *

class BungeeAuditMatchProcessor:
    def __init__(self, args: dict, bungee_audit_matches: DataFrame ) -> None:
        self.args = args
        self.bungee_audit_matches = bungee_audit_matches

    # def _generate_additional_columns(self, bungee_audit_matches: DataFrame)-> DataFrame:
    #     bungee_audit_matches = bungee_audit_matches.withColumn(MATCH_WAREHOUSE.BASE_SOURCE_STORE.NAME, regexp_replace(col('base_source_store'), '_', '<>'))
    #     bungee_audit_matches = bungee_audit_matches.withColumn(MATCH_WAREHOUSE.COMP_SOURCE_STORE.NAME, regexp_replace(col('comp_source_store'), '_', '<>'))\
    #                                 .withColumn(MATCH_WAREHOUSE.BASE_SKU_UUID.NAME, col('sku_uuid_a') )\
    #                                 .withColumn(MATCH_WAREHOUSE.COMP_SKU_UUID.NAME, col('sku_uuid_b') )\
    #                                 .withColumn(MATCH_WAREHOUSE.PAIR_ID.NAME, concat_ws("_", col("sku_uuid_a"), col("sku_uuid_b")))\
    #                                 .withColumn(MATCH_WAREHOUSE.AGGREGATED_SCORE.NAME, when( col("score") > 1, 1.0 ).otherwise( col("score") ) )\
    #                                 .withColumn(MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME, 
    #                                                 when( col("answer").contains("exact"), lit(BUNGEE_AUDIT_STATUS.EXACT_MATCH))\
    #                                                 .when( col("answer").contains("similar") | col("answer").contains("equivalent"), lit(BUNGEE_AUDIT_STATUS.SIMILAR_MATCH))\
    #                                                 .otherwise( lit(BUNGEE_AUDIT_STATUS.NOT_MATCH.NAME) )
    #                                             )\
    #                                 .withColumn(MATCH_WAREHOUSE.BUNGEE_AUDIT_DATE, to_date(unix_timestamp(col("date_str"), "yyyyMMdd")) ) \
    #                                 .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L1.NAME, lit(None) )\
    #                                 .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L1.NAME, lit(None) )\
    #                                 .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_DATE_L1.NAME, lit(None) )\
    #                                 .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L2.NAME, lit(None) )\
    #                                 .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L2.NAME, lit(None) )\
    #                                 .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_DATE_L2.NAME, lit(None) )\
    #                                 .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L2_COMMENT.NAME, lit(None)) \
    #                                 .withColumn(MATCH_WAREHOUSE.BUNGEE_AUDITOR_COMMENT.NAME, lit(None)) \
    #                                 .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L1_COMMENT.NAME, lit(None)) \
    #                                 .withColumn(MATCH_WAREHOUSE.CREATED_DATE.NAME, lit(None)) \
    #                                 .withColumn(MATCH_WAREHOUSE.CREATED_BY.NAME, lit(None)) \
    #                                 .withColumn(MATCH_WAREHOUSE.UPDATED_DATE.NAME, lit(None)) \
    #                                 .withColumn(MATCH_WAREHOUSE.UPDATED_BY.NAME, lit(None)) \
    #                                 .withColumn(MATCH_WAREHOUSE.MISC_INFO.NAME, lit(None)) \
    #                                 .withColumn(MATCH_WAREHOUSE.SELLER_TYPE.NAME, lit(None)) \
    #                                 .withColumn("priority", lit(2))
    #     return bungee_audit_matches
        
    def process(self):
        print(f'Running bungee_audit_matches')
        bungee_audit_matches = self.bungee_audit_matches.withColumn(MATCH_WAREHOUSE.BASE_SOURCE_STORE.NAME, regexp_replace(col('base_source_store'), '_', '<>'))
        bungee_audit_matches = bungee_audit_matches.withColumn(MATCH_WAREHOUSE.COMP_SOURCE_STORE.NAME, regexp_replace(col('comp_source_store'), '_', '<>'))\
                                    .withColumn(MATCH_WAREHOUSE.BASE_SKU_UUID.NAME, col('sku_uuid_a') )\
                                    .withColumn(MATCH_WAREHOUSE.COMP_SKU_UUID.NAME, col('sku_uuid_b') )\
                                    .withColumn(MATCH_WAREHOUSE.PAIR_ID.NAME, concat_ws("_", col("sku_uuid_a"), col("sku_uuid_b")))\
                                    .withColumn(MATCH_WAREHOUSE.AGGREGATED_SCORE.NAME, when( col("score") > 1, 1.0 ).otherwise( col("score") ) )\
                                    .withColumn(MATCH_WAREHOUSE.BUNGEE_AUDIT_STATUS.NAME, 
                                                    when( col("answer").contains("exact"), lit(BUNGEE_AUDIT_STATUS.EXACT_MATCH))\
                                                    .when( col("answer").contains("similar") | col("answer").contains("equivalent"), lit(BUNGEE_AUDIT_STATUS.SIMILAR_MATCH))\
                                                    .otherwise( lit(BUNGEE_AUDIT_STATUS.NOT_MATCH) )
                                                ) \
                                    .withColumn(MATCH_WAREHOUSE.BUNGEE_AUDIT_DATE.NAME, to_date(col("match_date")) ) \
                                    .withColumn(MATCH_WAREHOUSE.BUNGEE_AUDITOR.NAME, lit(None) ) \
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L1.NAME, lit(None) ) \
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L1.NAME, lit(None) ) \
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_DATE_L1.NAME, lit(None) ) \
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_STATUS_L2.NAME, lit(None) ) \
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L2.NAME, lit(None) ) \
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDIT_DATE_L2.NAME, lit(None) ) \
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L2_COMMENT.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.BUNGEE_AUDITOR_COMMENT.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.CLIENT_AUDITOR_L1_COMMENT.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.CREATED_DATE.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.CREATED_BY.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.UPDATED_DATE.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.UPDATED_BY.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.MISC_INFO.NAME, lit(None)) \
                                    .withColumn(MATCH_WAREHOUSE.SELLER_TYPE.NAME, lit(None)) \
                                    .withColumn("priority", lit(2))
        bungee_audit_matches = bungee_audit_matches.select(get_column_list(MATCH_WAREHOUSE))
        return bungee_audit_matches
    