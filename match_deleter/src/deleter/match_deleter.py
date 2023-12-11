from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
import bungee_utils.spark_utils.function.dataframe_util as utils


class Match_Deleter:
    def __init__(self, mdw: DataFrame, deleted_matches: DataFrame) -> None:
        self.mdw = mdw
        self.deleted_matches = deleted_matches.dropDuplicates(["sku_uuid"])
    
    def fetch_deactivate_matches(self):
        
        deleted_product_matches = self.mdw.join(self.deleted_matches, "pair_id", "inner")
        deleted_product_matches = deleted_product_matches.withColumn("bungee_audit_status", lit("deleted"))
        deleted_product_matches = deleted_product_matches.withColumn("bungee_auditor", lit("match_management_system"))
        deleted_product_matches = deleted_product_matches.withColumn("updated_by", lit("match_management_system"))
        deleted_product_matches = deleted_product_matches.withColumn("updated_date", current_timestamp())
        return deleted_product_matches
    