from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
import bungee_utils.spark_utils.function.dataframe_util as utils


class Match_Deactivator:
    def __init__(self, mdw: DataFrame, deactivated_product: DataFrame) -> None:
        self.mdw = mdw
        self.deactivated_product = deactivated_product
    
    def deactivate_matches(self):
        
        base_deactivated_product_matches = self.mdw.join(self.deactivated_product.withColumn("sku_uuid", "base_sku_uuid"), "base_sku_uuid", "inner")
        base_deactivated_product_matches = base_deactivated_product_matches.withColumn("bungee_audit_status", lit("deactivated")).\
                                                                    withColumn("", lit("deactivated_base_product_pdp"))
        base_deactivated_product_matches.cache()
                                                                  
        comp_deactivated_product_matches = self.mdw.join(self.deactivated_product.withColumn("sku_uuid", "comp_sku_uuid"), "comp_sku_uuid", "inner")
        comp_deactivated_product_matches = comp_deactivated_product_matches.withColumn("bungee_audit_status", lit("deactivated")).\
                                                                    withColumn("", lit("deactivated_comp_product_pdp"))
        comp_deactivated_product_matches.cache()                                                            
        deactivated_product_matches = utils.combine_dfs([base_deactivated_product_matches, comp_deactivated_product_matches])
        
        return deactivated_product_matches
    