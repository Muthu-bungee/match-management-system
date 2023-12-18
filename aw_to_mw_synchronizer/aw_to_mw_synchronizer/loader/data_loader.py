from mdp_common_utils.hudi_writer import HudiDataFrameWriter
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

class DataLoader:
    def __init__(self, args: dict) -> None:
        self.mw_writer = HudiDataFrameWriter(args["hudi_config"], args["match_warehouse"])
        
    def save_date_to_db(self, mw: DataFrame):
         
        mw = mw.\
                withColumn("pair_id", col("pair_id").cast("string") ).\
                withColumn("segment", col("segment").cast("string") ).\
                withColumn("base_sku_uuid", col("base_sku_uuid").cast("string") ).\
                withColumn("comp_sku_uuid", col("comp_sku_uuid").cast("string") ).\
                withColumn("base_source_store", col("base_source_store").cast("string") ).\
                withColumn("comp_source_store", col("comp_source_store").cast("string") ).\
                withColumn("bungee_audit_status", col("bungee_audit_status").cast("string") ).\
                withColumn("misc_info", col("misc_info").cast("string") ).\
                withColumn("bungee_auditor", col("bungee_auditor").cast("string") ).\
                withColumn("bungee_audit_date", col("bungee_audit_date").cast("date") ).\
                withColumn("bungee_auditor_comment", col("bungee_auditor_comment").cast("string") ).\
                withColumn("seller_type", col("seller_type").cast("string") ).\
                withColumn("client_audit_status_l1", col("client_audit_status_l1").cast("string") ).\
                withColumn("client_auditor_l1", col("client_auditor_l1").cast("string") ).\
                withColumn("client_audit_date_l1", col("client_audit_date_l1").cast("date") ).\
                withColumn("client_auditor_l1_comment", col("client_auditor_l1_comment").cast("string") ).\
                withColumn("client_audit_status_l2", col("client_audit_status_l2").cast("string") ).\
                withColumn("client_auditor_l2", col("client_auditor_l2").cast("string") ).\
                withColumn("client_audit_date_l2", col("client_audit_date_l2").cast("date") ).\
                withColumn("client_auditor_l2_comment", col("client_auditor_l2_comment").cast("string") ).\
                withColumn("created_date", col("created_date").cast("date") ).\
                withColumn("created_by", col("created_by").cast("string") ).\
                withColumn("updated_date", col("updated_date").cast("date") ).\
                withColumn("updated_by", col("updated_by").cast("string") )
                
        mw = mw.na.fill("", ["updated_by", "bungee_audit_status", "bungee_auditor", "bungee_auditor_comment", "client_audit_status_l1", "client_auditor_l1", "client_auditor_l1_comment", "client_audit_status_l2", "client_auditor_l2", "client_auditor_l2_comment", "misc_info" ])
                                       
        self.mw_writer.write_hudi_data(mw)
    
    