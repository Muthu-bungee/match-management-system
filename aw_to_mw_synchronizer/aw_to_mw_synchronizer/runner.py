from aw_to_mw_synchronizer.extractor.data_reader import DataFetcher
from aw_to_mw_synchronizer.transformer.match_merger import MatchMerger
from aw_to_mw_synchronizer.transformer.bungee_audit_match_processor import BungeeAuditMatchProcessor
from aw_to_mw_synchronizer.transformer.customer_audit_match_processor import CustomerAuditMatchProcessor
from aw_to_mw_synchronizer.loader.data_loader import DataLoader
from mdp_common_utils.parameter_fetcher import *
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from bungee_utils.spark_utils.function.dataframe_util import *

class Synchronizer:
    def __init__(self, spark: SparkSession, args: dict, env: str) -> None:
        self.args = fetch_parameters(args, env)
        self.env = env
        self.spark = spark
    
    def sync(self):
        bungee_audit_matches, customer_audit_matches = self._extraction()
        merged_audited_matches = self._transformation(bungee_audit_matches, customer_audit_matches)
        merged_directed_audited_matches = self.convert_to_direct_pair_matches(merged_audited_matches)
        self._loading(merged_directed_audited_matches)
        
    def _extraction(self):
        print("data fetching started")
        reader = DataFetcher(self.args, self.spark, self.env)
        bungee_audit_matches = reader.fetch_bungee_audit_library()
        customer_audit_matches = reader.fetch_customer_audit_library()
        print("Data fetching finished")
        return bungee_audit_matches, customer_audit_matches

    def _transformation(self,  bungee_audit_matches:DataFrame, customer_audit_matches:DataFrame):
        bungee_audit_matches = BungeeAuditMatchProcessor(self.args, bungee_audit_matches).process()
        customer_audit_matches = CustomerAuditMatchProcessor(self.args, customer_audit_matches).process()
        merger = MatchMerger(self.args, bungee_audit_matches, customer_audit_matches)
        merged_audited_matches = merger.merge_matches()
        return merged_audited_matches
    
    def _loading(self, merged_audited_matches:DataFrame):
        DataLoader(self.args).save_date_to_db(merged_audited_matches)
    
    def convert_to_direct_pair_matches(self, undirected_matches:DataFrame):
        print("converting undirected to directed pairs")
        match_source_to_dest = undirected_matches
        match_dest_to_source = undirected_matches.withColumnRenamed("base_sku_uuid", "temp_sku_uuid").\
                                                withColumnRenamed("comp_sku_uuid", "base_sku_uuid").\
                                                withColumnRenamed("temp_sku_uuid", "comp_sku_uuid").\
                                                withColumnRenamed("base_source_store", "temp_source_store").\
                                                withColumnRenamed("comp_source_store", "base_source_store").\
                                                withColumnRenamed("temp_source_store", "comp_source_store").\
                                                withColumn("pair_id", concat_ws("_", col("base_sku_uuid"), col("comp_sku_uuid")))
                                                
        directed_matches = combine_dfs([match_source_to_dest, match_dest_to_source]).dropDuplicates(["pair_id"])
        
        if self.env != "prod":
            print("undirected matches count ", undirected_matches.count())
            undirected_matches.show(truncate = False, n = 100)
            print("directed matches count ", directed_matches.count())
            directed_matches.show(truncate = False, n = 100)
        print("Converted undirected to directed pairs")
        return directed_matches
        
                 





