from aw_to_mw_synchronizer.extractor.data_reader import DataFetcher
from aw_to_mw_synchronizer.transformer.match_merger import MatchMerger
from aw_to_mw_synchronizer.transformer.bungee_audit_match_processor import BungeeAuditMatchProcessor
from aw_to_mw_synchronizer.transformer.customer_audit_match_processor import CustomerAuditMatchProcessor
from aw_to_mw_synchronizer.loader.data_loader import DataLoader
from mdp_common_utils.parameter_fetcher import *
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from bungee_utils.spark_utils.function.dataframe_util import *
from mdp_common_utils.schema import *
class Synchronizer:
    def __init__(self, spark: SparkSession, args: dict, env: str, glue_context) -> None:
        self.args = fetch_parameters(args, env)
        self.env = env
        self.spark = spark
        self.glue_context = glue_context
        self.reader = DataFetcher(self.args, self.spark, self.env, self.glue_context)
    
    def _fetch_audited_match_for_merging(self):
        print("Fetching audited match")
        self.successful_bungee_audit_matches = self.reader.fetch_successful_bungee_audit_library()
        self.unsuccessful_bungee_audit_matches = self.reader.fetch_unsuccessful_bungee_audit_library()
        self.customer_audit_matches = self.reader.fetch_customer_audit_library()
        self.mdw = self.reader.fetch_audited_matches_from_mw()
        print("audited match fetched")

    def _merge_matches(self):
        print("fetching audited match")
        bungee_audit_matches = BungeeAuditMatchProcessor(self.args, self.successful_bungee_audit_matches, self.unsuccessful_bungee_audit_matches).process()
        bungee_audit_matches.show()
        customer_audit_matches = CustomerAuditMatchProcessor(self.args, self.customer_audit_matches).process()
        customer_audit_matches.show()
        self.merged_matches = MatchMerger(self.env, self.args, bungee_audit_matches, customer_audit_matches, self.mdw).merge_matches()
        
    def _fetch_unaudited_matches_for_pruning(self):
        self.unaudited_non_pruned_matches = self.reader.fetch_unaudited_matches_from_mw_based_on_source_store()
        print("Data fetching finished")
        
    def _prune_matches():
        pass 
    
    def _save_data_to_mw(self):
        DataLoader(self.args).save_date_to_db(self.merged_matches)
        
    def sync(self):
        self._fetch_audited_match_for_merging()
        self._merge_matches()
        self._fetch_unaudited_matches_for_pruning()
        self._prune_matches()
        self._save_data_to_mw()
        # self._loading()
        
                 





