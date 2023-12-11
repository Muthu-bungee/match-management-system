from match_deleter.src.extractor.data_fetcher import Data_Fetcher
from match_deleter.src.deleter.match_deleter import Match_Deleter
from match_deleter.src.loader.mdw_updater import MDW_Updater

from pyspark.sql import DataFrame

class Match_Deleter_Runner:
    def __init__(self, args: dict, spark) -> None:
        self.args = args
        self.spark = spark
    
    def data_extraction(self):
        fetcher = Data_Fetcher(self.args, self.spark)
        mdw = fetcher.fetch_match_warehouse()
        deleted_matches = fetcher.fetch_deleted_matches()
        return mdw, deleted_matches
        
    def match_deletion(self, mdw: DataFrame, deleted_matches: DataFrame):
        deleted_matches = Match_Deleter(mdw, deleted_matches).fetch_deactivate_matches()
        return deleted_matches
        
    def mdw_updation(self, deleted_matches: DataFrame):
        MDW_Updater(self.args, deleted_matches).update_mdw()
    
    def run(self):
        mdw, deleted_matches = self.data_extraction()
        deleted_matches = self.match_deletion(mdw, deleted_matches)
        self.mdw_updation(deleted_matches)
