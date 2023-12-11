from match_deactivator.src.extractor.data_fetcher import Data_Fetcher
from match_deactivator.src.deactivator.deactivator import Match_Deactivator
from match_deactivator.src.loader.mdw_updater import MDW_Updater

from pyspark.sql import DataFrame

class Match_Deactivator_Runner:
    def __init__(self, args: dict, spark) -> None:
        self.args = args
        self.spark = spark
    
    def data_extraction(self):
        fetcher = Data_Fetcher(self.args, self.spark)
        mdw = fetcher.fetch_match_warehouse()
        deactivated_matches = fetcher.fetch_deactivated_matches()
        return mdw, deactivated_matches
        
    def match_deactivation(self, mdw: DataFrame, deactivated_matches: DataFrame):
        deactivated_matches = Match_Deactivator(mdw, deactivated_matches).fetch_deactivate_matches()
        return deactivated_matches
        
    def mdw_updation(self, deactivated_matches: DataFrame):
        MDW_Updater(self.args, deactivated_matches).update_mdw()
    
    def run(self):
        mdw, deactivated_matches = self.data_extraction()
        deactivated_matches = self.match_deactivation(mdw, deactivated_matches)
        self.mdw_updation(deactivated_matches)
