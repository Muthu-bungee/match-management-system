from match_generator.data_loader.hudi_writer import HudiDataFrameWriter
from pyspark.sql import DataFrame

class DataLoader:
    def __init__(self, args: dict) -> None:
        self.mw_writer = HudiDataFrameWriter(args["hudi_config"], args["match_warehouse"])
        
    def save_date_to_db(self, mw: DataFrame):
        self.mw_writer.write_hudi_data(mw)
    
    