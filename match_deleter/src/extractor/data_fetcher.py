


class Data_Fetcher:
    def __init__(self, args, spark) -> None:
        self.args = args
        self.spark = spark
        
    def fetch_match_warehouse(self):
        db = self.args["mdw"]["database"]
        table = self.args["mdw"]["table"]
        mdw = self.spark.sql(f"select * from {db}.{table}")
        return mdw
    
    def fetch_deleted_matches(self):
        db = self.args["deactivated_matches"]["database"]
        table = self.args["deactivated_matches"]["table"]
        deactivated_matches = self.spark.sql(f"select * from {db}.{table}")
        return deactivated_matches