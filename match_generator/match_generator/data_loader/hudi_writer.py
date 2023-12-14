from pyspark.sql import DataFrame
from pyspark.sql.functions import *
# from bungee_utils.python_utils.glue_catalog import GlueCatalog


class HudiDataFrameWriter:

    def __init__(self, hudi_config: dict, db_info: dict):
        """
        Initialize the HudiDataFrameWriter.
        """
        self.hudi_config = hudi_config
        self.db_info = db_info
        self.partition_columns, self.path = self.get_table_path_and_partition(self.db_info["database"], self.db_info["table"])
        self.configs = self.generate_hoodie_config()
        self.write_operation = self.verify_write_operation(self.db_info["write_operation"])
        
    def verify_write_operation(self, write_operation: str):
        if write_operation not in ['init', 'upsert', 'delete']:
            raise ValueError("write_operation must be 'init', 'upsert', or 'delete'")
        else:
            return write_operation

    def generate_hoodie_config(self):
        table_config = {
            "hoodie.table.name": self.db_info["table"],
            "hoodie.datasource.hive_sync.database": self.db_info["database"],
            "hoodie.datasource.hive_sync.table":  self.db_info["table"],
            "hoodie.datasource.hive_sync.partition_fields": self.partition_columns,
            "path": self.path,
            "hoodie.datasource.write.precombine.field": self.db_info['precombine_field'], 
            "hoodie.datasource.write.recordkey.field": self.db_info['record_key'], 
            "hoodie.datasource.write.partitionpath.field": self.partition_columns,
        }
        configs = {
            'init': {**table_config, **self.hudi_config['commonConfig'], **self.hudi_config['partitionDataConfig'], **self.hudi_config['initLoadConfig']},
            'upsert': {**table_config, **self.hudi_config['commonConfig'], **self.hudi_config['partitionDataConfig'], **self.hudi_config['incrementalWriteConfig']},
            'delete': {**table_config, **self.hudi_config['commonConfig'], **self.hudi_config['partitionDataConfig'], **self.hudi_config['deleteWriteConfig']}
        }
        return configs

    def get_table_path_and_partition(self, db_name, table_name):
        if self.db_info['write_operation'] == 'init':
            path = f's3://{self.db_info["s3_bucket"]}/{self.db_info["s3_prefix"]}'
            partition_columns = ','.join(self.db_info['partition_columns'])
            return partition_columns, path
        else:
            path = f's3://{self.db_info["s3_bucket"]}/{self.db_info["s3_prefix"]}'
            partition_columns = ','.join(self.db_info['partition_columns'])
            return partition_columns, path
        # else:
        #     catalog_metadata = GlueCatalog(db_name, table_name)
        #     path = catalog_metadata.get_table_location()
        #     partition_columns = ','.join(catalog_metadata.get_table_partition())
        #     return partition_columns, path

    def write_hudi_data(self, spark_df: DataFrame):
        """
        Write data to the Hudi table.
        """
        spark_df.printSchema()
        spark_df.show()
        spark_df.write.format("org.apache.hudi") \
            .options(**self.configs[self.db_info["write_operation"]]) \
            .mode('append') \
            .save()
