import json
from pyspark.sql import DataFrame, SparkSession
from awsglue.context import GlueContext
import boto3
from pyspark.sql.functions import *
import pkg_resources
from bungee_utils.python_utils.glue_catalog import GlueCatalog


class HudiDataFrameWriter:

    def __init__(self, glue_context: GlueContext, hudi_config: dict, database: str, table: str, s3_bucket: str, s3_prefix: str, write_operation: str):
        """
        Initialize the HudiDataFrameWriter.
        """
        self.db_name = database
        self.table_name = table
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.hudi_config = hudi_config
        self.partition_columns, self.path = self.get_table_path_and_partition(self.db_name, self.table_name)
        self.configs = self.generate_hoodie_config()
        self.write_operation = self.verify_write_operation(write_operation)
        
    def verify_write_operation(self, write_operation: str):
        if write_operation not in ['init', 'upsert', 'delete']:
            raise ValueError("write_operation must be 'init', 'upsert', or 'delete'")
        else:
            return write_operation

    def generate_hoodie_config(self):
        table_config = {
            "hoodie.table.name": self.table_name,
            "hoodie.datasource.hive_sync.database": self.db_name,
            "hoodie.datasource.hive_sync.table": self.table_name,
            "hoodie.datasource.hive_sync.partition_fields": self.partition_columns,
            "path": self.path,
            "hoodie.datasource.write.precombine.field": self.hudi_config['precombine_field'], 
            "hoodie.datasource.write.recordkey.field": self.hudi_config['record_key'], 
            "hoodie.datasource.write.partitionpath.field": self.partition_columns,
        }
        configs = {
            'init': {**table_config, **self.hudi_config['commonConfig'], **self.hudi_config['partitionDataConfig'], **self.hudi_config['initLoadConfig']},
            'upsert': {**table_config, **self.hudi_config['commonConfig'], **self.hudi_config['partitionDataConfig'], **self.hudi_config['incrementalWriteConfig']},
            'delete': {**table_config, **self.hudi_config['commonConfig'], **self.hudi_config['partitionDataConfig'], **self.hudi_config['deleteWriteConfig']}
        }
        return configs

    def get_table_path_and_partition(self, db_name, table_name):
        if self.args['match_data_warehouse']['write_operation'] == 'init':
            path = f's3://{self.s3_bucket}/{self.s3_prefix}'
            partition_columns = ','.join(self.args['match_data_warehouse']['partition_columns'])
            return partition_columns, path
        else:
            catalog_metadata = GlueCatalog(db_name, table_name)
            path = catalog_metadata.get_table_location()
            partition_columns = ','.join(catalog_metadata.get_table_partition())
            return partition_columns, path

    def write_hudi_data(self, spark_df: DataFrame, write_operation: str=None):
        """
        Write data to the Hudi table.
        """
        spark_df.write.format("org.apache.hudi") \
            .options(**self.configs[write_operation]) \
            .mode('append') \
            .save()
