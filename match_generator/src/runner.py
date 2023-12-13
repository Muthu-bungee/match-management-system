import sys
import pkg_resources
import configparser
import bungee_utils.spark_utils.function.dataframe_util as utils

# from awsglue.context import GlueContext
# from awsglue.dynamicframe import DynamicFrame
# from awsglue.job import Job
# from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from match_generator.src.data_reader.data_reader import DataFetcher
from match_generator.src.pruner.pruner import Match_Pruner
from match_generator.src.aggregator.aggregator import Aggregator
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *

class MatchGenerator:
    def __init__(self, glue_context, spark: SparkSession, args: dict, env: str) -> None:
        self.glue_context=glue_context
        # resource_stream=pkg_resources.resource_string('match_generator','mdp-config.ini')
        # self.mdp_config=configparser.ConfigParser()
        # self.mdp_config.read_string(resource_stream.decode("utf-8"))
        # print('match generator initiation done')
        self.args = self.fetch_arguments(args)
        self.client_config = self.fetch_client_config()
        self.env = env
        self.spark = spark
        self.customer_list = []
        self.customer_match_list = []
    
    def run(self):
        match_suggestion, mw = self.extraction()
        aggregated_match_suggestion = self.match_aggregation(match_suggestion, mw)
        directed_matches = self.convert_to_direct_pair_matches(aggregated_match_suggestion)
        match_suggestion = self.match_pruning(mw, directed_matches)
        if self.env != "prod":
            match_suggestion.show()
        
    def extraction(self):
        print("data fetching started")
        reader = DataFetcher(self.args, self.spark, self.env)
        match_suggestion = reader.fetch_match_suggestion()
        mw = reader.fetch_mdw()
        print("Data fetching finished")
        return match_suggestion, mw

    def match_pruning(self, mw: DataFrame, directed_matches: DataFrame):
        
        for customer, config in self.client_config.items():
            self.customer_list.append(customer)
            customer_matches = directed_matches.filter(col("base_source_store") == customer)
            if self.env != "prod":
                customer_matches.show(truncate=False, n = 100)
            pruner = Match_Pruner(mw, customer_matches, config, self.env)
            updated_customer_matches = pruner.prune_matches()
            self.customer_match_list.append(updated_customer_matches)
        
        customer_match_suggestion = utils.combine_dfs(self.customer_match_list)
        customer_match_suggestion.cache()
        non_customer_match_suggestion = directed_matches.filter(~(col('base_source_store').isin(self.customer_list)))
        non_customer_match_suggestion.cache()
        match_suggestion = utils.combine_dfs([customer_match_suggestion, non_customer_match_suggestion])
        match_suggestion.cache()
        if self.env != "prod":
            print("directed match count = ", directed_matches.count() )
            print("customer match count = ", customer_match_suggestion.count())
            print("non_customer match count = ", non_customer_match_suggestion.count())
            print("combined match count = ", match_suggestion.count())
        return match_suggestion

    def match_aggregation(self, match_suggestion, mw):
        print("match aggregation started")
        aggregator=Aggregator( match_suggestion, mw, self.env)
        aggregated_match_suggestion = aggregator.aggregate_matches()
        print('match aggregation completed')
        return aggregated_match_suggestion



    def fetch_arguments(self, input_args):
        env_dependent_args = {
            "match_suggestion" : {
                "database": "ml-mdp-test",
                "table" : "match_suggestion",
                "s3_bucket" : "mdp-ut.east1",
                "s3_prefix" : "match_suggestion"
            },
            "mdw" : {
                "database": "ml-mdp-test",
                "table" : "match_warehouse",
                "s3_bucket" : "mdp-ut.east1",
                "s3_prefix" : "match_warehouse",
            },
            "hudi_config":{
                "commonConfig" :{
                    "className" : "org.apache.hudi",
                    "hoodie.datasource.hive_sync.use_jdbc":"false", 
                    "hoodie.consistency.check.enabled": "true", 
                    "hoodie.datasource.hive_sync.enable": "true", 
                    "hoodie.index.type": "GLOBAL_SIMPLE",
                    "hoodie.simple.index.update.partition.path": "true",
                    "hoodie.global.simple.index.parallelism": "500" 
                },
                "partitionDataConfig" : { 
                    "hoodie.datasource.write.keygenerator.class" : "org.apache.hudi.keygen.ComplexKeyGenerator",  
                    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor", 
                    "hoodie.datasource.write.hive_style_partitioning": "true"
                },
                "initLoadConfig" : {
                    "hoodie.bulkinsert.shuffle.parallelism": 500,
                    "hoodie.datasource.write.operation": "bulk_insert"
                },
                "incrementalWriteConfig" : {
                    "hoodie.upsert.shuffle.parallelism": 500, 
                    "hoodie.datasource.write.operation": "upsert", 
                    "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS", 
                    "hoodie.cleaner.commits.retained": 5
                },
                "deleteWriteConfig" : {
                    "hoodie.delete.shuffle.parallelism": 500, 
                    "hoodie.datasource.write.operation": "delete", 
                    "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS", 
                    "hoodie.cleaner.commits.retained": 5
                }
            }
        }
        args = {**input_args, **env_dependent_args}
        return args
    
    def fetch_client_config(self):
        client_config = {
            "cfspharmacy<>cfspharmacy" : {
                "seller_type" : "1p",
                "match_type" : "exact",
                "cardinality" : "1",
            }
        }
        return client_config
    
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
                                                
        directed_matches = utils.combine_dfs([match_source_to_dest, match_dest_to_source]).dropDuplicates(["pair_id"])
        
        if self.env != "prod":
            print("undirected matches count ", undirected_matches.count())
            undirected_matches.show(truncate = False, n = 100)
            print("directed matches count ", directed_matches.count())
            directed_matches.show(truncate = False, n = 100)
        print("Converted undirected to directed pairs")
        return directed_matches
        
                 





