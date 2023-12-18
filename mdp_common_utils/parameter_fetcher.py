
def fetch_parameters(input_args: dict, env):
    env_dependent_args = {
        "match_suggestion" : {
            "database": "ml-mdp-test",
            "table" : "match_suggestion",
            "s3_bucket" : "mdp-ut.east1",
            "s3_prefix" : "match_suggestion"
        },
        "match_warehouse" : {
            "database": "ml-mdp-test",
            "table" : "match_warehouse",
            "s3_bucket" : "mdp-ut.east1",
            "s3_prefix" : "match_warehouse",
            "write_operation" : "upsert",
            "partition_columns" : ["segment","base_source_store","bungee_audit_status","comp_source_store"],
            "record_key" : "pair_id",
            "precombine_field" : "updated_date"
        },
        "match_library" : {
            "database": "match_library",
            "table" : "match_library_snapshot",
        },
        "fastlane" : {
            "database": "ml_internal_uat",
            "table" : "fastlane_dump",
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

def fetch_client_config(env):
    client_config = {
        "cfspharmacy<>cfspharmacy" : {
            "seller_type" : "1p",
            "match_type" : "exact",
            "cardinality" : "1",
        }
    }
    return client_config