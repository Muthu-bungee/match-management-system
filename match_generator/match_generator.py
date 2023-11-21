import sys
sys.path.insert(0,'/home/glue_user/workspace')
import pkg_resources
import configparser

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from match_generator.src.data_reader.data_reader import DataReader
from match_generator.src.pruner.pruner import Pruner
from match_generator.src.aggregator.aggregator import Aggregator


class MatchGenerator:
    def __init__(self,glue_context:GlueContext) -> None:
        self.glue_context=glue_context
        resource_stream=pkg_resources.resource_string('match_generator','mdp-config.ini')
        self.mdp_config=configparser.ConfigParser()
        self.mdp_config.read_string(resource_stream.decode("utf-8"))
        print('match generator initiation done')
    
    def run(self,run_config):
        reader=DataReader(self.glue_context)

        config={}
        config['seller_type']='1P'
        config['match_type']='EXACT_MATCH'
        config['cardinality']='1'
        
        delta_match_df=reader.getDeltaMatches()
        #delta_match_df.show()
        delta_pairs=delta_match_df.select('pair_id').rdd.flatMap(lambda x:x).collect()
        print('delta_match_df printed above',len(delta_pairs))
        

        existing_match_df=reader.getExistingMatches(delta_pairs)
        #existing_match_df.show()
        print('existing_match_df printed above')


        aggregator=Aggregator(self.glue_context)
        match_df=aggregator.aggregate(delta_match_df,existing_match_df)
        print('aggregation completed')

        pruner=Pruner(config)
        pruner.prune(match_df)

        






        