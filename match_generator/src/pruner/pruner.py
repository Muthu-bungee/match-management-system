from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql import DataFrame

class Pruner:
    def __init__(self,config):
        self.config=config

    def pruneOnSellerType(self, df,seller_type):
        if seller_type== '1P' or seller_type=='3P':
            df=df[df['seller_flag']==seller_type]
        return df
   
    def getMatch(self, match_df,pair_id):  
        return match_df[match_df['pair_id']==pair_id]
    
    def filter_based_on_seller_type(self, seller_type: str, match_suggestion: DataFrame, match_data_warehouse: DataFrame):
        match_data_warehouse_pair_id = match_data_warehouse.filter( col('') == '' & col('') == '').select('pair_id')
        old_pair_id = match_data_warehouse_pair_id.join(match_suggestion.select('pair_id'), 'pair_id', 'inner')
        
        old_matches_from_match_suggestions = match_suggestion.join(old_pair_id, 'pair_id', 'inner')
        old_matches_from_match_data_warehouse = match_data_warehouse.join(old_pair_id, 'pair_id', 'inner')
        
        window_spec = Window.partitionBy("pair_id").orderBy("seller_type")
        df_with_row_number = old_matches_from_match_suggestions.withColumn("row_num", row_number().over(window_spec))
        result_df = df_with_row_number.filter(col("row_num") == 1).drop("row_num")
         
        seller_type = seller_type.lower()
        if seller_type == '1P':
            old_matches_from_match_suggestions.filter( col("seller_type") == "3p" )
            pass
        elif seller_type == '3P':
            old_matches_from_match_suggestions.filter( col("seller_type") == "1p" )
            pass
        elif seller_type == '1p_or_3P':
            pass
        elif seller_type == '1p_over_3p':
            window_spec = Window.partitionBy("pair_id").orderBy("seller_type")
            df_with_row_number = old_matches_from_match_suggestions.withColumn("row_num", row_number().over(window_spec))
            result_df = df_with_row_number.filter(col("row_num") == 1).drop("row_number")
        

    '''def hasTypeMatch(self, match,match_type):
        filtered_match =match.filter(col('BUNGEE_AUDIT_STATUS') == 'EXACT_MATCH')
        if filtered_match.rdd.getNumPartitions()  0:
            return  match_type in ['EXACT_MATCH','EXACT_OR_EQUIVALENT','EXACT_OVER_EQUIVALENT']
                 
        if match[match['BUNGEE_AUDIT_STATUS'] == 'EQUIVALENT_MATCH'].shape[0] >0:
            return match_type in ('EQUIVALENT_MATCH','EXACT_OR_EQUIVALENT')

        
    def hasSellerMatch(self,match,seller_type):
        if match[match['seller_flag']=='1P'].shape[0]>0:
            return seller_type in ['1P','1P_OR_3P','1p_OVER_3P']
   
        if match[match['seller_flag']=='3P'].shape[0]>0:
            return seller_type in ['1P_OR_3P','3P']'''
    
    def prune(self,match_df):
        
       
       

    def executor(self,row):
        print('===.',row)
        if (self.hasTypeMatch(row,self.config['match_type']) and (self.hasSellerMatch(row,self.config['seller_type']))):
            msg=msg+'match present based on match type and seller type config'+row['pair_id']
            row['status']='pruned'
            row['reason']=msg   
        return row
    
  
    
        
# INPUT
#   what we have aggregated in previous step (list of all match suggestion present with us)
#   complete match data warehouse (list of all match suggestion present in mdw)

# PROCESSING
   