from pyspark.sql.functions import *

class Pruner:
    def __init__(self,config):
        self.config=config

    def pruneOnSellerType(self, df,seller_type):
        if seller_type== '1P' or seller_type=='3P':
            df=df[df['seller_flag']==seller_type]
        return df
   
    def getMatch(self, match_df,pair_id):  
        return match_df[match_df['pair_id']==pair_id]

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
    
  
    
        
    

   