from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.types import *

def get_max_id(df: DataFrame) -> int:
    if df == None or len(df.head(1)) == 0:
        return 0
    max_id = int(df.select(max(df.id)).collect()[0][0])
    print(max_id)
    return max_id

def combine_dfs(df_list: List[DataFrame]) -> DataFrame:
    print("Combining dfs")
    combined_df = reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), df_list)
    # print("Combined df size = {}".format(combined_df.count()))
    return combined_df

def insert_id(df: DataFrame, start_idx: str):
    print("Inserting ids")
    updated_schema = StructType(df.schema.fields[:] + [StructField("id", LongType(), False)])
    zipped_rdd = df.rdd.zipWithIndex().map(lambda x: (x[0], x[1] + start_idx))
    id_df = (zipped_rdd.map(lambda ri: Row(*list(ri[0]) + [ri[1]])).toDF(updated_schema))
    # print("New id_uuid = {}".format(new_id_uuid_df.count()))
    return id_df
