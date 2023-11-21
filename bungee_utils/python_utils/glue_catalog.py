from pyspark.sql import functions as F
import boto3

class GlueCatalog:
    def __init__(self, database_name, table_name, output_location='s3://aws-athena-query-results-209656230012-us-east-1/Unsaved/'):
        self.database_name = database_name
        self.table_name = table_name
        self.output_location = output_location
        self.client = boto3.client('athena')

        self.glue_client = boto3.client('glue')
        self.metadata = self.glue_client.get_table(
            DatabaseName=self.database_name,
            Name=self.table_name
        )

    def add_partitions(self, df):
        partition_columns = self.get_table_partition()
        unique_values = self._get_unique_values(df, partition_columns)
        partitions_string = self._create_partition_clauses(unique_values, partition_columns)
        response = self._run_query(partitions_string)
        return response

    def get_table_location(self):
        return self.metadata['Table']['StorageDescriptor']['Location']

    def get_table_partition(self):
        return [i['Name'] for i in self.metadata['Table']['PartitionKeys']]


    def _get_unique_values(self, df, partition_columns):
        return df.select(*partition_columns).distinct().collect()

    def _create_partition_clauses(self, unique_values, partition_columns):
        partition_clauses = []
        for row in unique_values:
            partition_clause = "PARTITION (" + ", ".join([f"{col}='{row[col]}'" for col in partition_columns]) + ")"
            partition_clauses.append(partition_clause)
        return '\n'.join(partition_clauses)

    def _run_query(self, partitions_string):
        query = f"""ALTER TABLE {self.database_name}.{self.table_name} ADD IF NOT EXISTS
          {partitions_string}
        """
        print(query)
        response = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': self.database_name},
            ResultConfiguration={'OutputLocation': self.output_location}
        )
        return response