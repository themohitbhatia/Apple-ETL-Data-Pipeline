class DataSource:
    # Abstract Class
    
    def __init__(self, path):
        self.path = path
    
    def get_dataframe(self):
        # Abstract Method - Function will be defined in sub-classess
        
        raise ValueError("Not Implemented")

class CSVDataSource(DataSource):

    def get_dataframe(self):
        return (
            spark.read.format("csv")\
            .option("header", True)\
            .load(self.path)
        )

class ParquetDataSource(DataSource):

    def get_dataframe(self):
        return (
            spark.read.format("parquet")\
            .load(self.path)
        )

class DeltaDataSource(DataSource):

    def get_dataframe(self):

        delta_table_name = self.path
        return (
            spark.read.table(delta_table_name)
        )

def get_data_source(source_type, source_uri):
    if source_type == 'csv':
        return CSVDataSource(source_uri)
    elif source_type == 'parquet':
        return ParquetDataSource(source_uri)
    elif source_type == 'delta':
        return DeltaDataSource(source_uri)
    else:
        raise ValueError(f"Not implemented for source_type: {source_type}")