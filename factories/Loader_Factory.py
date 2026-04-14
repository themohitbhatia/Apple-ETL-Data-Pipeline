class DataSink:
    def __init__(self, df, path, method, params):
        self.df = df
        self.path = path
        self.method = method
        self.params = params
    
    # Abstract Method - Function will be defined in sub-classess
    def load_dataframe(self):
        
        raise ValueError("Not Implemented")

class LoadToFile(DataSink):
    def load_dataframe(self):

        self.df.write.mode(self.method).save(self.path)

class LoadToFileWithPartion(DataSink):
    def load_dataframe(self):

        partitionByColumns = self.params.get("partitionByColumns")
        self.df.write.mode(self.method).partitionBy(*partitionByColumns).save(self.path)

class LoadToDelta(DataSink):
    def load_dataframe(self):

        self.df.write.format("delta").mode(self.method).saveAsTable(self.path)

def get_sink_source(sink_type, df, path, method, params = None):
    if sink_type == "file":
        return LoadToFile(df, path, method, params)
    elif sink_type == "file_with_partition":
        return LoadToFileWithPartion(df, path, method, params)
    elif sink_type == "delta":
        return LoadToDelta(df, path, method, params)
    else:
        return ValueError(f"Not implemented for sink type: {sink_type}")