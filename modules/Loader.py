from factories.Loader_Factory import *

class AbstractLoader:
    def __init__(self, transformed_df):
        self.transformed_df = transformed_df

    def sink(self):
        pass

class IphoneToAirpodsConversionsLoader(AbstractLoader):

    def sink(self):
        get_sink_source(
            sink_type = "file",
            df = self.transformed_df,
            path = "/Volumes/apple_data/default/output/airpodsAfterIphone",
            method = "overwrite"
        ).load_dataframe()

class ExclusiveIphoneAirpodsLoader(AbstractLoader):

    def sink(self):

        params = {
            "partitionByColumns": ["location"]
        }

        get_sink_source(
            sink_type = "file_with_partition",
            df = self.transformed_df,
            path = "/Volumes/apple_data/default/output/Iphoneandairpods",
            method = "overwrite",
            params = params
        ).load_dataframe()

class CategoryProductLeaderboardLoader(AbstractLoader):

    def sink(self):
        get_sink_source(
            sink_type = "file",
            df = self.transformed_df,
            path = "/Volumes/apple_data/default/output/CategoryProductLeaderboardLoader",
            method = "overwrite"
        ).load_dataframe()