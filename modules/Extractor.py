from factories.Reader_Factory import *

# Abstract Class
class Extractor:
    def __init__(self):
        pass

    def extract(self):
        pass

# Child Class
class DataExtractor(Extractor):

    def extract(self):
        transactionInputDF = get_data_source(
                "csv",
                "/Volumes/apple_data/default/files/Transaction_Updated.csv"
            ).get_dataframe()

        transactionInputDF.orderBy("customer_id", "transaction_date").show()

        customer_df = get_data_source(
            "delta",
            "apple_data.default.customer_updated"
        ).get_dataframe()

        input_df = {
            "transactionInputDF": transactionInputDF,
            "customer_df" : customer_df
        }

        return input_df