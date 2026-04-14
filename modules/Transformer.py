from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast, collect_set, size, array_contains


class Transformer:
    def __init__(self):
        pass

    def transform(self, input_df):
        pass

# Class to find the customers who bought Product2 after buying Product1
class IphoneToAirpodsConversions(Transformer):
    def transform(self, input_df, Product1, Product2):
        transactionInputDF = input_df.get("transactionInputDF")
        print("transactionInputDF in transform")
        transactionInputDF.show()

        WindowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
        transform_df = transactionInputDF.withColumn("next_product_name", lead("product_name").over(WindowSpec))

        print(f"Customer who bought {Product2} after buying {Product1}")
        transform_df.orderBy("customer_id", "transaction_date", "product_name").show()

        filtered_df = transform_df.filter((col("product_name") == Product1) & (col("next_product_name") == Product2))
        filtered_df.orderBy("customer_id", "transaction_date", "product_name").show()

        customer_df = input_df.get("customer_df")
        customer_df.show()

        print("Joined DF")
        join_df = customer_df.join(broadcast(filtered_df), "customer_id")
        join_df.show()

        return join_df.select("customer_id", "customer_name", "location")

class ExclusiveIphoneAndAirpodsBuyers(Transformer):

    # Customers who bought iPhone and Airpods and nothing else
    def transform(self, input_df, Product1, Product2):

        transactionInputDF = input_df.get("transactionInputDF")
        print("transactionInputDF in transform")
        transactionInputDF.show()

        grouped_df = transactionInputDF.groupBy("customer_id").agg(
            collect_set("product_name").alias("products")
        )
        grouped_df.show()

        filtered_df = grouped_df.filter(
            (array_contains(col("products"), "iPhone")) &
            (array_contains(col("products"), "AirPods")) &
            (size(col("products")) == 2)
        )

        filtered_df.show()

        customer_df = input_df.get("customer_df")
        customer_df.show()

        print("Joined DF")
        join_df = customer_df.join(broadcast(filtered_df), "customer_id")
        join_df.show()

        join_df.select("customer_id", "customer_name", "location").show()

        return join_df.select("customer_id", "customer_name", "location")