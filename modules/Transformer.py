from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast, collect_set, size, array_contains, sum, row_number

class Transformer:
    def __init__(self):
        pass

    def transform(self, input_df):
        pass


# Class to find the customers who bought Product2 after buying Product1
class IphoneToAirpodsConversions(Transformer):

    def transform(self, input_df):
        transactionInputDF = input_df.get("transactionInputDF")
        print("transactionInputDF in transform")
        transactionInputDF.show()

        WindowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
        transform_df = transactionInputDF.withColumn("next_product_name", lead("product_name").over(WindowSpec))

        print(f"Customer who bought AirPods after buying iPhone")
        transform_df.orderBy("customer_id", "transaction_date", "product_name").show()

        filtered_df = transform_df.filter((col("product_name") == "iPhone") & (col("next_product_name") == "AirPods"))
        filtered_df.orderBy("customer_id", "transaction_date", "product_name").show()

        customer_df = input_df.get("customer_df")
        customer_df.show()

        print("Joined DF")
        join_df = customer_df.join(broadcast(filtered_df), "customer_id")
        join_df.show()

        return join_df.select("customer_id", "customer_name", "location")


# Customers who bought iPhone and Airpods and nothing else
class ExclusiveIphoneAndAirpodsBuyers(Transformer):
    
    def transform(self, input_df):

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


# To get the top 3 products in a category based on revenue generated
class CategoryProductLeaderboard(Transformer):

    def transform(self, input_df):
        transaction_df = input_df.get("transactionInputDF")
        print("transactionInputDF in transform")
        transaction_df.show()

        product_df = input_df.get("product_df")
        product_df.show()

        product_df = product_df.replace({"iPhone SE": "iPhone", "AirPods Pro": "AirPods", "MacBook Air": "MacBook"}, subset=["product_name"])

        transaction_joined = transaction_df.join(product_df, on = "product_name", how = "left")

        transaction_joined = transaction_joined.withColumn("price", col("price").cast("int"))

        top_3_products_df = (transaction_joined
            .groupBy("category", "product_name")
            .agg(sum("price").alias("total_revenue"))
            .withColumn("rank", row_number().over(
                Window.partitionBy("category").orderBy(col("total_revenue").desc())
            ))
            .filter(col("rank") <= 3)
            .drop("rank")
        )

        top_3_products_df.show()

        return top_3_products_df