from modules import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, lit, coalesce, greatest, row_number, expr, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
from pyspark.sql.window import Window
import logging


# Initialize the Spark session
spark = SparkSession.builder.appName("exercise_2").getOrCreate()

def get_max_value_as_string(df, column_name):
    # Get the maximum value of a specified column as a string
    max_value_row = df.select(spark_max(col(column_name)).alias("max_value")).collect()[0]
    return str(max_value_row["max_value"])

# Main function to orchestrate the database setup
def main():
    # Set up logging
    setup_logging("logs/exercise_two.log")

    try:
        # Load historical data if it exists
        try:
            df_purchase_log_history = spark.table("purchase_log_history").cache()

            # Filter active and inactive purchase data
            df_active_purchase_data = df_purchase_log_history.filter(col("is_active") == 1)
            df_inactive_purchase_data = df_purchase_log_history.filter(col("is_active") == 0)

        except:
            logging.info("The table purchase_log_history does not exist")

            # Define schema for the historical data
            schema = StructType([
                StructField("transaction_datetime", TimestampType(), True)
                , StructField("transaction_date", TimestampType(), True)
                , StructField("purchase_id", IntegerType(), True)
                , StructField("buyer_id", IntegerType(), True)
                , StructField("prod_item_id", IntegerType(), True)
                , StructField("item_quantity", IntegerType(), True)
                , StructField("purchase_value", DoubleType(), True)
                , StructField("order_date", DateType(), True)
                , StructField("release_date", DateType(), True)
                , StructField("producer_id", IntegerType(), True)
                , StructField("subsidiary", StringType(), True)
                , StructField("is_active", IntegerType(), True)
            ])

            # Create empty DataFrames if the historical table does not exist
            df_active_purchase_data = spark.createDataFrame([], schema)
            df_inactive_purchase_data = spark.createDataFrame([], schema)

        # Get the last transaction datetime from the active purchase data
        last_transaction_datetime = get_max_value_as_string(df_active_purchase_data, "transaction_datetime")

        # Filter new data from the source tables based on the last transaction datetime
        df_purchase = (
            spark.table("purchase")
            .filter(
                (col("transaction_datetime") > lit(last_transaction_datetime))
                | (col("transaction_datetime").isNull())
            )
        )

        df_product_item = (
            spark.table("product_item")
            .filter(
                (col("transaction_datetime") > lit(last_transaction_datetime))
                | (col("transaction_datetime").isNull())
            )
        )

        df_purchase_extra_info = (
            spark.table("purchase_extra_info")
            .filter(
                (col("transaction_datetime") > lit(last_transaction_datetime))
                | (col("transaction_datetime").isNull())
            )
        )

        # Combine all transactions into a single DataFrame
        df_purchases_transactions = (
            df_purchase
            .select(
                col("transaction_datetime")
                , col("purchase_id")
            )
            .union(
                df_product_item
                .select(
                    col("transaction_datetime")
                    , col("purchase_id")
                )
            )
            .union(
                df_purchase_extra_info
                .select(
                    col("transaction_datetime")
                    , col("purchase_id")
                )
            )
        )

        # Define window specifications to get the most recent records
        window_spec = (
            Window
            .partitionBy(
                coalesce(col("a.transaction_date"), col("e.transaction_date"))
                , coalesce(col("a.purchase_id"), col("e.purchase_id"))
            )
            .orderBy(
                col("b.transaction_datetime").desc()
                , col("c.transaction_datetime").desc()
                , col("d.transaction_datetime").desc()
                , col("e.transaction_datetime").desc()
            )
        )

        window_spec2 = (
            Window
            .partitionBy(
                coalesce(col("a.purchase_id"), col("e.purchase_id"))
            )
            .orderBy(
                coalesce(col("a.transaction_datetime"), col("e.transaction_datetime"))
            )
        )

        # Get new active data by joining the transactions with the source tables and filtering the most recent record per purchase_id
        df_new_active_data = (
            df_purchases_transactions
            .alias("a")
            .join(
                df_purchase.alias("b")
                , (col("a.purchase_id") == col("b.purchase_id"))
                & (col("a.transaction_datetime") >= col("b.transaction_datetime"))
                , "left"
            )
            .join(
                df_product_item.alias("c")
                , (col("a.purchase_id") == col("c.purchase_id"))
                & (col("a.transaction_datetime") >= col("c.transaction_datetime"))
                , "left"
            )
            .join(
                df_purchase_extra_info.alias("d")
                , (col("a.purchase_id") == col("d.purchase_id"))
                & (col("a.transaction_datetime") >= col("d.transaction_datetime"))
                , "left"
            )
            .join(
                df_active_purchase_data.alias("e")
                , (col("a.purchase_id") == col("e.purchase_id"))
                & (col("a.transaction_datetime") >= col("e.transaction_datetime"))
                , "full"
            )
            .withColumn("row_number", row_number().over(window_spec))
            .filter(col("row_number") == 1)
            .drop("row_number")
            .withColumn("row_number2", row_number().over(window_spec2))
            .select(
                coalesce(col("a.transaction_datetime"), col("e.transaction_datetime")).alias("transaction_datetime")
                , coalesce(col("a.transaction_date"), col("e.transaction_date")).alias("transaction_date")
                , coalesce(col("a.purchase_id"), col("e.purchase_id")).alias("purchase_id")
                , coalesce(col("b.buyer_id"), col("e.buyer_id")).alias("buyer_id")
                , coalesce(col("b.prod_item_id"), col("e.prod_item_id")).alias("prod_item_id")
                , coalesce(col("c.item_quantity"), col("e.item_quantity")).alias("item_quantity")
                , coalesce(col("c.purchase_value"), col("e.purchase_value")).alias("purchase_value")
                , coalesce(col("b.order_date"), col("e.order_date")).alias("order_date")
                , coalesce(col("b.release_date"), col("e.release_date")).alias("release_date")
                , coalesce(col("b.producer_id"), col("e.producer_id")).alias("producer_id")
                , coalesce(col("d.subsidiary"), col("e.subsidiary")).alias("subsidiary")
                , (
                    when(
                        col("row_number2") == 1
                        , lit(1)
                    )
                    .otherwise(lit(0))
                    .alias("is_active")
                )
            )
        )

        # Get no longer active data
        df_no_longer_active_data = (
            df_active_purchase_data.alias("e")
            .join(
                df_purchase.alias("a")
                , col("a.purchase_id") == col("e.purchase_id")
                , "left"
            )
            .filter(col("a.purchase_id").isNull())
            .select(
                col("e.transaction_datetime")
                , col("e.transaction_date")
                , col("e.purchase_id")
                , col("e.buyer_id")
                , col("e.prod_item_id")
                , col("e.item_quantity")
                , col("e.purchase_value")
                , col("e.order_date")
                , col("e.release_date")
                , col("e.producer_id")
                , col("e.subsidiary")
                , lit(0).alias("is_active")
            )
        )

        # Combine new active data, no longer active data, and inactive purchase data
        final_history_df = df_new_active_data.union(df_no_longer_active_data).union(df_inactive_purchase_data)

        # Save the final historical DataFrame partitioned by transaction_date
        final_history_df.write.mode("overwrite").partitionBy("transaction_date").parquet("path_to_history.parquet")

        # Register the final DataFrame as a temporary table for SQL queries
        final_history_df.createOrReplaceTempView("purchase_log_history")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()