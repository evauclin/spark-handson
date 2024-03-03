import time

import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, when


def add_category_name_without_udf(df):
    df = df.withColumn(
        "category_name", when(col("category") < 6, "food").otherwise("furniture")
    )
    return df


def calculate_total_price_per_category_per_day(df):
    window_spec = Window.partitionBy("category", "date")
    df = df.withColumn(
        "total_price_per_category_per_day", f.sum("price").over(window_spec)
    )
    df = df.dropDuplicates(
        ["date", "category_name", "total_price_per_category_per_day"]
    )
    return df


def calculate_total_price_per_category_per_day_last_30_days(df):
    df = df.dropDuplicates(["date", "category_name"])
    window_spec = (
        Window.partitionBy("category_name").orderBy("date").rowsBetween(-29, 0)
    )
    df = df.withColumn(
        "total_price_per_category_per_day_last_30_days",
        f.sum("price").over(window_spec),
    )
    return df.select(
        "id",
        "date",
        "category",
        "price",
        "category_name",
        "total_price_per_category_per_day_last_30_days",
    )


def main():
    spark = SparkSession.builder.appName("no_udf").getOrCreate()

    df = spark.read.csv("src/resources/exo4/sell.csv", header=True)

    df = add_category_name_without_udf(df)

    start_time = time.time()
    df.write.csv("resultat.csv", header=True, mode="overwrite")
    # df.count()
    end_time = time.time()
    print("Execution time without UDF: ", end_time - start_time)
