import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window


def main():
    start_time = time.time()

    spark = SparkSession.builder.appName("exo4").master("local[*]").getOrCreate()

    df = spark.read.csv("src/resources/exo4/sell.csv", header=True)

    df_raw = df.withColumn(
        "category_name",
        (f.when(f.col("category") < 6, "food").otherwise(("furniture"))),
    )
    end_time = time.time()
    df_raw.count()
    print("Execution time: ", end_time - start_time)

    df_formated = df_raw.withColumn("date", f.to_date("date"))

    # calculate_total_price_per_category_per_day(df_formated).show()

    calculate_total_price_per_category_per_day_last_30_days(df_formated)


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


if __name__ == "__main__":
    main()
