import pyspark.sql.functions as f
from pyspark.sql import SparkSession
import time

def main():
    start_time = time.time()

    spark = SparkSession.builder.appName(
        "exo4").master("local[*]").getOrCreate()

    df1 = spark.read.csv("src/resources/exo4/sell.csv", header=True)

    df1 = df1.withColumn("category_name", (
        f.when(f.col("category") < 6, "food")
        .otherwise(("furniture")))
    )
    df1 = df1.withColumn("time",f.lit(time.time() - start_time))
    
    df1.show()
    
