import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
import time


def main():
    start_time = time.time()

    spark = (
        SparkSession.builder.appName("exo4")
        .config("spark.jars", "src/resources/exo4/udf.jar")
        .master("local[*]")
        .getOrCreate()
    )

    def addCategoryName(col):
        sc = spark.sparkContext
        add_category_name_udf = (
            sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
        )
        return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

    df = spark.read.csv("src/resources/exo4/sell.csv", header=True)

    df = df.withColumn("category_name", addCategoryName(df["category"]))
    df.count()

    end_time = time.time()
    print("Execution time: ", end_time - start_time)
