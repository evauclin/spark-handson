import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def categorize_category(category):
    category = int(category)
    if category < 6:
        return "food"
    else:
        return "furniture"


categorize_category_udf = udf(categorize_category, StringType())


def main():
    spark = SparkSession.builder.appName("python_udf").master("local[*]").getOrCreate()
    df = spark.read.csv("src/resources/exo4/sell.csv", header=True)
    df = df.withColumn("category_name", categorize_category_udf(df["category"]))

    start_time = time.time()
    # df.write.csv("resultat1.csv", header=True, mode="overwrite")
    df.count()
    df = df.withColumn("time", f.lit(time.time() - start_time))
    df.show()


if __name__ == "__main__":
    main()
