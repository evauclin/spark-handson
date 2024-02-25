import pyspark.sql.functions as f
from pyspark.sql import SparkSession, DataFrame


def wordcount(df: DataFrame, col_name: str) -> DataFrame:
    """This function takes a dataframe and a column name as input and returns a dataframe with the word count."""
    return (
        df.withColumn("word", f.explode(f.split(f.col(col_name), " ")))
        .groupBy("word")
        .count()
    )


def main():
    """This function reads the data.csv file, counts the words and writes the result in a CSV file."""

    spark = SparkSession.builder.master("local[*]").appName("Word Count").getOrCreate()

    df = spark.read.option("header", "true").csv("src/resources/exo1/data.csv")
    wordcount_output = wordcount(df, "text")
    wordcount_output.write.mode("overwrite").partitionBy("count").csv(
        "data/exo1/output"
    )


if __name__ == "__main__":
    main()
