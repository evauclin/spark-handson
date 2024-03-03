import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession


def departement_format(df: DataFrame) -> DataFrame:
    """This function adds a column "departement" to the dataframe based on the zip code column. It returns the modified dataframe."""
    return df.withColumn(
        "departement",
        f.when((f.col("zip") >= 20_000) & (f.col("zip") <= 20_190), "2A")
        .when((f.col("zip") <= 20_999) & (f.col("zip") > 20_190), "2B")
        .when(f.length(f.col("zip")) == 3, f.substring(f.col("zip"), 1, 2))
        .when(f.length(f.col("zip")) == 2, f.col("zip"))
        .when(f.length(f.col("zip")) == 1, f.concat(f.lit("0"), f.col("zip")))
        .otherwise(f.substring(f.col("zip"), 1, 2)),
    )


def age_filter(df: DataFrame, column_name: str) -> DataFrame:
    """This function filters the dataframe based on the age column. It keeps only the rows where the age is greater or equal to 18."""
    return df.filter(f.col(column_name) >= 18)


def my_join(df1: DataFrame, df2: DataFrame, column_name: str) -> DataFrame:
    """This function joins two dataframes on a column name."""
    return df1.join(df2, column_name, "inner")


def data_cleaning(df_clients: DataFrame, df_city: DataFrame) -> None:
    """This function cleans the data by removing the rows with null values."""
    df_clients_filtered = age_filter(df_clients, "age")
    df_joined = my_join(df_clients_filtered, df_city, "zip")
    df_cleaned = departement_format(df_joined)
    return df_cleaned


def main():
    """This function reads the clients_bdd.csv and city_zipcode.csv files, cleans the data and writes the result in a parquet file."""

    spark = SparkSession.builder.master("local[*]").appName("clean").getOrCreate()

    df_clients = spark.read.option("header", "true").csv(
        "src/resources/exo2/clients_bdd.csv"
    )
    df_city = spark.read.option("header", "true").csv(
        "src/resources/exo2/city_zipcode.csv"
    )

    df_cleaned = data_cleaning(df_clients, df_city)

    df_cleaned.write.mode("overwrite").parquet("data/exo2/output")


if __name__ == "__main__":
    main()
