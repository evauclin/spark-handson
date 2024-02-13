import pyspark.sql.functions as f
from pyspark.sql import SparkSession , DataFrame




def my_group_by(df : DataFrame , column_name : str) -> DataFrame:
    '''This function groups the dataframe by a column name and counts the number of people in each group.
      It returns a dataframe with the groups ordered by the number of people in descending order and the column_name in ascending order.'''
    return df.groupBy(column_name) \
        .count() \
        .withColumnRenamed("count", "nb_people") \
        .orderBy(f.col("nb_people").desc(), f.col(column_name).asc())

def main():
    '''This function reads the parquet file created by the clean.py script, aggregates the data and writes the result in a CSV file.'''

    spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("aggregate") \
    .getOrCreate()
    
    df = spark.read.option("header", "true").parquet("data/exo2/output")
    df.show()
    df_depart_grouped = my_group_by(df, "departement")
    df_depart_grouped.write.mode("overwrite").csv("data/exo2/aggregate")


if __name__ == "__main__":
    main()
