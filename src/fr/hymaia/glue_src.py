from src.fr.hymaia.exo1.wordcount import wordcount


def main(spark, df_1, output_path):

    df = spark.read.option("header", "true").csv(df_1)
    wordcount_output = wordcount(df, "text")
    wordcount_output.write.mode("overwrite").partitionBy("count").csv(output_path)
