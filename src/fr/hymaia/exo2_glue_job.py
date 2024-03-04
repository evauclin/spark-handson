import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# TODO : import custom spark code dependencies
from pyspark.sql import SparkSession

from src.fr.hymaia.exo1.wordcount import wordcount

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    glueContext = GlueContext(spark.sparkContext)
    job = Job(glueContext)
    args = getResolvedOptions(sys.argv, ["wordcount"], "PARAM_1")
    job.init(args["wordcount"], args)

    PARAM_1 = args["PARAM"]
    # PARAM_2 = args["col_name"]

    df = spark.read.option("header", "true").csv(PARAM_1)

    wordcount_output = wordcount(df, "text")

    wordcount_output.show()

    job.commit()
