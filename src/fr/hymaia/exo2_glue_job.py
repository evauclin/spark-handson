import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

from src.fr.hymaia.glue_src import main

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    glueContext = GlueContext(spark.sparkContext)
    job = Job(glueContext)
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "df1", "output"])
    job.init(args["JOB_NAME"], args)

    df_1 = args["df1"]
    output = args["output"]

    main(spark, df_1, output)

    job.commit()
