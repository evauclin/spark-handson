import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
# TODO : import custom spark code dependencies
import pyspark.sql.functions as f
from pyspark.sql import SparkSession, DataFrame
from src.fr.hymaia.exo1.wordcount import wordcount

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    glueContext = GlueContext(spark.sparkContext)
    job = Job(glueContext)
    args = getResolvedOptions(sys.argv, ["wordcount", "PARAM_1", "PARAM_2"])  
    job.init(args['wordcount'], args)

    PARAM_1 = args["data_path"] 
    PARAM_2 = args["col_name"]

    # Read the DataFrame from the specified data path
    '''df = spark.read.option("header", "true").csv(PARAM_1)

    # Call the wordcount function
    wordcount_output = wordcount(df, PARAM_2)

    # Write the wordcount output to the specified output path
    wordcount_output.write.mode('overwrite').partitionBy("count").csv("data/exo1/output")    
    wordcount_output.show()'''
    
    print("Hello World!")
    job.commit()