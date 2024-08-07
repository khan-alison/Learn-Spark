from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Structured_API_overview")\
    .getOrCreate()

df = spark.range(500).toDF("number")
transformedDf = df.select((df["number"] + 10).alias("number_plus_10"))

df.show()
transformedDf.show()

spark.range(2).collect()

b = ByteType()