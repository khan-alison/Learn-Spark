from pyspark.sql import SparkSession
from pyspark.sql.functions import max
from pyspark.sql.functions import desc

spark = SparkSession.builder \
    .appName("SimpleApp") \
    .getOrCreate()

myRange = spark.range(1000).toDF("number")

# COMMAND ----------

myRange.where("number % 2 = 0").show()

# COMMAND ----------

flightData2015 = spark.read.option("inferSchema", "true") \
    .option("header", "true") \
    .csv("../data/flight-data/csv/2015-summary.csv")

flightData2015.show(3)

flightData2015.sort("count").explain()

spark.conf.set("spark.sql.shuffle.partitions", 5)

flightData2015.sort("count").show(2)

flightData2015.createOrReplaceTempView("flight_data_2015")

sqlWay = spark.sql("""
          SELECT DEST_COUNTRY_NAME, count(1)
          FROM flight_data_2015
          GROUP BY DEST_COUNTRY_NAME
          """)

dataFrameWay = flightData2015 \
    .groupBy("DEST_COUNTRY_NAME") \
    .count()

sqlWay.explain()
dataFrameWay.explain()

flightData2015.select(max("count")).show(1)

maxSql = spark.sql("""
    SELECT DEST_COUNTRY_NAME, SUM(count) as DESTINATION_TOTAL
    FROM flight_data_2015
    GROUP BY DEST_COUNTRY_NAME
    ORDER BY DESTINATION_TOTAL DESC
    LIMIT(5)""")

maxSql.show()

flightData2015\
    .groupBy("DEST_COUNTRY_NAME")\
    .sum("count")\
    .withColumnRenamed("sum(count)", "DESTINATION_TOTAL")\
    .sort(desc("DESTINATION_TOTAL"))\
    .limit(5)\
    .show()

flightData2015 \
    .groupBy("DEST_COUNTRY_NAME") \
    .sum("count") \
    .withColumnRenamed("sum(count)", "destination_total") \
    .sort(desc("destination_total")) \
    .limit(5) \
    .explain()
