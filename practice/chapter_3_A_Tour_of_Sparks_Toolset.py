from pyspark.sql import SparkSession
from pyspark.sql.functions import window, column, desc, col

spark = SparkSession.builder \
    .appName("Structured Streaming") \
    .getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "5")

staticDataFrame = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("../data/retail-data/by-day/*.csv")

staticDataFrame.createOrReplaceGlobalTempView("retail_data")
staticSchema = staticDataFrame.schema

staticDataFrame \
    .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate") \
    .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day")) \
    .sum("total_cost") \
    .show(5)

# streaming code
streamingDataFrame = spark.readStream \
    .schema(staticSchema) \
    .option("maxFilesPerTrigger", 1) \
    .format("csv") \
    .option("header", "true") \
    .load("../data/retail-data/by-day/*.csv")

print(staticDataFrame.isStreaming)

purchaseByCustomerPerHour = streamingDataFrame \
    .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate") \
    .groupBy(
    col("CustomerId"), window(col("InvoiceDate"), "1 day")) \
    .sum("total_cost")
