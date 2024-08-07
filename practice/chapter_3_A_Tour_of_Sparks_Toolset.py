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

staticDataFrame.createOrReplaceTempView("retail_data")
staticSchema = staticDataFrame.schema

staticDataFrame \
    .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate") \
    .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day")) \
    .sum("total_cost") \
    .show(5)

streamingDataFrame = spark.readStream \
    .schema(staticSchema) \
    .option("maxFilesPerTrigger", 1) \
    .format("csv") \
    .option("header", "true") \
    .load("../data/retail-data/by-day/*.csv")

print(streamingDataFrame.isStreaming)

purchaseByCustomerPerHour = streamingDataFrame \
    .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate") \
    .groupBy(
    col("CustomerId"), window(col("InvoiceDate"), "1 day")) \
    .sum("total_cost")

purchaseByCustomerPerHour.writeStream \
    .format("memory") \
    .queryName("customer_purchase") \
    .outputMode("complete") \
    .start()

spark.sql("""
    SELECT * FROM customer_purchase
    ORDER BY `SUM(total_cost)` DESC
    """) \
    .show(5)


# Machine Learning and Advanced Analytics
from pyspark.sql.functions import  date_format, col
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans

preppedDataFrame = staticDataFrame\
    .na.fill(0)\
    .withColumn("day_of_week", date_format(col("InvoiceDate"), "EEE"))\
    .coalesce(5)

trainDataFrame = preppedDataFrame\
    .where("InvoiceDate < '2011-07-01'")
testDataFrame = preppedDataFrame\
    .where("InvoiceDate >= '2011-07-01'")

print(trainDataFrame.count())
trainDataFrame.show()
print(testDataFrame.count())
testDataFrame.show()

indexer = StringIndexer()\
    .setInputCol("day_of_week")\
    .setOutputCol("day_of_week_index")

encoder = OneHotEncoder()\
    .setInputCol("day_of_week_index")\
    .setOutputCol("day_of_week_encoded")

vectorAssembler = VectorAssembler()\
    .setInputCols(["UnitPrice", "Quantity", "day_of_week_encoded"])\
    .setOutputCol("features")

transformationPipeline = Pipeline()\
    .setStages([indexer, encoder, vectorAssembler])

fittedPipeline = transformationPipeline.fit(trainDataFrame)
transformedTraining = fittedPipeline.transform(trainDataFrame)
transformedTraining.cache()
transformedTraining.show()

kmeans = KMeans()\
    .setK(20)\
    .setSeed(1)

kmModel = kmeans.fit(transformedTraining)
transformedTest = fittedPipeline.transform(testDataFrame)
training_cost = kmModel.summary.trainingCost
print(training_cost)