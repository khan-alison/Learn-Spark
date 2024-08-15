from pyspark.sql import SparkSession

# Define the absolute path to your SQLite JDBC driver
sqlite_jdbc_jar_path = "/Users/khanhnn/Developer/DE/spark/practice_spark/jars/sqlite-jdbc-3.34.0.jar"
postgres_jdbc_jar_path="/Users/khanhnn/Developer/DE/spark/practice_spark/jars/postgresql-42.7.3.jar"
# Initialize the SparkSession with the correct JDBC jar
spark = SparkSession.builder\
    .appName("Data Sources")\
    .config("spark.jars", f"{sqlite_jdbc_jar_path},{postgres_jdbc_jar_path}")\
    .config("spark.driver.extraClassPath", f"{sqlite_jdbc_jar_path}:{postgres_jdbc_jar_path}")\
    .config("spark.executor.extraClassPath", f"{sqlite_jdbc_jar_path}:{postgres_jdbc_jar_path}")\
    .config("spark.sql.shuffle.partitions", "5")\
    .getOrCreate()
# Define the SQLite JDBC connection details
driver = "org.sqlite.JDBC"
url = "jdbc:sqlite:/Users/khanhnn/Developer/DE/spark/practice_spark/practice/data/flight-data/jdbc/my-sqlite.db"
url = "jdbc:sqlite:/Users/khanhnn/Developer/DE/spark/practice_spark/data/flight-data/jdbc/my-sqlite.db"

tablename = "flight_info"

csvFile = spark.read.format("csv")\
    .option("header", "true")\
    .option("mode", "FAILFAST")\
    .option("inferSchema", "true")\
    .load("data/flight-data/csv/2010-summary.csv")

# Load the data from SQLite database
dbDataFrame = spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", tablename) \
    .option("driver", driver) \
    .load()

# Show the loaded data
# dbDataFrame.show()

# pgDF = spark.read.format("jdbc")\
#     .option("driver", "org.postgresql.Driver")\
#     .option("url", "jdbc:postgresql://database_server")\
#     .option("dbtable", "schema.tablename")\
#     .option("user", "username").option("password", "my-secret-password").load()

# dbDataFrame.select("DEST_COUNTRY_NAME").distinct().show(5)
dbDataFrame = spark.read.format("jdbc")\
    .option("url", url).option("dbtable", tablename).option("driver",  driver)\
    .option("numPartitions", 10).load()


dbDataFrame.select("DEST_COUNTRY_NAME").distinct().show()

props = {"driver":"org.sqlite.JDBC"}
predicates = [
    "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
    "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'"]
spark.read.jdbc(url, tablename, predicates=predicates, properties=props).show()
spark.read.jdbc(url,tablename,predicates=predicates,properties=props)\
    .rdd.getNumPartitions() # 2

props = {"driver":"org.sqlite.JDBC"}
predicates = [
    "DEST_COUNTRY_NAME != 'Sweden' OR ORIGIN_COUNTRY_NAME != 'Sweden'",
    "DEST_COUNTRY_NAME != 'Anguilla' OR ORIGIN_COUNTRY_NAME != 'Anguilla'"]
spark.read.jdbc(url, tablename, predicates=predicates, properties=props).count()

# Partitioning based on a sliding window
colName = "count"
lowerBound = 0
upperBound = 348113 # this is the max count in our database
numPartitions = 10

result = spark.read.jdbc(url, tablename, column=colName, properties=props,
                  lowerBound=lowerBound, upperBound=upperBound,
numPartitions=numPartitions).count() # 255
print(result)

# Writing to SQL Databases
newPath = "jdbc:sqlite://tmp/my-sqlite.db"
csvFile.write.jdbc(newPath, tablename, mode="overwrite", properties=props)

spark.read.jdbc(newPath, tablename, properties=props).count()
csvFile.write.jdbc(newPath, tablename, mode="append", properties=props)
result = spark.read.jdbc(newPath, tablename, properties=props).count()
print(result)

spark.read.text("../data/flight-data/csv/2010-summary.csv")\
    .selectExpr("split(value, ',') as rows").show()