from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Create a SparkSession
spark = SparkSession.builder.appName("RetailDataAnalysis").getOrCreate()
# Set the log level to ERROR to minimize log output
spark.sparkContext.setLogLevel('ERROR')

# Function to check if the type is an order
def is_a_order(type):
    return 1 if type == 'ORDER' else 0

# Function to check if the type is a return
def is_a_return(type):
    return 1 if type == 'RETURN' else 0

# Function to calculate the total item count
def total_item_count(items):
    if items is not None:
        item_count = 0
        for item in items:
            item_count += item['quantity']
        return item_count
    return 0

# Function to calculate the total cost
def total_cost(items, type):
    if items is not None:
        total_cost = 0
        for item in items:
            item_price = item['quantity'] * item['unit_price']
            total_cost += item_price
        if type == 'RETURN':
            return total_cost * -1
        else:
            return total_cost
    return 0

# Register User Defined Functions (UDFs)
is_order = udf(is_a_order, IntegerType())
is_return = udf(is_a_return, IntegerType())
add_total_item_count = udf(total_item_count, IntegerType())
add_total_cost = udf(total_cost, FloatType())

# Kafka Options
kafka_options = {
    "kafka.bootstrap.servers": "18.211.252.152:9092",
    "subscribe": "real-time-project",
    "startingOffsets": "latest"
}

# Read input data from Kafka
raw_stream = spark \
    .readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()

# Define the schema for JSON data
JSON_Schema = StructType() \
    .add("invoice_no", LongType()) \
    .add("country", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("type", StringType()) \
    .add("items", ArrayType(StructType([
        StructField("SKU", StringType()),
        StructField("title", StringType()),
        StructField("unit_price", FloatType()),
        StructField("quantity", IntegerType())
    ])))

# Extract data from JSON format
order_stream_data = raw_stream.select(from_json(col("value").cast("string"), JSON_Schema).alias("data")).select("data.*")

# Calculate additional columns for the stream
enriched_stream_output = order_stream_data \
    .withColumn("total_cost", add_total_cost(order_stream_data.items, order_stream_data.type)) \
    .withColumn("total_items", add_total_item_count(order_stream_data.items)) \
    .withColumn("is_order", is_order(order_stream_data.type)) \
    .withColumn("is_return", is_return(order_stream_data.type))

# Write the summarized input table to the console
order_batch = enriched_stream_output \
    .select("invoice_no", "country", "timestamp", "total_cost", "total_items", "is_order", "is_return") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("path", "/Console_output") \
    .option("checkpointLocation", "/Console_output_checkpoints") \
    .trigger(processingTime="1 minute") \
    .start()

# Calculate Time-based KPIs
agg_time = enriched_stream_output \
    .withWatermark("timestamp", "1 minutes") \
    .groupby(window("timestamp", "1 minute")) \
    .agg(sum("total_cost").alias("total_volume_of_sales"),
         avg("total_cost").alias("average_transaction_size"),
         count("invoice_no").alias("OPM"),
         avg("is_Return").alias("rate_of_return")) \
    .select("window.start", "window.end", "OPM", "total_volume_of_sales", "average_transaction_size", "rate_of_return")

# Calculate Time and country-based KPIs
agg_country_time = enriched_stream_output \
    .withWatermark("timestamp", "1 minutes") \
    .groupBy(window("timestamp", "1 minutes"), "country") \
    .agg(sum("total_cost").alias("total_volume_of_sales"),
         count("invoice_no").alias("OPM"),
         avg("is_Return").alias("rate_of_return")) \
    .select("window.start", "window.end", "country", "OPM", "total_volume_of_sales", "rate_of_return")

# Write to the Console: Time-based KPI values
by_time = agg_time.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "Timebased-KPI") \
    .option("checkpointLocation", "Timebased-KPI-checkpoints") \
    .trigger(processingTime="1 minutes") \
    .start()

# Write to the Console: Time and country-based KPI values
by_time_country = agg_country_time.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "Country-and-timebased-KPI") \
    .option("checkpointLocation", "Country-and-timebased-KPI_checkpoints") \
    .trigger(processingTime="1 minutes") \
    .start()

# Await termination of the streams
order_batch.awaitTermination()
by_time.awaitTermination()
by_time_country.awaitTermination()