import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, split
from pyspark.sql import functions as F

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

def getScramAuthString(username, password):
  return f"""org.apache.kafka.common.security.scram.ScramLoginModule required
   username="{username}"
   password="{password}";
  """

# Define the Kafka broker and topic to read from
kafka_bootstrap_servers = os.environ.get("HWE_BOOTSTRAP")
username = os.environ.get("HWE_USERNAME")
password = os.environ.get("HWE_PASSWORD")
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
kafka_topic = "reviews"

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week4Lab") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

# Read data from Kafka using the DataFrame API
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "10") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", getScramAuthString(username, password)) \
    .load().selectExpr("CAST(value AS STRING) as message")

df_with_aliases = df.selectExpr(
    "split(message, '\t')[0] AS marketplace",
    "split(message, '\t')[1] AS customer_id",
    "split(message, '\t')[2] AS review_id",
    "split(message, '\t')[3] AS product_id",
    "split(message, '\t')[4] AS product_parent",
    "split(message, '\t')[5] AS product_title",
    "split(message, '\t')[6] AS product_category",
    "split(message, '\t')[7] AS star_rating",
    "split(message, '\t')[8] AS helpful_votes",
    "split(message, '\t')[9] AS total_votes",
    "split(message, '\t')[10] AS vine",
    "split(message, '\t')[11] AS verified_purchase",
    "split(message, '\t')[12] AS review_headline",
    "split(message, '\t')[13] AS review_body",
    "split(message, '\t')[14] AS purchase_date",
)
review_timestamp = df_with_aliases.withColumn('current_time', F.current_timestamp())

review_timestamp.printSchema()

# Process the received data
query = review_timestamp \
  .writeStream \
  .outputMode("append") \
  .format("parquet") \
  .option("path", "s3a://hwe-fall-2024/eschneider/bronze/reviews") \
  .option("checkpointLocation", "C:/tmp/kafka-checkpoint") \
  .start()

# Wait for the streaming query to finish
query.awaitTermination()

# Stop the SparkSession
spark.stop()