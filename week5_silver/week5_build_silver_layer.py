import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week5Lab") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
    .master('local[*]') \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

bronze_schema = StructType([
    StructField("marketplace", StringType, nullable=False)
    , StructField("customer_id", StringType, nullable=False)
    , StructField("review_id", StringType, nullable=False)
    , StructField("product_id", StringType, nullable=False)
    , StructField("product_parent", StringType, nullable=False)
    , StructField("product_title", StringType, nullable=False)
    , StructField("product_category", StringType, nullable=False)
    , StructField("star_rating", IntegerType, nullable=False)
    , StructField("helpful_votes", IntegerType, nullable=False)
    , StructField("total_votes", IntegerType, nullable=False)
    , StructField("vine", StringType, nullable=False)
    , StructField("verified_purchase", StringType, nullable=False)
    , StructField("review_headline", StringType, nullable=False)
    , StructField("review_body", StringType, nullable=False)
    , StructField("purchase_date", StringType, nullable=False)
    , StructField("current_timestamp", TimestampType, nullable=False)

])

bronze_reviews = spark.readStream.format("parquet").load("s3a://hwe-fall-2024/eschneider/bronze/reviews")
bronze_reviews.createOrReplaceTempView("bronze_reviews_view")
bronze_customers = bronze_customers = spark.read.parquet("s3a://hwe-fall-2024/eschneider/bronze/customers")
bronze_customers.createOrReplaceTempView("bronze_customers_view")
joined_data = bronze_reviews.join(bronze_customers, "customer_id")
silver_data = silver_data = joined_data.filter(joined_data.verified == True)


streaming_query = silver_data \
  .writeStream \
  .outputMode("append") \
  .format("parquet") \
  .option("path", "s3a://hwe-fall-2024/eschneider/silver/reviews") \
  .option("checkpointLocation", "C:/tmp/silver-checkpoint") \
  .start()

streaming_query.start().awaitTermination()

## Stop the SparkSession
spark.stop()
