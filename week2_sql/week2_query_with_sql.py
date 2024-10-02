from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

### Setup: Create a SparkSession
spark = SparkSession.builder\
        .appName("week2")\
        .master("local")\
        .getOrCreate()

# For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

### Questions

# Question 1: Read the tab separated file named "resources/reviews.tsv.gz" into a dataframe. Call it "reviews".
reviews=spark.read.csv("resources/reviews.tsv.gz", sep='\t', header=True)

# Question 2: Create a virtual view on top of the reviews dataframe, so that we can query it with Spark SQL.
reviews.createOrReplaceTempView("reviews")

# Question 3: Add a column to the dataframe named "review_timestamp", representing the current time on your computer. 
review_timestamp = spark.sql("SELECT reviews.*, current_timestamp() AS review_timestamp from reviews")
review_timestamp.show(1)

# Question 4: How many records are in the reviews dataframe? 
total_reviews = spark.sql("Select count(*) from reviews")
total_reviews.show()

# Question 5: Print the first 5 rows of the dataframe. 
# Some of the columns are long - print the entire record, regardless of length.
five_rows = spark.sql("select * from reviews limit 5")
five_rows.show(truncate=False)



# Question 6: Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.
# Look at the first 50 rows of that dataframe. 
# Which value appears to be the most common?
product_category = spark.sql("SELECT distinct product_category from reviews")
product_category.show()

# Question 7: Find the most helpful review in the dataframe - the one with the highest number of helpful votes.
# What is the product title for that review? How many helpful votes did it have?
most_helpful = spark.sql("Select * from reviews ORDER BY helpful_votes desc")
most_helpful.show(1)
# Question 8: How many reviews exist in the dataframe with a 5 star rating?
five_star = spark.sql("SELECT count(*) from reviews where star_rating=5")
five_star.show()


# Question 9: Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
# Create a new dataframe with just those 3 columns, except cast them as "int"s.
# Look at 10 rows from this dataframe.
ints = spark.sql("Select cast(helpful_votes as INT), cast(star_rating as INT), cast(total_votes as INT) from reviews")
ints.show(10)
# Question 10: Find the date with the most purchases.
# Print the date and total count of the date which had the most purchases.
purchase_date = spark.sql("Select cast(purchase_date as DATE) as purchase_date, count(*) as count from reviews group by purchase_date order by count desc")
purchase_date.show(1)

##Question 11: Write the dataframe from Question 3 to your drive in JSON format.
##Feel free to pick any directory on your computer.
##Use overwrite mode.
review_timestamp.write.json("week2_sql/question11", mode="overwrite")

### Teardown
# Stop the SparkSession
spark.stop()