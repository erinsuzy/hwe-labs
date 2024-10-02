from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Demo") \
    .master("local") \
    .getOrCreate()

version = spark.version
print(f"I've started a Spark cluster running Spark {version}")


scores_data = spark.read.csv("resources/video_game_scores.tsv", sep=",", header=True)
scores_data.printSchema()
scores_data.show(n=4, truncate=False)

scores_data.write.json("resources/video_game_scores.json")
spark.stop()
