from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import TimestampType

# Initializing Spark session
spark = SparkSession.builder \
    .appName("IPL Cricket") \
    .getOrCreate()

# Defining schema for the streaming data
schema = "season INT, match_id INT, match STRING, home_team STRING, away_team STRING, venue STRING, city STRING, country STRING, current_innings STRING, name STRING, runs INT, balls_faced INT, fours INT, sixes INT, strike_rate DOUBLE, timestamp TIMESTAMP"

bootstrap_servers = "ip-172-31-13-101.eu-west-2.compute.internal:9092,ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-9-237.eu-west-2.compute.internal:9092"

# Set Hive metastore URI
spark.conf.set("hive.metastore.uris", "thrift://ec2-18-132-63-52.eu-west-2.compute.amazonaws.com:9083")

# Reading streaming data from the kafka
streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", "cricket") \
    .option("startingOffsets", "latest") \
    .load()

# Splitting the received data into columns
split_cols = F.split(streaming_df.value, ',')

# Creating DataFrame with schema
streaming_df = streaming_df \
    .select(
        split_cols[0].cast("int").alias("season"),
        split_cols[1].cast("int").alias("match_id"),
        split_cols[2].alias("match"),
        split_cols[3].alias("home_team"),
        split_cols[4].alias("away_team"),
        split_cols[5].alias("venue"),
        split_cols[6].alias("city"),
        split_cols[7].alias("country"),
        split_cols[8].alias("current_innings"),
        split_cols[9].alias("name"),
        split_cols[10].cast("int").alias("runs"),
        split_cols[11].cast("int").alias("balls_faced"),
        split_cols[12].cast("int").alias("fours"),
        split_cols[13].cast("int").alias("sixes"),
        split_cols[14].cast("double").alias("strike_rate"),
        split_cols[15].cast("timestamp").alias("timestamp")
    )

streaming_df = streaming_df.withColumnRenamed("name", "batsman")

# Filter the streaming DataFrame to include only "failed" transactions and add watermarking
batter_df = streaming_df \
    .filter(F.col("batsman") == "Virat") \
    .withWatermark("timestamp", "10 minutes")  # Add watermarking

# Storing the input into the
streaming_query = batter_df \
    .writeStream \
    .format("parquet") \
    .option("path", "/user/ec2-user/UKUSMarHDFS/naga/kafka") \
    .option("checkpointLocation", "/user/ec2-user/UKUSMarHDFS/naga/offset") \
    .outputMode("append") \
    .start()

streaming_query.awaitTermination()











