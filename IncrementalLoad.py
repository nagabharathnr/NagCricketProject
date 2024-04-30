import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

if __name__ == '__main__':
    # Initialize SparkSession
    spark = SparkSession.builder.master("local[*]").appName("Incremental Load").enableHiveSupport().getOrCreate()

    # Define the schema for batting data
    batting_schema = StructType([
        StructField("season", IntegerType(), True),
        StructField("match_id", IntegerType(), True),
        StructField("match", StringType(), True),
        StructField("home_team", StringType(), True),
        StructField("away_team", StringType(), True),
        StructField("venue", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),  # Include the "country" column
        StructField("current_innings", StringType(), True),
        StructField("name", StringType(), True),
        StructField("runs", IntegerType(), True),
        StructField("balls_faced", IntegerType(), True),
        StructField("fours", IntegerType(), True),
        StructField("sixes", IntegerType(), True),
        StructField("strike_rate", DoubleType(), True)
    ])

    # Read new data from CSV file
    new_batting_df = spark.read.option("header", True).schema(batting_schema).csv(sys.argv[1])
    col_to_drop = ['country']
    new_batting_df = new_batting_df.drop(*col_to_drop)
    new_batting_df.show()
    # replace the column name of "name" with "batsman"
    new_batting_df = new_batting_df.withColumnRenamed("name", "batsman")

    # Drop the header row if it exists in the new data
    new_batting_df = new_batting_df.filter(F.col("season").isNotNull())  # Assuming "season" is a required field

    # Identify new rows using left anti-join
    #new_rows_df = new_batting_df.join(batting_df, ["season", "match_id", "name"], "left_anti")

    #incremental insertion of records into summary_details table:
    summary_schema = StructType([
        StructField("season", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("home_team", StringType(), True),
        StructField("away_team", StringType(), True),
        StructField("toss_won", StringType(), True),
        StructField("toss_decision", StringType(), True),
        StructField("winner", StringType(), True),
        StructField("date", DateType(), True),
        StructField("venue", StringType(), True),
        StructField("city", StringType(), True),
        StructField("pom", StringType(), True),
        StructField("home_overs", DoubleType(), True),
        StructField("home_runs", IntegerType(), True),
        StructField("home_wickets", IntegerType(), True),
        StructField("home_boundaries", IntegerType(), True),
        StructField("away_overs", DoubleType(), True),
        StructField("away_runs", IntegerType(), True),
        StructField("away_wickets", IntegerType(), True),
        StructField("away_boundaries", IntegerType(), True)
    ])

    new_summary_df = spark.read.option("header", True).schema(summary_schema).csv(sys.argv[3])
    col_to_drop = ['date']
    new_summary_df = new_summary_df.drop(*col_to_drop)
    new_summary_df.show()

    # change the column name
    new_summary_df = new_summary_df \
        .withColumnRenamed("id", "match_id") \
        .withColumnRenamed("name", "teams") \
        .withColumnRenamed("pom", "mom")

    new_batting_df.coalesce(1).write.mode("append").option("header", True).csv(sys.argv[2])
    new_summary_df.coalesce(1).write.mode("append").option("header", True).csv(sys.argv[4])

    # If there are new rows, append them to the PostgreSQL table
    new_batting_df.write.format("jdbc") \
        .mode("append") \
        .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb") \
        .option("dbtable", "nag_batting_fullLoad") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "consultants") \
        .option("password", "WelcomeItc@2022") \
        .save()

    # Write DataFrame to PostgreSQL
    new_summary_df.write.format("jdbc") \
        .mode("append") \
        .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb") \
        .option("dbtable", "nag_summary_fullLoad") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "consultants") \
        .option("password", "WelcomeItc@2022") \
        .save()


    # Also update the Hive table with new incremental records
    new_batting_df.write.mode("append").option("header", True).saveAsTable("ukusmar.nag_batting_fullLoad_table")
    new_summary_df.write.mode("append").option("header", True).saveAsTable("ukusmar.nag_summary_fullLoad_table")

    new_batting_df.show()
    new_summary_df.show()



