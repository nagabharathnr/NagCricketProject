import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType, DateType, DecimalType, \
    DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Ipl Data").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext

#batting data

    batting_schema = StructType([
        StructField("season", IntegerType(),True),
        StructField("match_id", IntegerType(), True),
        StructField("match", StringType(), True),
        StructField("home_team", StringType(), True),
        StructField("away_team", StringType(), True),
        StructField("venue", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("current_innings", StringType(), True),
        StructField("name", StringType(), True),
        StructField("runs", IntegerType(), True),
        StructField("balls_faced", IntegerType(), True),
        StructField("fours", IntegerType(), True),
        StructField("sixes", IntegerType(), True),
        StructField("strike_rate", DoubleType(), True)
    ])

    #match_df = spark.read.option("header", True).schema(match_schema).csv("A:\\nag_python\\NagCricketProject\\input\\match.csv")
    batting_df = spark.read.option("header", True).schema(batting_schema).csv(sys.argv[1])
    col_to_drop = ['country']
    batting_df = batting_df.drop(*col_to_drop)
    batting_df.show()

    #replace the column name of "name" with "batsman"
    batting_df = batting_df.withColumnRenamed("name", "batsman")



#summary data

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

    summary_df = spark.read.option("header", True).schema(summary_schema).csv(sys.argv[3])
    col_to_drop = ['date']
    summary_df = summary_df.drop(*col_to_drop)
    summary_df.show()

    #change the column name
    summary_df = summary_df\
        .withColumnRenamed("id", "match_id")\
        .withColumnRenamed("name", "teams")\
        .withColumnRenamed("pom", "mom")

    #
    # Complex Use Case 1: Home Advantage Analysis
    home_away_avg_df = batting_df.join(summary_df, ["match_id"], "inner") \
        .filter(batting_df["runs"].cast("int").isNotNull() & batting_df["balls_faced"].cast("int").isNotNull()) \
        .groupBy(batting_df["home_team"]) \
        .agg(
        (F.sum(batting_df["runs"].cast("int")) / F.sum(batting_df["balls_faced"].cast("int"))).alias("BattingAverage")) \
        .orderBy("BattingAverage", ascending=False)

    # Complex Use Case 2: Toss Impact Analysis
    toss_impact_df = summary_df.groupBy("toss_won", "winner").agg(F.count("*").alias("MatchCount"))

    # Complex Use Case 3: Player Performance in Pressure Situations
    pressure_situation_df = batting_df.join(summary_df, ["match_id"], "inner") \
        .filter((summary_df["winner"] != "") & (summary_df["winner"] == summary_df["home_team"])) \
        .filter(batting_df["runs"].cast("int").isNotNull() & batting_df["balls_faced"].cast("int").isNotNull()) \
        .groupBy(batting_df["batsman"]) \
        .agg(((F.sum(batting_df["runs"].cast("int")) / F.sum(batting_df["balls_faced"].cast("int"))) * 100).alias(
        "StrikeRateUnderPressure")) \
        .orderBy("StrikeRateUnderPressure", ascending=False)

    # Complex Use Case 4: Venue Analysis
    venue_avg_runs_df = summary_df.groupBy("venue").agg(
        ((F.sum(summary_df["home_runs"].cast("int")) + F.sum(summary_df["away_runs"].cast("int"))) / (
                    F.count("*") * 2)).alias("AverageRuns")
    )

    batting_df.coalesce(1).write.mode("overwrite").option("header", True).csv(sys.argv[2])
    summary_df.coalesce(1).write.mode("overwrite").option("header", True).csv(sys.argv[4])


    # Write DataFrame to PostgreSQL
    batting_df.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb") \
        .option("dbtable", "nag_batting_fullLoad") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "consultants") \
        .option("password", "WelcomeItc@2022") \
        .save()


    # Write DataFrame to PostgreSQL
    summary_df.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb") \
        .option("dbtable", "nag_summary_fullLoad") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "consultants") \
        .option("password", "WelcomeItc@2022") \
        .save()

    # writing it to hive
    batting_df.write.mode("overwrite").option("header", True).saveAsTable("ukusmar.nag_batting_fullLoad_table")
    summary_df.write.mode("overwrite").option("header", True).saveAsTable("ukusmar.nag_summary_fullLoad_table")

    # Final actions
    home_away_avg_df.show()
    toss_impact_df.show()
    pressure_situation_df.show()
    venue_avg_runs_df.show()
    print("jenkins, changed paths2324")

    # Stop Spark session
    spark.stop()

