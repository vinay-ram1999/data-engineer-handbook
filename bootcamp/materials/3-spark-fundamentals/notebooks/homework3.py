from pyspark.sql.functions import broadcast, split, lit
from pyspark.sql.functions import expr, col, avg, count, count_distinct
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("bootcamp").getOrCreate()

matches = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")
match_details = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")
medals_matches_players = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv")
medals = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals.csv")
maps = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/maps.csv")

matches_bucketed_DDL = """
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
     match_id STRING,
     mapid STRING,
     is_team_game BOOLEAN,
     playlist_id STRING,
     completion_date TIMESTAMP
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
"""

match_details_bucketed_DDL = """
CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
     match_id STRING,
     player_gamertag STRING,
     player_total_kills INTEGER,
     player_total_deaths INTEGER
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
"""

medals_matches_players_bucketed_DDL = """
CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players_bucketed (
    match_id STRING,
    player_gamertag STRING,
    medal_id BIGINT,
    count INTEGER
 )
 USING iceberg
 PARtITIONED BY (bucket(16, match_id));
"""

spark.sql("""DROP TABLE IF EXISTS bootcamp.matches_bucketed""")
spark.sql(matches_bucketed_DDL)

spark.sql("""DROP TABLE IF EXISTS bootcamp.match_details_bucketed""")
spark.sql(match_details_bucketed_DDL)

spark.sql("""DROP TABLE IF EXISTS bootcamp.medals_matches_players_bucketed""")
spark.sql(medals_matches_players_bucketed_DDL)

matches.select(col("match_id"), col("mapid"), col("is_team_game"), col("playlist_id"), col("completion_date")) \
    .write.mode("append") \
    .bucketBy(16, "match_id").saveAsTable("bootcamp.matches_bucketed")

match_details.select(col("match_id"), col("player_gamertag"), col("player_total_kills"), col("player_total_deaths")) \
    .write.mode("append") \
    .bucketBy(16, "match_id").saveAsTable("bootcamp.match_details_bucketed")

medals_matches_players.select(col("match_id"), col("player_gamertag"), col("medal_id"), col("count")) \
    .write.mode("append") \
    .bucketBy(16, "match_id").saveAsTable("bootcamp.medals_matches_players_bucketed")

matches_bucketed = spark.read.table("bootcamp.matches_bucketed")
match_details_bucketed = spark.read.table("bootcamp.match_details_bucketed")
medals_matches_players_bucketed = spark.read.table("bootcamp.medals_matches_players_bucketed")

bucketed_join_df = matches_bucketed \
    .join(match_details_bucketed, "match_id", "left") \
    .join(medals_matches_players_bucketed, ["match_id", "player_gamertag"], "left")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

final_df = bucketed_join_df \
    .join(broadcast(medals.select(col("medal_id"), col("sprite_uri"), col("classification"))), "medal_id", "inner") \
    .join(broadcast(maps.select(col("mapid"), col("name"))), "mapid", "inner")

player_avg_kills = final_df.groupBy("player_gamertag") \
    .agg(avg("player_total_kills").alias("avg_kills")) \
    .orderBy(col("avg_kills").desc())

playlist_count = final_df.groupBy("playlist_id") \
    .agg(count_distinct("match_id").alias("count")) \
    .orderBy(col("count").desc())

map_count = final_df.groupBy("mapid") \
    .agg(count_distinct("match_id").alias("count")) \
    .orderBy(col("count").desc())

killing_spree_count = final_df.filter(col("classification") == "KillingSpree") \
    .groupBy("mapid") \
    .agg(count("player_gamertag").alias("count")) \
    .orderBy(col("count").desc())

print(player_avg_kills.show(1))
print(playlist_count.show(1))
print(map_count.show(1))
print(killing_spree_count.show(1))

final_df.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bootcamp.combined_data")

final_df.sortWithinPartitions(col("playlist_id"), col("mapid")) \
    .write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bootcamp.combined_data_sorted")

query = """
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'unsorted' 
FROM demo.bootcamp.combined_data.files

UNION ALL

SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted' 
FROM demo.bootcamp.combined_data_sorted.files
"""

spark.sql(query).show()