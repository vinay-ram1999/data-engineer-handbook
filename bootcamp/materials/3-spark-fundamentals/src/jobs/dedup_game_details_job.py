from pyspark.sql import SparkSession

query = """

WITH with_dups AS (
	SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id ORDER BY game_id) AS row_num
	FROM game_details
)
SELECT game_id, team_id, player_id, player_name
FROM with_dups
WHERE row_num = 1

"""


def dedup_game_details_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("deduped_game_details") \
      .getOrCreate()
    output_df = dedup_game_details_transformation(spark, spark.table("game_details"))
    output_df.write.mode("overwrite").insertInto("deduped_game_details")

