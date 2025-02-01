from pyspark.sql import SparkSession

query = """

WITH change_indicator AS (
	SELECT 
		actorid,
		actor,
		quality_class,
		is_active,
		current_year,
		LAG(quality_class, 1) OVER(PARTITION BY actorid ORDER BY current_year) <> quality_class
		OR
		LAG(is_active, 1) OVER(PARTITION BY actorid ORDER BY current_year) <> is_active
		AS changed
	FROM actors
), streak_indicator AS (
	SELECT
		actorid,
		actor,
		quality_class,
		is_active,
		current_year,
		SUM(
			CASE 
				WHEN changed THEN 1 ELSE 0
			END
		) OVER(PARTITION BY actorid ORDER BY current_year) AS streak
	FROM change_indicator
), compressed AS (
	SELECT
		actorid,
		actor,
		quality_class,
		is_active,
		streak,
		MIN(current_year) AS start_date,
		MAX(current_year) AS end_date,
		MAX(current_year) AS current_year
	FROM streak_indicator
	GROUP BY 1, 2, 3, 4, 5
)
SELECT actorid, actor, quality_class, is_active, start_date, end_date, current_year
FROM compressed

"""


def do_actors_scd_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("actors")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("actors_scd") \
      .getOrCreate()
    output_df = do_actors_scd_transformation(spark, spark.table("actors"))
    output_df.write.mode("overwrite").insertInto("actors_scd")

