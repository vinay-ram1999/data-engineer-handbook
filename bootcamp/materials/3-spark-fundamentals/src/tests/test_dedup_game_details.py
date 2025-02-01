from chispa.dataframe_comparer import *
from ..jobs.dedup_game_details_job import dedup_game_details_transformation
from collections import namedtuple
GameDetails = namedtuple("GameDetails", "game_id team_id player_id player_name")
DedupedGameDetails = namedtuple("DedupedGameDetails", "game_id team_id player_id player_name")


def test_scd_generation(spark):
    source_data = [
        GameDetails(11600001, 1610612744, 201939, "Stephen Curry"),
        GameDetails(11600001, 1610612744, 201939, "Stephen Curry"),
        GameDetails(11600001, 1610612744, 201939, "Stephen Curry"),
        GameDetails(11600013, 1610612754, 101139, "CJ Miles"),
        GameDetails(11600013, 1610612754, 101139, "CJ Miles"),
        GameDetails(11600016, 1610612744, 201142, "Kevin Durant")
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = dedup_game_details_transformation(spark, source_df)
    expected_data = [
        DedupedGameDetails(11600001, 1610612744, 201939, "Stephen Curry"),
        DedupedGameDetails(11600013, 1610612754, 101139, "CJ Miles"),
        DedupedGameDetails(11600016, 1610612744, 201142, "Kevin Durant")
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)