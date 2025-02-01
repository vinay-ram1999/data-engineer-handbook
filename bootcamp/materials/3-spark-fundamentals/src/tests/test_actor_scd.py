from chispa.dataframe_comparer import *
from ..jobs.actors_scd_job import do_actors_scd_transformation
from collections import namedtuple
Actors = namedtuple("Actors", "actorid actor films quality_class is_active current_year")
ActorScd = namedtuple("ActorScd", "actorid actor quality_class is_active start_date end_date current_year")


def test_scd_generation(spark):
    source_data = [
        Actors("nm0000003", 
               "Brigitte Bardot", 
               [{"film":"Les novices", "votes":219}],
               "bad",
               True,
               1970),
        Actors("nm0000003", 
               "Brigitte Bardot", 
               [{"film":"Les novices", "votes":219}, {"film":"The Bear and the Doll", "votes":431}],
               "bad",
               True,
               1971),
        Actors("nm0000003", 
               "Brigitte Bardot", 
               [{"film":"Les novices", "votes":219}, {"film":"The Bear and the Doll", "votes":431}],
               "bad",
               False,
               1972),
        Actors("nm0000003", 
               "Brigitte Bardot", 
               [{"film":"Les novices", "votes":219}, {"film":"The Bear and the Doll", "votes":431}, {"film":"Rum Runners", "votes":469}],
               "bad",
               True,
               1973)
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actors_scd_transformation(spark, source_df)
    expected_data = [
        ActorScd("nm0000003", "Brigitte Bardot", "bad", True, 1970, 1971, 1971),
        ActorScd("nm0000003", "Brigitte Bardot", "bad", False, 1972, 1972, 1972),
        ActorScd("nm0000003", "Brigitte Bardot", "bad", True, 1973, 1973, 1973)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)