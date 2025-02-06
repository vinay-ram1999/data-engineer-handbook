INSERT INTO players_scd 
WITH streak_started AS (
	SELECT player_name,
	       current_season,
	       scoring_class,
	       is_active,
	       LAG(scoring_class, 1) OVER
	           (PARTITION BY player_name ORDER BY current_season) <> scoring_class
	           OR LAG(scoring_class, 1) OVER
	           (PARTITION BY player_name ORDER BY current_season) IS NULL
	           AS did_scoring_class_change,
	       LAG(is_active, 1) OVER
	           (PARTITION BY player_name ORDER BY current_season) <> is_active
	           OR LAG(is_active, 1) OVER
	           (PARTITION BY player_name ORDER BY current_season) IS NULL
	           AS did_is_active_change
	FROM players
), streak_identified AS (
	SELECT
		player_name,
		scoring_class,
		is_active,
		current_season,
		SUM(CASE WHEN did_scoring_class_change THEN 1 ELSE 0 END)
		    OVER (PARTITION BY player_name ORDER BY current_season) as scoring_class_streak_identifier,
		SUM(CASE WHEN did_is_active_change THEN 1 ELSE 0 END)
		    OVER (PARTITION BY player_name ORDER BY current_season) as is_active_streak_identifier
	FROM streak_started
), aggregated AS (
SELECT
	player_name,
	scoring_class,
	is_active,
	scoring_class_streak_identifier,
	is_active_streak_identifier,
	MIN(current_season) AS start_season,
	MAX(current_season) AS end_season
FROM streak_identified
GROUP BY 1,2,3,4,5
)
SELECT player_name, scoring_class, is_active, start_season, end_season, 2022 AS current_season
FROM aggregated
ORDER BY player_name, start_season;

