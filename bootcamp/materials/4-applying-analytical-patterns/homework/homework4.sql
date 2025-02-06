CREATE TYPE league_state_type AS 
	ENUM ('New', 'Retired', 'Continued Playing', 'Returned from Retirement', 'Stayed Retired');


CREATE TABLE IF NOT EXISTS players_league_state (
	player_name TEXT,
	league_state league_state_type,
	current_season INTEGER,
	PRIMARY KEY (player_name, current_season)
);

-- Query that does state change tracking for players without window functions (incremental self join)
WITH last_season AS (
	SELECT *
	FROM players  
	WHERE current_season = 2021
), this_season AS (
	SELECT *
	FROM players
	WHERE current_season = 2022
)
SELECT 
	COALESCE(t.player_name, l.player_name) AS player_name,
	(CASE
		WHEN l.player_name IS NULL AND t.player_name IS NOT NULL
			THEN 'New'
		WHEN l.years_since_last_active = 0 AND t.years_since_last_active = 1 
			THEN 'Retired'
		WHEN l.years_since_last_active = 0 AND t.years_since_last_active = 0 
			THEN 'Continued Playing'
		WHEN l.years_since_last_active > 0 AND t.years_since_last_active = 0 
			THEN 'Returned from Retirement'
		ELSE 'Stayed Retired'
	END)::league_state_type AS league_state,
	t.current_season  AS current_season 
FROM this_season t FULL OUTER JOIN last_season l
	ON t.player_name = l.player_name;


-- Query using window functions that does state change tracking for players
INSERT INTO players_league_state 
SELECT 
	player_name,
	(CASE 
		WHEN LAG(years_since_last_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) IS NULL 
			THEN 'New'
		WHEN LAG(years_since_last_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) = years_since_last_active
			THEN 'Continued Playing'
		ELSE CASE 
				WHEN years_since_last_active = 1 THEN 'Retired'
				WHEN years_since_last_active = 0 THEN 'Returned from Retirement'
				ELSE 'Stayed Retired'
			 END	
	END)::league_state_type AS league_state,
	current_season
FROM players
ORDER BY player_name, current_season;


--  query that uses GROUPING SETS to do efficient aggregations of game_details data
CREATE TABLE aggregated_game_details AS
WITH combined AS (
	SELECT 
		gd.*,
		g.season,
		CASE 
			WHEN g.home_team_wins = 1
				THEN CASE 
						WHEN g.team_id_home = gd.team_id THEN 1 
						ELSE 0 
					 END
			ELSE CASE 
					WHEN g.team_id_away = gd.team_id THEN 1 
					ELSE 0 
				 END
		END AS on_winning_team
	FROM game_details gd JOIN games g
		ON gd.game_id = g.game_id
)
SELECT 
	CASE 
		WHEN GROUPING(player_name) = 0 AND GROUPING(team_abbreviation) = 0 AND GROUPING(season) = 1
			THEN 'player__team'
		WHEN GROUPING(player_name) = 0 AND GROUPING(team_abbreviation) = 1 AND GROUPING(season) = 0
			THEN 'player__season'
		WHEN GROUPING(player_name) = 1 AND GROUPING(team_abbreviation) = 0 AND GROUPING(season) = 1
			THEN 'team'
		ELSE 'player__team__season'
	END AS agg_level,
	player_name,
	team_abbreviation,
	season,
	SUM(pts) AS pts_scored,
	SUM(on_winning_team) AS num_wins
FROM combined
GROUP BY GROUPING SETS (
	(player_name, team_abbreviation, season),
	(player_name, team_abbreviation),
	(player_name, season),
	(team_abbreviation)
);

-- who scored the most points playing for one team?
SELECT * 
FROM aggregated_game_details
WHERE agg_level = 'player__team' AND pts_scored IS NOT NULL 
ORDER BY pts_scored DESC
LIMIT 1;

-- who scored the most points in one season?
SELECT * 
FROM aggregated_game_details
WHERE agg_level = 'player__season' AND pts_scored IS NOT NULL 
ORDER BY pts_scored DESC
LIMIT 1;

-- which team has won the most games?
SELECT * 
FROM aggregated_game_details
WHERE agg_level = 'team' AND num_wins IS NOT NULL 
ORDER BY num_wins DESC
LIMIT 1;


-- What is the most games a team has won in a 90 game stretch?
SELECT 
    gd.team_abbreviation,
    g.game_id,
    g.game_date_est,
    SUM(CASE WHEN (g.home_team_wins = 1 AND g.team_id_home = gd.team_id) 
                 OR (g.home_team_wins = 0 AND g.team_id_away = gd.team_id) 
             THEN 1 ELSE 0 END) 
        OVER (PARTITION BY g.game_id, gd.team_id ORDER BY g.game_date_est ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) 
    AS win_count_last_90
FROM game_details gd
JOIN games g ON gd.game_id = g.game_id
ORDER BY win_count_last_90 DESC;


-- How many games in a row did LeBron James score over 10 points a game?
WITH identifier AS (
	SELECT 
		gd.*,
		g.game_date_est,
		CASE WHEN gd.pts > 10 THEN 1 ELSE 0 END AS over_10_points,
	    SUM(CASE WHEN gd.pts > 10 THEN 0 ELSE 1 END) 
	        OVER (PARTITION BY gd.player_id ORDER BY g.game_date_est) AS streak_id
	FROM games g JOIN game_details gd
		ON g.game_id = gd.game_id 
	WHERE gd.player_name = 'LeBron James'
)
SELECT 
	player_name,
	count(*) AS longest_streak
FROM identifier
GROUP BY player_name, streak_id
ORDER BY longest_streak DESC
LIMIT 1;


