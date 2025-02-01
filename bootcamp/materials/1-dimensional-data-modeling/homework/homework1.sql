CREATE TYPE films_struct AS (
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT
);

CREATE TYPE quality_class AS
	ENUM ('bad', 'average', 'good', 'star');

-- DDL for actors table
CREATE TABLE IF NOT EXISTS actors (
	actorid TEXT,
	actor TEXT,
	films films_struct[],
	quality_class quality_class,
	is_active BOOL,
	current_year INTEGER,
	PRIMARY KEY (actorid, current_year)
);

-- DROP TABLE actors;

SELECT MIN(a.year), MAX(a.year) FROM actor_films a;
-- min_year = 1970; max_year = 2021

-- Query to insert cummulative data into actors table 1 year at a time
INSERT INTO actors
WITH last_year AS (
	SELECT * FROM actors
	WHERE current_year = 1972
), current_year AS (
	SELECT * FROM actor_films f
	WHERE f.year = 1973
), combined AS (
	SELECT 
		COALESCE(c.actorid, l.actorid) AS actorid,
		COALESCE(c.actor, l.actor) AS actor,
		CASE 
			WHEN l.films IS NULL
				THEN ARRAY[ROW(c.film, c.votes, c.rating, c.filmid)::films_struct]
			WHEN c.film IS NOT NULL
				THEN ARRAY(SELECT DISTINCT UNNEST(l.films || ARRAY[ROW(c.film, c.votes, c.rating, c.filmid)::films_struct]))
			ELSE l.films
		END AS films,
		COALESCE(c.year IS NOT NULL, FALSE) AS is_active,
		COALESCE(c.year, l.current_year + 1) AS current_year
	FROM current_year c FULL OUTER JOIN last_year l
		ON c.actorid = l.actorid
)
SELECT 
	actorid,
	actor,
	ARRAY(SELECT DISTINCT UNNEST(ARRAY_AGG(film::films_struct))) AS films,
	(CASE
		WHEN AVG((film).rating) > 8 THEN 'star'
		WHEN AVG((film).rating) > 7 THEN 'good'
		WHEN AVG((film).rating) > 6 THEN 'average'
		ELSE 'bad'
	END)::quality_class AS quality_class,
	BOOL_OR(is_active) AS is_active,
	MAX(current_year) AS current_year
FROM (
	SELECT 
		actorid,
		actor,
		UNNEST(films) AS film,
		is_active,
		current_year
	FROM combined
) unnested
GROUP BY actorid, actor;

-- SELECT * FROM actors
-- WHERE actorid = 'nm0000032';

-- DDL for actors_history_scd table
CREATE TABLE IF NOT EXISTS actors_history_scd (
	actorid TEXT,
	actor TEXT,
	quality_class quality_class,
	is_active BOOL,
	start_date INTEGER,
	end_date INTEGER,
	current_year INTEGER,
	PRIMARY KEY (actorid, start_date)
);

-- DROP TABLE actors_history_scd;

-- Query to backfill all years scd data at a time
INSERT INTO actors_history_scd
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
ORDER BY 1, 5;

SELECT * FROM actors
WHERE actorid = 'nm0000003';

SELECT * FROM actors_history_scd
WHERE actorid = 'nm0000003';

CREATE TYPE scd_type AS (
	quality_class quality_class,
	is_active BOOL,
	start_date INTEGER,
	end_date INTEGER
)

-- Query to insert scd data incrementally 1 year at a time
WITH last_year AS (
	SELECT *
	FROM actors_history_scd
	WHERE current_year = 1999
), scd_history AS (
SELECT *
	FROM actors_history_scd
	WHERE current_year <> 1999
	AND end_date < 1999
), this_year AS (
	SELECT *
	FROM actors
	WHERE current_year = 2000
), unchanged AS (
	SELECT
		ty.actorid,
		ty.actor,
		ty.quality_class,
		ty.is_active,
		ly.start_date,
		ty.current_year,
		ty.current_year
	FROM this_year ty JOIN last_year ly
		ON ty.actorid = ly.actorid
	WHERE ty.quality_class = ly.quality_class
		AND ty.is_active = ly.is_active
), changed AS (
	SELECT
		ty.actorid,
		ty.actor,
		UNNEST(ARRAY[
			ROW(ly.quality_class, ly.is_active, ly.start_date, ly.end_date)::scd_type,
			ROW(ty.quality_class, ty.is_active, ty.current_year, ty.current_year)::scd_type
		]) AS records,
		ty.current_year
	FROM this_year ty JOIN last_year ly
		ON ty.actorid = ly.actorid
	WHERE ty.quality_class <> ly.quality_class
		OR ty.is_active <> ly.is_active
), unnested_changed AS (
	SELECT
		actorid,
		actor,
		(records::scd_type).quality_class,
		(records::scd_type).is_active,
		(records::scd_type).start_date,
		(records::scd_type).end_date,
		current_year
	FROM changed
), new_actors AS (
	SELECT
		ty.actorid,
		ty.actor,
		ty.quality_class,
		ty.is_active,
		ty.current_year,
		ty.current_year,
		ty.current_year
	FROM this_year ty LEFT JOIN last_year ly
		ON ty.actorid = ly.actorid
	WHERE ly.actorid IS NULL
)
SELECT *
FROM (
	SELECT * FROM scd_history
	UNION ALL
	SELECT * FROM unchanged
	UNION ALL
	SELECT * FROM unnested_changed
	UNION ALL
	SELECT * FROM new_actors
) new_scd;




