-- Query to deduplicate game_details table
WITH with_dups AS (
	SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id) AS row_num
	FROM game_details
)
SELECT COUNT(*)
FROM with_dups
WHERE row_num = 1;


-- DDL for user_devices_cumulated table
CREATE TABLE IF NOT EXISTS user_devices_cumulated (
	user_id NUMERIC,
	device_id NUMERIC,
	present_date DATE,
	browser_type TEXT,
	device_activity_datelist DATE[],
	PRIMARY KEY (user_id, device_id, present_date)
);


-- Cumulative query to generate device_activity_datelist from events
INSERT INTO user_devices_cumulated 
WITH yesterday AS (
	SELECT *
	FROM user_devices_cumulated 
	WHERE present_date = '2023-01-30'
), events_today AS (
	SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY device_id, DATE(event_time)) AS row_num
	FROM events
	WHERE DATE(event_time) = '2023-01-31'
		AND device_id IS NOT NULL 
		AND user_id IS NOT NULL
), devices_dedup AS (
	SELECT 
		device_id, 
		browser_type
	FROM devices
	GROUP BY device_id, browser_type
), today AS (
	SELECT
		e.user_id,
		e.device_id,
		DATE(e.event_time) AS present_date,
		d.browser_type
	FROM events_today e JOIN devices_dedup d
		ON e.device_id = d.device_id
	WHERE e.row_num = 1
)
SELECT 
	COALESCE(t.user_id, y.user_id) AS user_id,
	COALESCE(t.device_id, y.device_id) AS device_id,
	COALESCE(t.present_date, y.present_date + INTERVAL '1 day') AS present_date,
	COALESCE(t.browser_type, y.browser_type) AS browser_type,
	CASE
		WHEN y.device_activity_datelist IS NULL
			THEN ARRAY[t.present_date]
		WHEN t.present_date IS NULL
			THEN y.device_activity_datelist
		ELSE ARRAY[t.present_date] || y.device_activity_datelist 
	END AS device_activity_datelist
FROM yesterday y FULL OUTER JOIN today t
	ON y.device_id = t.device_id;


-- Query to convert the device_activity_datelist column into a datelist_int column
WITH user_devices AS (
	SELECT * 
	FROM user_devices_cumulated
	WHERE present_date = '2023-01-31'
), date_series AS (
	SELECT CAST(generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS DATE) AS series_date
), placeholder_int AS (
	SELECT 
		CASE 
			WHEN ud.device_activity_datelist @> ARRAY[ds.series_date]
				THEN POW(2, 30 - (ud.present_date - ds.series_date))
			ELSE 0
		END AS placeholder,
		*
	FROM user_devices ud CROSS JOIN date_series ds
)
SELECT 
	user_id,
	device_id,
	present_date,
	browser_type,
	CAST(CAST(SUM(placeholder) AS BIGINT) AS BIT(31)) AS datelist_int,
	BIT_COUNT(CAST(CAST(SUM(placeholder) AS BIGINT) AS BIT(31))) AS num_days_active
FROM placeholder_int
GROUP BY 1,2,3,4
ORDER BY 6 DESC;


-- DDL for hosts_cumulated table
CREATE TABLE IF NOT EXISTS hosts_cumulated (
	host TEXT,
	present_date DATE,
	host_activity DATE[],
	PRIMARY KEY (host, present_date)
);


-- Incremental query to generate host_activity_datelist 
INSERT INTO hosts_cumulated 
WITH yesterday AS (
	SELECT *
	FROM hosts_cumulated 
	WHERE present_date = '2023-01-30'
), today AS (
	SELECT 
		host,
		DATE(event_time) AS present_date
	FROM events
	GROUP BY host, DATE(event_time)
	HAVING DATE(event_time) = '2023-01-31'
)
SELECT 
	COALESCE(t.host, y.host) AS host,
	COALESCE(t.present_date, y.present_date) AS present_date,
	CASE
		WHEN y.host_activity IS NULL
			THEN ARRAY[t.present_date]
		WHEN t.present_date IS NULL
			THEN y.host_activity
		ELSE ARRAY[t.present_date] || y.host_activity
	END AS host_activity
FROM yesterday y FULL OUTER JOIN today t
	ON y.host = t.host;


-- Monthly reduced fact table DDL host_activity_reduced
CREATE TABLE IF NOT EXISTS host_activity_reduced (
	host TEXT,
	month_start DATE,
	hit_array INTEGER[],
	unique_visitors INTEGER[],
	PRIMARY KEY (host, month_start)
);


-- Incremental query that loads host_activity_reduced
INSERT INTO host_activity_reduced
WITH yesterday AS (
	SELECT *
	FROM host_activity_reduced
	WHERE month_start = '2023-01-01'
), daily_aggregate AS (
	SELECT 
		host,
		DATE(event_time) AS present_date,
		COUNT(*) AS hit_array,
		COUNT(DISTINCT user_id) AS unique_visitors
	FROM events
	WHERE user_id IS NOT NULL
		AND DATE(event_time) = '2023-01-31'
	GROUP BY host, DATE(event_time)
)
SELECT 
	COALESCE(da.host, y.host) AS host,
	COALESCE(y.month_start, DATE_TRUNC('month', da.present_date)) AS month_start,
	CASE 
		WHEN y.month_start IS NULL
			THEN ARRAY[COALESCE(da.hit_array, 0)]
		WHEN y.hit_array IS NULL
			THEN ARRAY_FILL(0, ARRAY[da.present_date - y.month_start]) || ARRAY[COALESCE(da.hit_array, 0)]
		ELSE y.hit_array || ARRAY[COALESCE(da.hit_array, 0)]
	END AS hit_array,
	CASE 
		WHEN y.month_start IS NULL
			THEN ARRAY[COALESCE(da.unique_visitors, 0)]
		WHEN y.unique_visitors IS NULL
			THEN ARRAY_FILL(0, ARRAY[da.present_date - y.month_start]) || ARRAY[COALESCE(da.unique_visitors, 0)]
		ELSE y.unique_visitors || ARRAY[COALESCE(da.unique_visitors, 0)]
	END AS unique_visitors
FROM yesterday y FULL OUTER JOIN daily_aggregate da
	ON y.host = da.host
ON CONFLICT (host, month_start)
DO UPDATE
SET
	hit_array = EXCLUDED.hit_array,
	unique_visitors = EXCLUDED.unique_visitors;


