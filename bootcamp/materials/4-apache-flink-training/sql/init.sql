-- Create processed_events table
CREATE TABLE IF NOT EXISTS processed_events (
    ip VARCHAR,
    event_timestamp TIMESTAMP(3),
    referrer VARCHAR,
    host VARCHAR,
    url VARCHAR,
    geodata VARCHAR
);

SELECT * FROM processed_events;


CREATE TABLE IF NOT EXISTS processed_events_aggregated (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    num_hits BIGINT
);

SELECT * FROM processed_events_aggregated;


CREATE TABLE IF NOT EXISTS processed_events_aggregated_source (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    referrer VARCHAR,
    num_hits BIGINT
);

SELECT * FROM processed_events_aggregated_source;


CREATE TABLE processed_events_aggregated_hw (
    ip VARCHAR,
    host VARCHAR,
    num_events BIGINT
);


SELECT * FROM processed_events_aggregated_hw;


SELECT 
	ip,
	host,
	avg(num_events) AS avg_num_events 
FROM processed_events_aggregated_hw
GROUP BY ip, host
ORDER BY avg_num_events DESC;


SELECT 
	ip,
	host,
	avg(num_events) AS avg_num_events 
FROM processed_events_aggregated_hw
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY ip, host
ORDER BY avg_num_events DESC;





