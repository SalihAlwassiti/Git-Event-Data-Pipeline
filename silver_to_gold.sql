CREATE TABLE IF NOT EXISTS github_gold_watermark (
  table_name STRING,
  last_processed_timestamp TIMESTAMP,
  last_update_time TIMESTAMP
);

INSERT INTO github_gold_watermark
SELECT 'github_gold_event_distribution', TIMESTAMP('1970-01-01'), CURRENT_TIMESTAMP()
WHERE NOT EXISTS (SELECT 1 FROM github_gold_watermark WHERE table_name = 'github_gold_event_distribution');

INSERT INTO github_gold_watermark
SELECT 'github_gold_top_actors', TIMESTAMP('1970-01-01'), CURRENT_TIMESTAMP()
WHERE NOT EXISTS (SELECT 1 FROM github_gold_watermark WHERE table_name = 'github_gold_top_actors');

INSERT INTO github_gold_watermark
SELECT 'github_gold_top_repos', TIMESTAMP('1970-01-01'), CURRENT_TIMESTAMP()
WHERE NOT EXISTS (SELECT 1 FROM github_gold_watermark WHERE table_name = 'github_gold_top_repos');

INSERT INTO github_gold_watermark
SELECT 'github_gold_temporal_patterns', TIMESTAMP('1970-01-01'), CURRENT_TIMESTAMP()
WHERE NOT EXISTS (SELECT 1 FROM github_gold_watermark WHERE table_name = 'github_gold_temporal_patterns');

CREATE OR REPLACE TABLE github_gold_summary_metrics AS
WITH base_counts AS (
  SELECT 
    COUNT(event_id) as total_events,
    COUNT(DISTINCT actor_id) as distinct_actors,
    COUNT(DISTINCT repo_id) as distinct_repos,
    COUNT(DISTINCT CASE WHEN actor_login LIKE '%bot%' THEN actor_id END) as distinct_bot_actors,
    COUNT(CASE WHEN actor_login LIKE '%bot%' THEN actor_id END) as bot_events,
    COUNT(DISTINCT CASE WHEN org_id IS NOT NULL THEN actor_id END) as distinct_org_actors,
    COUNT(CASE WHEN org_id IS NOT NULL THEN actor_id END) as org_events,
    COUNT(CASE WHEN event_type = 'CreateEvent' THEN repo_id END) as create_events
  FROM github_silver
)
SELECT 
  'Total Events' as metric_name,
  total_events as value,
  NULL as percentage,
  CURRENT_TIMESTAMP() as last_updated
FROM base_counts
UNION ALL
SELECT 'Distinct Actors', distinct_actors, NULL, CURRENT_TIMESTAMP() FROM base_counts
UNION ALL
SELECT 'Distinct Repositories', distinct_repos, NULL, CURRENT_TIMESTAMP() FROM base_counts
UNION ALL
SELECT 'Create Events', create_events, NULL, CURRENT_TIMESTAMP() FROM base_counts
UNION ALL
SELECT 'Bot Actors (Distinct)', distinct_bot_actors, 
  ROUND(distinct_bot_actors * 100.0 / distinct_actors, 2), CURRENT_TIMESTAMP() FROM base_counts
UNION ALL
SELECT 'Bot Events (Total)', bot_events, 
  ROUND(bot_events * 100.0 / total_events, 2), CURRENT_TIMESTAMP() FROM base_counts
UNION ALL
SELECT 'Org Actors (Distinct)', distinct_org_actors, 
  ROUND(distinct_org_actors * 100.0 / distinct_actors, 2), CURRENT_TIMESTAMP() FROM base_counts
UNION ALL
SELECT 'Org Events (Total)', org_events, 
  ROUND(org_events * 100.0 / total_events, 2), CURRENT_TIMESTAMP() FROM base_counts;

CREATE TABLE IF NOT EXISTS github_gold_event_distribution (
  event_type STRING,
  event_count BIGINT,
  percentage DOUBLE,
  last_updated TIMESTAMP
);

MERGE INTO github_gold_event_distribution AS target
USING (
  SELECT 
    event_type,
    COUNT(*) as new_event_count
  FROM github_silver
  WHERE created_at > (SELECT last_processed_timestamp FROM github_gold_watermark WHERE table_name = 'github_gold_event_distribution')
  GROUP BY event_type
) AS source
ON target.event_type = source.event_type
WHEN MATCHED THEN UPDATE SET
  target.event_count = target.event_count + source.new_event_count,
  target.last_updated = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (event_type, event_count, percentage, last_updated)
  VALUES (source.event_type, source.new_event_count, NULL, CURRENT_TIMESTAMP());

MERGE INTO github_gold_event_distribution AS target
USING (
  SELECT 
    event_type,
    ROUND(event_count * 100.0 / (SELECT SUM(event_count) FROM github_gold_event_distribution), 2) as pct
  FROM github_gold_event_distribution
) AS source
ON target.event_type = source.event_type
WHEN MATCHED THEN UPDATE SET target.percentage = source.pct;

CREATE TABLE IF NOT EXISTS github_gold_top_actors (
  rank BIGINT,
  actor_login STRING,
  event_count BIGINT,
  percentage_of_total DOUBLE,
  last_updated TIMESTAMP
);

MERGE INTO github_gold_top_actors AS target
USING (
  SELECT 
    actor_login,
    COUNT(*) as new_event_count
  FROM github_silver
  WHERE created_at > (SELECT last_processed_timestamp FROM github_gold_watermark WHERE table_name = 'github_gold_top_actors')
  GROUP BY actor_login
) AS source
ON target.actor_login = source.actor_login
WHEN MATCHED THEN UPDATE SET
  target.event_count = target.event_count + source.new_event_count,
  target.last_updated = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (rank, actor_login, event_count, percentage_of_total, last_updated)
  VALUES (NULL, source.actor_login, source.new_event_count, NULL, CURRENT_TIMESTAMP());

CREATE OR REPLACE TEMPORARY VIEW top_actors_ranked AS
SELECT 
  ROW_NUMBER() OVER (ORDER BY event_count DESC) as rank,
  actor_login,
  event_count,
  ROUND(event_count * 100.0 / (SELECT SUM(event_count) FROM github_gold_top_actors), 2) as percentage_of_total,
  CURRENT_TIMESTAMP() as last_updated
FROM github_gold_top_actors
ORDER BY event_count DESC
LIMIT 10;

CREATE OR REPLACE TABLE github_gold_top_actors AS
SELECT * FROM top_actors_ranked;

CREATE TABLE IF NOT EXISTS github_gold_top_repos (
  rank BIGINT,
  repo_name STRING,
  event_count BIGINT,
  percentage_of_total DOUBLE,
  last_updated TIMESTAMP
);

MERGE INTO github_gold_top_repos AS target
USING (
  SELECT 
    repo_name,
    COUNT(*) as new_event_count
  FROM github_silver
  WHERE created_at > (SELECT last_processed_timestamp FROM github_gold_watermark WHERE table_name = 'github_gold_top_repos')
  GROUP BY repo_name
) AS source
ON target.repo_name = source.repo_name
WHEN MATCHED THEN UPDATE SET
  target.event_count = target.event_count + source.new_event_count,
  target.last_updated = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (rank, repo_name, event_count, percentage_of_total, last_updated)
  VALUES (NULL, source.repo_name, source.new_event_count, NULL, CURRENT_TIMESTAMP());

CREATE OR REPLACE TEMPORARY VIEW top_repos_ranked AS
SELECT 
  ROW_NUMBER() OVER (ORDER BY event_count DESC) as rank,
  repo_name,
  event_count,
  ROUND(event_count * 100.0 / (SELECT SUM(event_count) FROM github_gold_top_repos), 2) as percentage_of_total,
  CURRENT_TIMESTAMP() as last_updated
FROM github_gold_top_repos
ORDER BY event_count DESC
LIMIT 10;

CREATE OR REPLACE TABLE github_gold_top_repos AS
SELECT * FROM top_repos_ranked;

CREATE TABLE IF NOT EXISTS github_gold_temporal_patterns (
  hour_of_day INT,
  day_of_week_index INT,
  day_of_week_name STRING,
  event_count BIGINT,
  last_updated TIMESTAMP
);

MERGE INTO github_gold_temporal_patterns AS target
USING (
  SELECT 
    EXTRACT(HOUR FROM created_at) AS hour_of_day,
    EXTRACT(DOW FROM created_at) AS day_of_week_index,
    DAYNAME(created_at) AS day_of_week_name,
    COUNT(*) AS new_event_count
  FROM github_silver
  WHERE created_at > (SELECT last_processed_timestamp FROM github_gold_watermark WHERE table_name = 'github_gold_temporal_patterns')
  GROUP BY 1, 2, 3
) AS source
ON target.hour_of_day = source.hour_of_day 
   AND target.day_of_week_index = source.day_of_week_index
WHEN MATCHED THEN UPDATE SET
  target.event_count = target.event_count + source.new_event_count,
  target.last_updated = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  hour_of_day, day_of_week_index, day_of_week_name, 
  event_count, last_updated
) VALUES (
  source.hour_of_day, source.day_of_week_index, source.day_of_week_name,
  source.new_event_count, CURRENT_TIMESTAMP()
);

MERGE INTO github_gold_watermark AS target
USING (
  SELECT 
    'github_gold_event_distribution' as table_name,
    MAX(created_at) as max_timestamp
  FROM github_silver
) AS source
ON target.table_name = source.table_name
WHEN MATCHED THEN UPDATE SET
  target.last_processed_timestamp = source.max_timestamp,
  target.last_update_time = CURRENT_TIMESTAMP();

MERGE INTO github_gold_watermark AS target
USING (
  SELECT 
    'github_gold_top_actors' as table_name,
    MAX(created_at) as max_timestamp
  FROM github_silver
) AS source
ON target.table_name = source.table_name
WHEN MATCHED THEN UPDATE SET
  target.last_processed_timestamp = source.max_timestamp,
  target.last_update_time = CURRENT_TIMESTAMP();

MERGE INTO github_gold_watermark AS target
USING (
  SELECT 
    'github_gold_top_repos' as table_name,
    MAX(created_at) as max_timestamp
  FROM github_silver
) AS source
ON target.table_name = source.table_name
WHEN MATCHED THEN UPDATE SET
  target.last_processed_timestamp = source.max_timestamp,
  target.last_update_time = CURRENT_TIMESTAMP();

MERGE INTO github_gold_watermark AS target
USING (
  SELECT 
    'github_gold_temporal_patterns' as table_name,
    MAX(created_at) as max_timestamp
  FROM github_silver
) AS source
ON target.table_name = source.table_name
WHEN MATCHED THEN UPDATE SET
  target.last_processed_timestamp = source.max_timestamp,
  target.last_update_time = CURRENT_TIMESTAMP();
