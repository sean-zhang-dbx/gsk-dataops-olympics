-- Databricks notebook source
-- SDP Pipeline: team_01 Heart Disease Bronze/Silver/Gold (with dedup)

-- Bronze: Ingest all NDJSON from volume
CREATE OR REFRESH STREAMING TABLE heart_bronze
AS SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/dataops_olympics/default/raw_data/heart_events/',
  format => 'json'
);

-- COMMAND ----------

-- Silver: Clean with DQ expectations, then deduplicate
CREATE OR REFRESH STREAMING TABLE heart_silver_raw
(
  CONSTRAINT valid_age EXPECT (age IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_bp EXPECT (trestbps > 0 AND trestbps < 300) ON VIOLATION DROP ROW
)
AS SELECT * FROM STREAM(heart_bronze);

-- COMMAND ----------

-- Deduplicated silver using AUTO CDC (SCD Type 1 - keep latest per event_id)
CREATE OR REFRESH STREAMING TABLE heart_silver;
APPLY CHANGES INTO heart_silver
FROM STREAM(heart_silver_raw)
KEYS (event_id)
SEQUENCE BY event_timestamp
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- Gold: Aggregate by age group and diagnosis
CREATE OR REFRESH MATERIALIZED VIEW heart_gold
COMMENT 'Aggregated heart disease metrics by age group and diagnosis'
AS SELECT
  CASE WHEN age < 40 THEN 'Under 40'
       WHEN age BETWEEN 40 AND 49 THEN '40-49'
       WHEN age BETWEEN 50 AND 59 THEN '50-59'
       ELSE '60+' END AS age_group,
  CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
  COUNT(*) AS patient_count,
  ROUND(AVG(chol), 1) AS avg_cholesterol,
  ROUND(AVG(trestbps), 1) AS avg_blood_pressure,
  ROUND(AVG(thalach), 1) AS avg_max_heart_rate
FROM heart_silver
GROUP BY 1, 2;
