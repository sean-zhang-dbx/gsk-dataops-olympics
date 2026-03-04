# Databricks notebook source
# Event 1 Simulation: team_01 does SQL path + all bonuses

TEAM_NAME = "team_01"
CATALOG = TEAM_NAME
SHARED_CATALOG = "dataops_olympics"
RAW_DATA_PATH = f"/Volumes/{SHARED_CATALOG}/default/raw_data/heart_events/"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA default")

# COMMAND ----------

# === BRONZE ===
df_bronze = spark.read.json(f"{RAW_DATA_PATH}*.json")
df_bronze.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.heart_bronze")
print(f"Bronze: {df_bronze.count()} rows")

# COMMAND ----------

# === SILVER ===
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.heart_silver AS
    SELECT *, current_timestamp() as ingested_at
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp) as _rn
        FROM {CATALOG}.default.heart_bronze
    )
    WHERE _rn = 1
      AND age IS NOT NULL AND age BETWEEN 1 AND 120
      AND trestbps BETWEEN 50 AND 300
      AND chol >= 0
""")
silver_count = spark.table(f"{CATALOG}.default.heart_silver").count()
print(f"Silver: {silver_count} rows")

# COMMAND ----------

# === GOLD ===
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.heart_gold AS
    SELECT
        CASE
            WHEN age < 40 THEN 'Under 40'
            WHEN age < 50 THEN '40-49'
            WHEN age < 60 THEN '50-59'
            ELSE '60+'
        END as age_group,
        CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END as diagnosis,
        COUNT(*) as patient_count,
        ROUND(AVG(chol), 1) as avg_cholesterol,
        ROUND(AVG(trestbps), 1) as avg_blood_pressure,
        ROUND(AVG(thalach), 1) as avg_max_heart_rate
    FROM {CATALOG}.default.heart_silver
    GROUP BY 1, 2
    ORDER BY 1, 2
""")
print(f"Gold: {spark.table(f'{CATALOG}.default.heart_gold').count()} rows")

# COMMAND ----------

# === GOVERNANCE ===
spark.sql(f"ALTER TABLE {CATALOG}.default.heart_bronze SET TBLPROPERTIES ('comment' = 'Raw patient intake events from hospital EHR — 5 NDJSON batches, unmodified')")
spark.sql(f"ALTER TABLE {CATALOG}.default.heart_silver SET TBLPROPERTIES ('comment' = 'Cleaned patient intake data — validated age/BP/cholesterol, deduplicated on event_id')")
spark.sql(f"ALTER TABLE {CATALOG}.default.heart_gold SET TBLPROPERTIES ('comment' = 'Heart disease metrics aggregated by age group for dashboards and Genie')")

for col_name, comment in [
    ("age", "Patient age in years (validated 1-120)"),
    ("trestbps", "Resting blood pressure in mmHg (validated 50-300)"),
    ("chol", "Serum cholesterol in mg/dL (validated >= 0)"),
    ("target", "Diagnosis: 1 = heart disease present, 0 = healthy"),
    ("event_id", "Unique event identifier from source system"),
]:
    spark.sql(f"ALTER TABLE {CATALOG}.default.heart_silver ALTER COLUMN {col_name} COMMENT '{comment}'")

print("Governance comments applied")

# COMMAND ----------

# === BONUS: Liquid Clustering ===
try:
    spark.sql(f"ALTER TABLE {CATALOG}.default.heart_silver CLUSTER BY (age, target)")
    print("Liquid Clustering applied to heart_silver")
except Exception as e:
    print(f"Liquid Clustering: {e}")

# COMMAND ----------

# === BONUS: AI Functions ===
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.default.heart_gold_ai AS
    SELECT *,
        ai_query(
            'databricks-meta-llama-3-3-70b-instruct',
            CONCAT(
                'Classify cardiovascular risk as LOW, MEDIUM, or HIGH for: ',
                age_group, ' patients, ', diagnosis,
                ', avg cholesterol=', avg_cholesterol,
                ', avg BP=', avg_blood_pressure,
                '. Respond with ONLY one word: LOW, MEDIUM, or HIGH'
            )
        ) AS cardiovascular_risk
    FROM {CATALOG}.default.heart_gold
""")
print(f"heart_gold_ai: {spark.table(f'{CATALOG}.default.heart_gold_ai').count()} rows")
display(spark.table(f"{CATALOG}.default.heart_gold_ai"))

# COMMAND ----------

print("Event 1 simulation complete for team_01")
dbutils.notebook.exit("OK")
