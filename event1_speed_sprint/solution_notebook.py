# Databricks notebook source
# MAGIC %md
# MAGIC # 🏃 Event 1: Speed Sprint — SOLUTION
# MAGIC
# MAGIC > **⚠️ ORGANIZERS ONLY — Do not distribute to participants!**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Ingest CSV Data

# COMMAND ----------

csv_path = "file:/tmp/dataops_olympics/raw/heart_disease/heart.csv"

df_heart = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(csv_path)
)

display(df_heart)
print(f"Records: {df_heart.count()}, Columns: {len(df_heart.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Ingest JSON Data

# COMMAND ----------

json_path = "file:/tmp/dataops_olympics/raw/life_expectancy/life_expectancy_sample.json"

df_life = (
    spark.read.format("json")
    .option("multiLine", "true")
    .load(json_path)
)

display(df_life)
print(f"Records: {df_life.count()}, Columns: {len(df_life.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Delta Tables with Governance

# COMMAND ----------

TEAM_NAME = "solution"

# Write Delta tables
df_heart.write.format("delta").mode("overwrite").saveAsTable(f"{TEAM_NAME}_heart_disease")
df_life.write.format("delta").mode("overwrite").saveAsTable(f"{TEAM_NAME}_life_expectancy")

print("✅ Delta tables created")

# COMMAND ----------

# Add governance - table comments
spark.sql(f"""
    ALTER TABLE {TEAM_NAME}_heart_disease 
    SET TBLPROPERTIES ('comment' = 'UCI Heart Disease dataset - Cleveland processed. Contains 14 clinical attributes for predicting heart disease presence.')
""")

# Column comments
spark.sql(f"ALTER TABLE {TEAM_NAME}_heart_disease ALTER COLUMN age COMMENT 'Patient age in years'")
spark.sql(f"ALTER TABLE {TEAM_NAME}_heart_disease ALTER COLUMN sex COMMENT 'Patient sex (1=male, 0=female)'")
spark.sql(f"ALTER TABLE {TEAM_NAME}_heart_disease ALTER COLUMN cp COMMENT 'Chest pain type (0=typical angina, 1=atypical, 2=non-anginal, 3=asymptomatic)'")
spark.sql(f"ALTER TABLE {TEAM_NAME}_heart_disease ALTER COLUMN trestbps COMMENT 'Resting blood pressure (mm Hg)'")
spark.sql(f"ALTER TABLE {TEAM_NAME}_heart_disease ALTER COLUMN chol COMMENT 'Serum cholesterol (mg/dl)'")
spark.sql(f"ALTER TABLE {TEAM_NAME}_heart_disease ALTER COLUMN fbs COMMENT 'Fasting blood sugar > 120 mg/dl (1=true, 0=false)'")
spark.sql(f"ALTER TABLE {TEAM_NAME}_heart_disease ALTER COLUMN target COMMENT 'Heart disease diagnosis (1=disease, 0=no disease)'")

print("✅ Governance configured")

# COMMAND ----------

# Add governance for life expectancy table
spark.sql(f"""
    ALTER TABLE {TEAM_NAME}_life_expectancy
    SET TBLPROPERTIES ('comment' = 'WHO Life Expectancy dataset - Health indicators by country and year for predicting life expectancy.')
""")

spark.sql(f"ALTER TABLE {TEAM_NAME}_life_expectancy ALTER COLUMN country COMMENT 'Country name'")
spark.sql(f"ALTER TABLE {TEAM_NAME}_life_expectancy ALTER COLUMN year COMMENT 'Calendar year of observation'")
spark.sql(f"ALTER TABLE {TEAM_NAME}_life_expectancy ALTER COLUMN life_expectancy COMMENT 'Life expectancy in years'")

print("✅ Life expectancy governance configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Analytical Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 1: Average age by heart disease status
# MAGIC SELECT 
# MAGIC     CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'No Heart Disease' END as diagnosis,
# MAGIC     ROUND(AVG(age), 1) as avg_age,
# MAGIC     COUNT(*) as patient_count,
# MAGIC     ROUND(AVG(chol), 1) as avg_cholesterol,
# MAGIC     ROUND(AVG(trestbps), 1) as avg_blood_pressure
# MAGIC FROM solution_heart_disease
# MAGIC GROUP BY target
# MAGIC ORDER BY target

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 2: Distribution of chest pain types
# MAGIC SELECT 
# MAGIC     CASE cp
# MAGIC         WHEN 0 THEN 'Typical Angina'
# MAGIC         WHEN 1 THEN 'Atypical Angina'
# MAGIC         WHEN 2 THEN 'Non-Anginal Pain'
# MAGIC         WHEN 3 THEN 'Asymptomatic'
# MAGIC     END as chest_pain_type,
# MAGIC     COUNT(*) as count,
# MAGIC     ROUND(AVG(CASE WHEN target = 1 THEN 1.0 ELSE 0.0 END) * 100, 1) as disease_rate_pct
# MAGIC FROM solution_heart_disease
# MAGIC GROUP BY cp
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 3: Top 5 countries by average life expectancy
# MAGIC SELECT 
# MAGIC     country,
# MAGIC     ROUND(AVG(life_expectancy), 1) as avg_life_expectancy,
# MAGIC     ROUND(AVG(gdp_per_capita), 0) as avg_gdp,
# MAGIC     ROUND(AVG(schooling), 1) as avg_schooling
# MAGIC FROM solution_life_expectancy
# MAGIC GROUP BY country
# MAGIC ORDER BY avg_life_expectancy DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Visualizations

# COMMAND ----------

import plotly.express as px

# Visualization 1: Heart disease by age group
df_viz = spark.sql(f"""
    SELECT 
        CASE 
            WHEN age < 40 THEN 'Under 40'
            WHEN age < 50 THEN '40-49'
            WHEN age < 60 THEN '50-59'
            ELSE '60+'
        END as age_group,
        CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END as status,
        COUNT(*) as count
    FROM {TEAM_NAME}_heart_disease
    GROUP BY 1, 2
    ORDER BY 1
""").toPandas()

fig = px.bar(
    df_viz, x="age_group", y="count", color="status", barmode="group",
    title="Heart Disease Distribution by Age Group",
    labels={"count": "Number of Patients", "age_group": "Age Group"},
    color_discrete_map={"Heart Disease": "#e74c3c", "Healthy": "#2ecc71"}
)
fig.update_layout(template="plotly_white")
fig.show()

# COMMAND ----------

# Visualization 2: Life expectancy trends
df_viz2 = spark.sql(f"""
    SELECT country, year, life_expectancy
    FROM {TEAM_NAME}_life_expectancy
    WHERE country IN ('India', 'United States', 'Japan', 'Brazil', 'South Africa')
    ORDER BY country, year
""").toPandas()

fig2 = px.line(
    df_viz2, x="year", y="life_expectancy", color="country",
    title="Life Expectancy Trends by Country (2000-2023)",
    labels={"life_expectancy": "Life Expectancy (years)", "year": "Year"},
)
fig2.update_layout(template="plotly_white")
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

print("=" * 60)
print("SPEED SPRINT VALIDATION — SOLUTION")
print("=" * 60)

score = 0
# Check tables
for tbl in [f"{TEAM_NAME}_heart_disease", f"{TEAM_NAME}_life_expectancy"]:
    cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM {tbl}").collect()[0].cnt
    print(f"  ✅ {tbl}: {cnt} rows")
    score += 1

# Check Delta format
for tbl in [f"{TEAM_NAME}_heart_disease", f"{TEAM_NAME}_life_expectancy"]:
    detail = spark.sql(f"DESCRIBE DETAIL {tbl}").collect()[0]
    print(f"  ✅ {tbl}: format = {detail.format}")
    score += 1

print(f"\n  Score: {score}/4 automated checks passed")
print("=" * 60)
