# Databricks notebook source
# MAGIC %md
# MAGIC # 🏅 Event 4: The Relay Challenge — Leg 1
# MAGIC
# MAGIC ## Person A: Data Ingestion & Cleansing
# MAGIC **Time Limit: 7 minutes**
# MAGIC
# MAGIC ### Your Mission
# MAGIC Ingest raw data, clean it, and create a reliable Delta table for your teammate.
# MAGIC
# MAGIC ### Checkpoint (Baton Pass)
# MAGIC Your table must:
# MAGIC - ✅ Be in Delta format
# MAGIC - ✅ Have no null values in key columns
# MAGIC - ✅ Have correct data types
# MAGIC - ✅ Pass the validation cell at the bottom
# MAGIC
# MAGIC > ⚠️ If checkpoint fails: **2-minute penalty** added to team time!
# MAGIC
# MAGIC > ⏱️ START YOUR TIMER NOW!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

TEAM_NAME = "_____"  # e.g., "team_01"
TABLE_NAME = f"{TEAM_NAME}_relay_clean"

print(f"Team: {TEAM_NAME}")
print(f"Output table: {TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Ingest Raw Data
# MAGIC
# MAGIC Read both the CSV and JSON raw files and combine them.

# COMMAND ----------

# TODO: Read heart disease CSV
csv_path = "file:/tmp/dataops_olympics/raw/heart_disease/heart.csv"

df_heart = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(csv_path)
)

print(f"Heart disease records: {df_heart.count()}")
display(df_heart.limit(5))

# COMMAND ----------

# TODO: Read life expectancy data
life_path = "file:/tmp/dataops_olympics/raw/life_expectancy/life_expectancy.csv"

df_life = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(life_path)
)

print(f"Life expectancy records: {df_life.count()}")
display(df_life.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Data Cleansing
# MAGIC
# MAGIC **TODO:** Clean the life expectancy dataset:
# MAGIC 1. Handle null values (drop or impute)
# MAGIC 2. Fix data types
# MAGIC 3. Remove duplicates
# MAGIC 4. Add data quality columns

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# TODO: Clean the data
# Step 1: Drop rows with nulls in critical columns
df_clean = df_life.dropna(subset=["country", "year", "life_expectancy"])

# Step 2: Fill remaining nulls with sensible defaults
# Hint: Use .fillna() or .na.fill()
numeric_cols = [c for c, t in df_life.dtypes if t in ("double", "int", "bigint", "float") and c not in ("year",)]

# TODO: Fill numeric nulls with column medians or zeros
for col in numeric_cols:
    median_val = df_clean.approxQuantile(col, [0.5], 0.01)
    if median_val:
        df_clean = df_clean.fillna({col: median_val[0]})

# Step 3: Remove duplicates
df_clean = df_clean.dropDuplicates(["country", "year"])

# Step 4: Add data quality columns
df_clean = (
    df_clean
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source", F.lit("who_life_expectancy"))
    .withColumn("_team", F.lit(TEAM_NAME))
)

print(f"Clean records: {df_clean.count()}")
print(f"Columns: {len(df_clean.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Save as Delta Table

# COMMAND ----------

# TODO: Save cleaned data as Delta table
df_clean.write.format("delta").mode("overwrite").saveAsTable(TABLE_NAME)

# Add table comment
spark.sql(f"""
    ALTER TABLE {TABLE_NAME} 
    SET TBLPROPERTIES ('comment' = 'Cleaned WHO Life Expectancy data - Relay Challenge Leg 1')
""")

print(f"✅ Table {TABLE_NAME} created!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ CHECKPOINT — Run This to Pass the Baton!

# COMMAND ----------

print("=" * 60)
print("LEG 1 CHECKPOINT VALIDATION")
print("=" * 60)

passed = True
penalties = 0

# Check 1: Table exists
try:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {TABLE_NAME}").collect()[0].cnt
    print(f"  ✅ Table exists: {count} rows")
except:
    print(f"  ❌ Table {TABLE_NAME} not found!")
    passed = False

# Check 2: Delta format
try:
    detail = spark.sql(f"DESCRIBE DETAIL {TABLE_NAME}").collect()[0]
    if detail.format == "delta":
        print(f"  ✅ Delta format confirmed")
    else:
        print(f"  ❌ Not Delta format: {detail.format}")
        passed = False
except:
    print(f"  ❌ Could not verify format")
    passed = False

# Check 3: No nulls in key columns
try:
    null_count = spark.sql(f"""
        SELECT COUNT(*) as nulls FROM {TABLE_NAME} 
        WHERE country IS NULL OR year IS NULL OR life_expectancy IS NULL
    """).collect()[0].nulls
    if null_count == 0:
        print(f"  ✅ No null values in key columns")
    else:
        print(f"  ❌ Found {null_count} nulls in key columns!")
        passed = False
except:
    print(f"  ❌ Could not check nulls")
    passed = False

# Check 4: No duplicates
try:
    total = spark.sql(f"SELECT COUNT(*) as cnt FROM {TABLE_NAME}").collect()[0].cnt
    distinct = spark.sql(f"SELECT COUNT(DISTINCT country, year) as cnt FROM {TABLE_NAME}").collect()[0].cnt
    if total == distinct:
        print(f"  ✅ No duplicate country-year combinations")
    else:
        print(f"  ❌ Found {total - distinct} duplicate rows!")
        passed = False
except:
    print(f"  ❌ Could not check duplicates")
    passed = False

# Check 5: Metadata columns
try:
    cols = [c.name for c in spark.table(TABLE_NAME).schema]
    has_meta = all(c in cols for c in ["_ingested_at", "_source", "_team"])
    if has_meta:
        print(f"  ✅ Metadata columns present")
    else:
        print(f"  ⚠️  Missing metadata columns (minor)")
except:
    print(f"  ⚠️  Could not verify metadata")

print(f"\n{'='*60}")
if passed:
    print(f"  🎉 CHECKPOINT PASSED! Hand the baton to Person B!")
    print(f"  📌 Table name for Person B: {TABLE_NAME}")
else:
    print(f"  ❌ CHECKPOINT FAILED — 2 MINUTE PENALTY!")
    print(f"  Fix the issues above and re-run validation.")
print(f"{'='*60}")
