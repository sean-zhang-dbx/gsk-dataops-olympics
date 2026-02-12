# Databricks notebook source
# MAGIC %md
# MAGIC # 🏅 Event 4: The Relay Challenge — Leg 2
# MAGIC
# MAGIC ## Person B: Feature Engineering
# MAGIC **Time Limit: 8 minutes**
# MAGIC
# MAGIC ### Your Mission
# MAGIC Take the cleaned data from Person A and engineer features for ML.
# MAGIC
# MAGIC ### Input
# MAGIC Table from Leg 1: `{TEAM_NAME}_relay_clean`
# MAGIC
# MAGIC ### Checkpoint (Baton Pass)
# MAGIC Your feature table must:
# MAGIC - ✅ Have at least 5 engineered features
# MAGIC - ✅ Have a proper train/test split saved
# MAGIC - ✅ Be registered with column descriptions
# MAGIC - ✅ Pass the validation cell at the bottom
# MAGIC
# MAGIC > ⚠️ If checkpoint fails: **2-minute penalty** added to team time!
# MAGIC
# MAGIC > ⏱️ START YOUR TIMER NOW!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

TEAM_NAME = "_____"  # e.g., "team_01" — MUST MATCH LEG 1!
INPUT_TABLE = f"{TEAM_NAME}_relay_clean"
FEATURES_TABLE = f"{TEAM_NAME}_relay_features"
TRAIN_TABLE = f"{TEAM_NAME}_relay_train"
TEST_TABLE = f"{TEAM_NAME}_relay_test"

print(f"Input:    {INPUT_TABLE}")
print(f"Features: {FEATURES_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Load Cleaned Data

# COMMAND ----------

df = spark.table(INPUT_TABLE)
print(f"Loaded {df.count()} rows, {len(df.columns)} columns")
display(df.limit(5))

# COMMAND ----------

# Explore the data to plan features
df.describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Feature Engineering
# MAGIC
# MAGIC **TODO:** Create at least 5 new features. Ideas:
# MAGIC 1. Health index (composite score)
# MAGIC 2. GDP per capita bins (low/medium/high)
# MAGIC 3. Year-over-year change in life expectancy
# MAGIC 4. Regional averages
# MAGIC 5. Vaccination coverage score
# MAGIC 6. Disease burden index

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# TODO: Feature Engineering - Create at least 5 new features!

# Feature 1: Health Expenditure Category
df_feat = df.withColumn(
    "health_spend_category",
    F.when(F.col("health_expenditure_pct") < 5, "Low")
     .when(F.col("health_expenditure_pct") < 10, "Medium")
     .otherwise("High")
)

# Feature 2: Vaccination Coverage Score (average of available vaccines)
df_feat = df_feat.withColumn(
    "vaccination_score",
    (F.col("hepatitis_b_coverage") + F.col("polio_coverage") + F.col("diphtheria_coverage")) / 3
)

# Feature 3: TODO - Add your own feature
# Hint: Consider year-over-year change using Window functions
# w = Window.partitionBy("country").orderBy("year")
# df_feat = df_feat.withColumn("life_exp_change", F.col("life_expectancy") - F.lag("life_expectancy").over(w))

df_feat = df_feat.withColumn(
    "_____",  # YOUR FEATURE NAME
    _____  # YOUR FEATURE LOGIC
)

# Feature 4: TODO - Add your own feature
df_feat = df_feat.withColumn(
    "_____",
    _____
)

# Feature 5: TODO - Add your own feature
df_feat = df_feat.withColumn(
    "_____",
    _____
)

print(f"Total features: {len(df_feat.columns)}")
display(df_feat.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Prepare Target Variable
# MAGIC
# MAGIC **TODO:** Create a binary classification target.
# MAGIC
# MAGIC Let's predict: **Is life expectancy above the global median?**

# COMMAND ----------

# Calculate global median life expectancy
median_le = df_feat.approxQuantile("life_expectancy", [0.5], 0.01)[0]
print(f"Median life expectancy: {median_le:.1f} years")

# Create binary target
df_feat = df_feat.withColumn(
    "target_above_median",
    F.when(F.col("life_expectancy") > median_le, 1).otherwise(0)
)

# Check distribution
df_feat.groupBy("target_above_median").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Train/Test Split & Save

# COMMAND ----------

# Select numeric features for ML (drop string columns and metadata)
from pyspark.sql.types import NumericType

numeric_cols = [
    f.name for f in df_feat.schema.fields 
    if isinstance(f.dataType, NumericType) 
    and f.name not in ("year", "target_above_median")
    and not f.name.startswith("_")
]

# Add target
select_cols = numeric_cols + ["target_above_median", "country", "year"]

df_ml = df_feat.select(select_cols)

# Train/Test split (80/20)
train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)

# Save tables
train_df.write.format("delta").mode("overwrite").saveAsTable(TRAIN_TABLE)
test_df.write.format("delta").mode("overwrite").saveAsTable(TEST_TABLE)
df_feat.write.format("delta").mode("overwrite").saveAsTable(FEATURES_TABLE)

print(f"✅ Training set: {train_df.count()} rows saved to {TRAIN_TABLE}")
print(f"✅ Test set:     {test_df.count()} rows saved to {TEST_TABLE}")
print(f"✅ Feature table: {df_feat.count()} rows saved to {FEATURES_TABLE}")
print(f"\n📊 Feature columns for ML: {numeric_cols}")

# COMMAND ----------

# Add column descriptions
for col in numeric_cols[:5]:
    try:
        spark.sql(f"ALTER TABLE {FEATURES_TABLE} ALTER COLUMN `{col}` COMMENT 'Numeric feature for ML'")
    except:
        pass

try:
    spark.sql(f"ALTER TABLE {FEATURES_TABLE} ALTER COLUMN target_above_median COMMENT 'Binary target: 1 if life expectancy above global median'")
except:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ CHECKPOINT — Run This to Pass the Baton!

# COMMAND ----------

print("=" * 60)
print("LEG 2 CHECKPOINT VALIDATION")
print("=" * 60)

passed = True

# Check 1: Feature table exists with enough features
try:
    feat_cols = [c for c in spark.table(FEATURES_TABLE).columns if not c.startswith("_")]
    original_cols = [c for c in spark.table(INPUT_TABLE).columns if not c.startswith("_")]
    new_features = len(feat_cols) - len(original_cols)
    if new_features >= 5:
        print(f"  ✅ {new_features} new features created (minimum 5)")
    else:
        print(f"  ❌ Only {new_features} new features (need at least 5)")
        passed = False
except Exception as e:
    print(f"  ❌ Feature table error: {e}")
    passed = False

# Check 2: Train table exists
try:
    train_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {TRAIN_TABLE}").collect()[0].cnt
    print(f"  ✅ Training set: {train_count} rows")
except:
    print(f"  ❌ Training table not found!")
    passed = False

# Check 3: Test table exists
try:
    test_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {TEST_TABLE}").collect()[0].cnt
    print(f"  ✅ Test set: {test_count} rows")
except:
    print(f"  ❌ Test table not found!")
    passed = False

# Check 4: Proper split ratio
try:
    total = train_count + test_count
    ratio = train_count / total
    if 0.7 <= ratio <= 0.9:
        print(f"  ✅ Train/Test ratio: {ratio:.1%}/{1-ratio:.1%}")
    else:
        print(f"  ⚠️  Unusual split ratio: {ratio:.1%}")
except:
    pass

# Check 5: Target variable exists
try:
    target_dist = spark.sql(f"SELECT target_above_median, COUNT(*) as cnt FROM {TRAIN_TABLE} GROUP BY 1").collect()
    print(f"  ✅ Target variable present with {len(target_dist)} classes")
except:
    print(f"  ❌ Target variable 'target_above_median' not found!")
    passed = False

print(f"\n{'='*60}")
if passed:
    print(f"  🎉 CHECKPOINT PASSED! Hand the baton to Person C!")
    print(f"  📌 Tables for Person C:")
    print(f"      Training: {TRAIN_TABLE}")
    print(f"      Test:     {TEST_TABLE}")
    print(f"      Features: {numeric_cols}")
else:
    print(f"  ❌ CHECKPOINT FAILED — 2 MINUTE PENALTY!")
    print(f"  Fix the issues above and re-run validation.")
print(f"{'='*60}")
