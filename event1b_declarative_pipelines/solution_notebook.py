# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 Event 1B: Declarative Pipelines — SOLUTION
# MAGIC
# MAGIC > **⚠️ ORGANIZERS ONLY — Do not distribute to participants!**
# MAGIC
# MAGIC This notebook is a **complete DLT pipeline definition** ready to run as a
# MAGIC Delta Live Tables pipeline. It can also be previewed cell-by-cell in notebook mode.

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Pipeline Code
# MAGIC
# MAGIC When running as a DLT pipeline, **only the cells with `import dlt`** are executed.
# MAGIC The preview cells are ignored by the DLT runtime.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# Data source — adjust path as needed
DATA_SOURCE = "/tmp/dataops_olympics/raw/heart_disease/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze: Raw Ingestion

# COMMAND ----------

@dlt.table(
    name="bronze_heart_disease",
    comment="Raw heart disease data ingested from CSV batch files. No transformations applied.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_heart_disease():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(DATA_SOURCE + "*.csv")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", 
            F.regexp_extract(F.input_file_name(), r"batch_(\d+)", 1))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver: Cleaned & Validated
# MAGIC
# MAGIC - 5 data quality expectations
# MAGIC - Derived columns for analytics
# MAGIC - Deduplication

# COMMAND ----------

@dlt.table(
    name="silver_heart_disease",
    comment="Cleaned heart disease data with quality expectations and derived features.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"   # Bonus: CDF enabled
    }
)
@dlt.expect("valid_age", "age BETWEEN 1 AND 120")
@dlt.expect_or_drop("positive_blood_pressure", "trestbps BETWEEN 50 AND 300")
@dlt.expect("positive_cholesterol", "chol > 0")
@dlt.expect("valid_heart_rate", "thalach BETWEEN 40 AND 220")
@dlt.expect_or_drop("valid_chest_pain_type", "cp IN (0, 1, 2, 3)")
def silver_heart_disease():
    return (
        dlt.read("bronze_heart_disease")
        .filter("age > 0")
        
        # Cast types explicitly
        .withColumn("age", F.col("age").cast("int"))
        .withColumn("chol", F.col("chol").cast("int"))
        .withColumn("trestbps", F.col("trestbps").cast("int"))
        .withColumn("thalach", F.col("thalach").cast("int"))
        .withColumn("target", F.col("target").cast("int"))
        
        # Derived: Age group
        .withColumn("age_group",
            F.when(F.col("age") < 40, "Under 40")
             .when(F.col("age") < 50, "40-49")
             .when(F.col("age") < 60, "50-59")
             .otherwise("60+"))
        
        # Derived: Chest pain description
        .withColumn("chest_pain_type",
            F.when(F.col("cp") == 0, "Typical Angina")
             .when(F.col("cp") == 1, "Atypical Angina")
             .when(F.col("cp") == 2, "Non-Anginal Pain")
             .otherwise("Asymptomatic"))
        
        # Derived: Risk category
        .withColumn("risk_category",
            F.when(
                (F.col("cp") == 3) & (F.col("trestbps") > 140) & (F.col("age") > 55),
                "High Risk"
            ).when(
                (F.col("cp").isin(2, 3)) | (F.col("trestbps") > 160),
                "Moderate Risk"
            ).otherwise("Standard"))
        
        # Derived: Cardiac fitness indicator
        .withColumn("cardiac_fitness",
            F.when(F.col("thalach") > 150, "Good")
             .when(F.col("thalach") > 100, "Fair")
             .otherwise("Poor"))
        
        # Deduplication
        .dropDuplicates(["age", "sex", "cp", "trestbps", "chol", "thalach"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold: Business Aggregates

# COMMAND ----------

@dlt.table(
    name="gold_risk_summary_by_age",
    comment="Heart disease risk statistics aggregated by age group.",
    table_properties={"quality": "gold"}
)
def gold_risk_summary_by_age():
    return (
        dlt.read("silver_heart_disease")
        .groupBy("age_group")
        .agg(
            F.count("*").alias("total_patients"),
            F.sum("target").alias("disease_count"),
            F.round(F.avg("target"), 4).alias("disease_rate"),
            F.round(F.avg("chol"), 1).alias("avg_cholesterol"),
            F.round(F.avg("trestbps"), 1).alias("avg_blood_pressure"),
            F.round(F.avg("thalach"), 1).alias("avg_max_heart_rate"),
            F.round(F.avg("oldpeak"), 2).alias("avg_st_depression"),
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_risk_by_pain_type",
    comment="Heart disease risk statistics by chest pain presentation.",
    table_properties={"quality": "gold"}
)
def gold_risk_by_pain_type():
    return (
        dlt.read("silver_heart_disease")
        .groupBy("chest_pain_type")
        .agg(
            F.count("*").alias("total_patients"),
            F.round(F.avg("target"), 4).alias("disease_rate"),
            F.round(F.avg("age"), 1).alias("avg_age"),
            F.round(F.avg("chol"), 1).alias("avg_cholesterol"),
            F.countDistinct("risk_category").alias("risk_categories"),
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_high_risk_patients",
    comment="Filtered list of high-risk patients for clinical review.",
    table_properties={"quality": "gold"}
)
def gold_high_risk_patients():
    return (
        dlt.read("silver_heart_disease")
        .filter("risk_category = 'High Risk' OR target = 1")
        .select(
            "age", "sex", "chest_pain_type", "trestbps", "chol",
            "thalach", "oldpeak", "risk_category", "cardiac_fitness",
            "age_group", "target"
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_pipeline_quality_metrics",
    comment="Data quality metrics across the pipeline.",
    table_properties={"quality": "gold"}
)
def gold_pipeline_quality_metrics():
    bronze = dlt.read("bronze_heart_disease")
    silver = dlt.read("silver_heart_disease")
    
    bronze_count = bronze.count()
    silver_count = silver.count()
    
    return spark.createDataFrame([{
        "bronze_record_count": bronze_count,
        "silver_record_count": silver_count,
        "records_dropped": bronze_count - silver_count,
        "drop_rate": round((bronze_count - silver_count) / bronze_count, 4) if bronze_count > 0 else 0,
        "pipeline_run_time": F.current_timestamp(),
    }])
