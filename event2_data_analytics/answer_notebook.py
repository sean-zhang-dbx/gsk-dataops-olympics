# Databricks notebook source
# MAGIC %md
# MAGIC # Event 2: Answer Key — Complete Solutions
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY — Do not share with participants!**
# MAGIC
# MAGIC This notebook contains complete SQL solutions for:
# MAGIC - All 3 practice queries from the Build Phase
# MAGIC - All 8 benchmark questions with expected output
# MAGIC
# MAGIC Every benchmark answer is a **single value** — one number or one word.

# COMMAND ----------

CATALOG = "team_01"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA default")
print(f"Using: {CATALOG}.default")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Practice Query Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Practice 1: Heart Disease Prevalence

# COMMAND ----------

display(spark.sql("""
    SELECT
        COUNT(*) AS total_patients,
        SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) AS with_heart_disease,
        SUM(CASE WHEN target = 0 THEN 1 ELSE 0 END) AS healthy,
        ROUND(SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS disease_pct
    FROM heart_silver_correct
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Practice 2: Age Profile by Diagnosis

# COMMAND ----------

display(spark.sql("""
    SELECT
        CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
        COUNT(*) AS patient_count,
        ROUND(AVG(age), 1) AS avg_age,
        MIN(age) AS min_age,
        MAX(age) AS max_age
    FROM heart_silver_correct
    GROUP BY target
    ORDER BY target DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Practice 3: Chest Pain Distribution

# COMMAND ----------

display(spark.sql("""
    SELECT
        CASE
            WHEN cp = 0 THEN '0: Typical Angina'
            WHEN cp = 1 THEN '1: Atypical Angina'
            WHEN cp = 2 THEN '2: Non-Anginal Pain'
            WHEN cp = 3 THEN '3: Asymptomatic'
        END AS chest_pain_type,
        SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) AS heart_disease,
        SUM(CASE WHEN target = 0 THEN 1 ELSE 0 END) AS healthy,
        COUNT(*) AS total
    FROM heart_silver_correct
    GROUP BY cp
    ORDER BY cp
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Benchmark Question Solutions
# MAGIC
# MAGIC Every answer is a **single value**. The expected answer is shown after each query.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1: How many patients have heart disease (target = 1)?
# MAGIC **Format:** whole number. **Answer: 264**

# COMMAND ----------

display(spark.sql("""
    SELECT COUNT(*) AS answer
    FROM heart_silver_correct
    WHERE target = 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2: Average age of heart disease patients (target = 1)?
# MAGIC **Format:** number rounded to 1 decimal. **Answer: 56.1**

# COMMAND ----------

display(spark.sql("""
    SELECT ROUND(AVG(age), 1) AS answer
    FROM heart_silver_correct
    WHERE target = 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3: What percentage of female patients (sex = 0) have heart disease?
# MAGIC **Format:** number rounded to 1 decimal (no % sign). **Answer: 46.8**

# COMMAND ----------

display(spark.sql("""
    SELECT ROUND(
        SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
    ) AS answer
    FROM heart_silver_correct
    WHERE sex = 0
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4: cp value of the most common chest pain type among heart disease patients?
# MAGIC **Format:** single digit (0-3). **Answer: 3**

# COMMAND ----------

display(spark.sql("""
    SELECT cp, COUNT(*) AS cnt
    FROM heart_silver_correct
    WHERE target = 1
    GROUP BY cp
    ORDER BY cnt DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q5: Average cholesterol for Heart Disease patients in the 60+ age group?
# MAGIC **Format:** number rounded to 1 decimal. **Answer: 253.8**

# COMMAND ----------

display(spark.sql("""
    SELECT ROUND(avg_cholesterol, 1) AS answer
    FROM heart_gold_correct
    WHERE age_group = '60+' AND diagnosis = 'Heart Disease'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q6: Patients with cholesterol > 240 AND blood pressure > 140?
# MAGIC **Format:** whole number. **Answer: 71**

# COMMAND ----------

display(spark.sql("""
    SELECT COUNT(*) AS answer
    FROM heart_silver_correct
    WHERE chol > 240 AND trestbps > 140
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q7: Which age group has the highest heart disease rate?
# MAGIC **Format:** age group name. **Answer: 60+**

# COMMAND ----------

display(spark.sql("""
    SELECT
        CASE
            WHEN age < 40 THEN 'Under 40'
            WHEN age < 50 THEN '40-49'
            WHEN age < 60 THEN '50-59'
            ELSE '60+'
        END AS age_group,
        COUNT(*) AS total,
        SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) AS with_disease,
        ROUND(SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS disease_rate_pct
    FROM heart_silver_correct
    GROUP BY 1
    ORDER BY disease_rate_pct DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q8: Average max heart rate for healthy patients under 50?
# MAGIC **Format:** number rounded to 1 decimal. **Answer: 150.4**

# COMMAND ----------

display(spark.sql("""
    SELECT ROUND(AVG(thalach), 1) AS answer
    FROM heart_silver_correct
    WHERE target = 0 AND age < 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Quick Reference: All Answers
# MAGIC
# MAGIC | Q | Answer | Format |
# MAGIC |---|--------|--------|
# MAGIC | Q1 | 264 | whole number |
# MAGIC | Q2 | 56.1 | decimal |
# MAGIC | Q3 | 46.8 | decimal (no %) |
# MAGIC | Q4 | 3 | single digit |
# MAGIC | Q5 | 253.8 | decimal |
# MAGIC | Q6 | 71 | whole number |
# MAGIC | Q7 | 60+ | age group name |
# MAGIC | Q8 | 150.4 | decimal |
