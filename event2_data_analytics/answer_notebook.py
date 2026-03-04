# Databricks notebook source
# MAGIC %md
# MAGIC # Event 2: Answer Key — Complete Solutions
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY — Do not share with participants!**
# MAGIC
# MAGIC This notebook contains complete SQL solutions for:
# MAGIC - All 3 practice queries from the Build Phase
# MAGIC - All 8 benchmark questions with expected output

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
    FROM heart_silver
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
    FROM heart_silver
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
    FROM heart_silver
    GROUP BY cp
    ORDER BY cp
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Benchmark Question Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1: How many patients have heart disease?

# COMMAND ----------

display(spark.sql("""
    SELECT COUNT(*) AS patients_with_heart_disease
    FROM heart_silver
    WHERE target = 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2: Average age — heart disease vs healthy?

# COMMAND ----------

display(spark.sql("""
    SELECT
        CASE WHEN target = 1 THEN 'Heart Disease' ELSE 'Healthy' END AS diagnosis,
        ROUND(AVG(age), 1) AS avg_age
    FROM heart_silver
    GROUP BY target
    ORDER BY target DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3: Percentage of female patients with heart disease?

# COMMAND ----------

display(spark.sql("""
    SELECT
        COUNT(*) AS total_female,
        SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) AS female_with_disease,
        ROUND(SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS disease_pct
    FROM heart_silver
    WHERE sex = 0
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4: Most common chest pain type among heart disease patients?

# COMMAND ----------

display(spark.sql("""
    SELECT
        cp,
        CASE
            WHEN cp = 0 THEN 'typical angina'
            WHEN cp = 1 THEN 'atypical angina'
            WHEN cp = 2 THEN 'non-anginal pain'
            WHEN cp = 3 THEN 'asymptomatic'
        END AS chest_pain_name,
        COUNT(*) AS patient_count
    FROM heart_silver
    WHERE target = 1
    GROUP BY cp
    ORDER BY patient_count DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q5: Average cholesterol for 60+ age group?

# COMMAND ----------

display(spark.sql("""
    SELECT age_group, diagnosis, avg_cholesterol
    FROM heart_gold
    WHERE age_group = '60+'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q6: Patients with cholesterol > 240 AND BP > 140?

# COMMAND ----------

display(spark.sql("""
    SELECT COUNT(*) AS high_risk_patients
    FROM heart_silver
    WHERE chol > 240 AND trestbps > 140
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q7: Age group with highest heart disease rate?

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
    FROM heart_silver
    GROUP BY 1
    ORDER BY disease_rate_pct DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q8: Average max heart rate for healthy patients under 50?

# COMMAND ----------

display(spark.sql("""
    SELECT ROUND(AVG(thalach), 1) AS avg_max_heart_rate
    FROM heart_silver
    WHERE target = 0 AND age < 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Genie Prompt Examples
# MAGIC
# MAGIC These are the natural language prompts a well-configured Genie should handle:
# MAGIC
# MAGIC | Question | Expected Genie Prompt |
# MAGIC |----------|----------------------|
# MAGIC | Q1 | "How many patients have heart disease?" |
# MAGIC | Q2 | "What is the average age for heart disease patients versus healthy?" |
# MAGIC | Q3 | "What percentage of female patients have heart disease?" |
# MAGIC | Q4 | "What chest pain type is most common in heart disease patients?" |
# MAGIC | Q5 | "What is the average cholesterol for patients aged 60 and over?" |
# MAGIC | Q6 | "How many patients have cholesterol above 240 and blood pressure above 140?" |
# MAGIC | Q7 | "Which age group has the highest rate of heart disease?" |
# MAGIC | Q8 | "Average maximum heart rate for healthy patients younger than 50?" |
