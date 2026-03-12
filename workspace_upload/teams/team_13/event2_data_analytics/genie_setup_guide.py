# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Space Setup Guide — Heart Disease Analyst
# MAGIC
# MAGIC **Your goal: create a Genie space that can answer clinical questions in natural language.**
# MAGIC
# MAGIC A well-configured Genie space earns **3 pts per correct answer** in the benchmark race
# MAGIC (vs 2 pts for raw SQL). Invest a few minutes here — it pays off.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 1: Create Your Genie Space
# MAGIC
# MAGIC 1. In the Databricks sidebar, click **+ New** → **Genie space**
# MAGIC 2. **Title:** `{TEAM_NAME} Heart Disease Analyst`
# MAGIC 3. **Description:** "Analyze heart disease patient data from a hospital intake system"
# MAGIC 4. **SQL Warehouse:** Select any available warehouse (pro or serverless)
# MAGIC 5. Click **Create**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 2: Add Your Tables
# MAGIC
# MAGIC In the Genie space settings (gear icon), add these tables:
# MAGIC
# MAGIC - `{TEAM_NAME}.default.heart_silver` — patient-level clinical data (~488 rows)
# MAGIC - `{TEAM_NAME}.default.heart_gold` — aggregated metrics by age group (~8 rows)
# MAGIC
# MAGIC > Replace `{TEAM_NAME}` with your actual team name (e.g., `team_01`).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 3: Add Instructions
# MAGIC
# MAGIC Go to the **Instructions** tab in your Genie space settings.
# MAGIC **Copy and paste the entire block below** into the general instructions field:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### COPY THIS — General Instructions
# MAGIC
# MAGIC ```
# MAGIC DOMAIN: Heart disease patient analytics from a hospital intake system.
# MAGIC
# MAGIC DATA TABLES:
# MAGIC - heart_silver: Patient-level clinical records. Each row is one patient visit.
# MAGIC   ~488 rows after data quality filtering.
# MAGIC - heart_gold: Pre-aggregated metrics grouped by age_group and diagnosis.
# MAGIC   ~8 rows (4 age groups x 2 diagnoses).
# MAGIC
# MAGIC KEY COLUMNS IN heart_silver:
# MAGIC - target: Diagnosis flag. 1 = heart disease present, 0 = healthy/no disease.
# MAGIC - sex: Patient sex. 0 = female, 1 = male.
# MAGIC - cp: Chest pain type. 0 = typical angina, 1 = atypical angina,
# MAGIC   2 = non-anginal pain, 3 = asymptomatic.
# MAGIC - age: Patient age in years (validated 1-120).
# MAGIC - chol: Serum cholesterol in mg/dL.
# MAGIC - trestbps: Resting blood pressure in mmHg.
# MAGIC - thalach: Maximum heart rate achieved during exercise testing.
# MAGIC - fbs: Fasting blood sugar > 120 mg/dL (1 = true, 0 = false).
# MAGIC - exang: Exercise-induced angina (1 = yes, 0 = no).
# MAGIC - oldpeak: ST depression induced by exercise relative to rest.
# MAGIC - ca: Number of major vessels colored by fluoroscopy (0-3).
# MAGIC - restecg: Resting ECG results. 0 = normal, 1 = ST-T wave abnormality,
# MAGIC   2 = probable or definite left ventricular hypertrophy.
# MAGIC
# MAGIC KEY COLUMNS IN heart_gold:
# MAGIC - age_group: One of "Under 40", "40-49", "50-59", "60+".
# MAGIC - diagnosis: "Heart Disease" (target=1) or "Healthy" (target=0).
# MAGIC - patient_count: Number of patients in that group.
# MAGIC - avg_cholesterol, avg_blood_pressure, avg_max_heart_rate: Averages per group.
# MAGIC
# MAGIC RULES:
# MAGIC - When asked about "heart disease patients", filter where target = 1.
# MAGIC - When asked about "healthy patients", filter where target = 0.
# MAGIC - When asked about "disease rate" or "prevalence", calculate:
# MAGIC   COUNT(target=1) / COUNT(*) * 100 as a percentage.
# MAGIC - Always round percentages to 1 decimal place.
# MAGIC - Always round averages to 1 decimal place.
# MAGIC - When grouping by age, use these buckets:
# MAGIC   Under 40 (age < 40), 40-49 (40 <= age < 50),
# MAGIC   50-59 (50 <= age < 60), 60+ (age >= 60).
# MAGIC - For aggregated questions (by age group, by diagnosis), prefer heart_gold.
# MAGIC - For patient-level questions, use heart_silver.
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 4: Add Example SQL Queries
# MAGIC
# MAGIC In the **Example SQL** tab, add these queries. They teach Genie common patterns.
# MAGIC
# MAGIC ### Example 1: Disease Prevalence
# MAGIC
# MAGIC **Prompt:** "What percentage of patients have heart disease?"
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_patients,
# MAGIC   SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) AS with_disease,
# MAGIC   ROUND(SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS disease_pct
# MAGIC FROM heart_silver
# MAGIC ```
# MAGIC
# MAGIC ### Example 2: Metrics by Age Group
# MAGIC
# MAGIC **Prompt:** "What is the average cholesterol by age group?"
# MAGIC
# MAGIC ```sql
# MAGIC SELECT age_group, diagnosis, avg_cholesterol, avg_blood_pressure, avg_max_heart_rate
# MAGIC FROM heart_gold
# MAGIC ORDER BY age_group
# MAGIC ```
# MAGIC
# MAGIC ### Example 3: Filtered Count
# MAGIC
# MAGIC **Prompt:** "How many female patients have heart disease?"
# MAGIC
# MAGIC ```sql
# MAGIC SELECT COUNT(*) AS female_heart_disease
# MAGIC FROM heart_silver
# MAGIC WHERE sex = 0 AND target = 1
# MAGIC ```
# MAGIC
# MAGIC ### Example 4: Disease Rate by Group
# MAGIC
# MAGIC **Prompt:** "Which age group has the highest heart disease rate?"
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN age < 40 THEN 'Under 40'
# MAGIC     WHEN age < 50 THEN '40-49'
# MAGIC     WHEN age < 60 THEN '50-59'
# MAGIC     ELSE '60+'
# MAGIC   END AS age_group,
# MAGIC   ROUND(SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS disease_rate_pct
# MAGIC FROM heart_silver
# MAGIC GROUP BY 1
# MAGIC ORDER BY disease_rate_pct DESC
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 5: Add Column Metadata (Optional but Recommended)
# MAGIC
# MAGIC In the Genie space data settings, click on each table and add column descriptions.
# MAGIC Genie uses these to understand ambiguous column names:
# MAGIC
# MAGIC | Column | Synonyms / Description to Add |
# MAGIC |--------|-------------------------------|
# MAGIC | `target` | "diagnosis", "heart disease flag", "disease status" |
# MAGIC | `thalach` | "max heart rate", "maximum heart rate", "peak heart rate" |
# MAGIC | `trestbps` | "blood pressure", "resting BP", "systolic pressure" |
# MAGIC | `chol` | "cholesterol", "serum cholesterol" |
# MAGIC | `cp` | "chest pain", "chest pain type", "angina type" |
# MAGIC | `sex` | "gender" (note: 0=female, 1=male) |
# MAGIC | `fbs` | "fasting blood sugar", "blood sugar" |
# MAGIC | `exang` | "exercise angina", "exercise-induced chest pain" |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 6: Test Your Space
# MAGIC
# MAGIC Ask these questions to verify your setup:
# MAGIC
# MAGIC 1. "How many patients have heart disease?"
# MAGIC    - Expected: a count around 260-270
# MAGIC 2. "What is the average age of heart disease patients?"
# MAGIC    - Expected: a number around 52-54
# MAGIC 3. "Show the patient count by age group and diagnosis"
# MAGIC    - Expected: 8 rows from the Gold table
# MAGIC
# MAGIC If Genie gives wrong answers, check your instructions and column metadata.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Docs & Best Practices
# MAGIC
# MAGIC - [Curate an effective Genie space](https://docs.databricks.com/aws/en/genie/best-practices)
# MAGIC - [Set up and manage a Genie space](https://docs.databricks.com/aws/en/genie/set-up)
# MAGIC - [Build a knowledge store for Genie](https://docs.databricks.com/aws/en/genie/sample-values)
# MAGIC - [Genie blog: Best practices guide](https://www.databricks.com/blog/data-dialogue-best-practices-guide-building-high-performing-genie-spaces)
# MAGIC
# MAGIC ### Key Tips from Databricks Docs
# MAGIC
# MAGIC - **Start small, iterate:** Begin with minimal instructions; add more based on test results
# MAGIC - **SQL > text instructions:** Genie understands SQL expressions and examples better than plain text
# MAGIC - **Stay focused:** Include only the tables needed (we use just 2)
# MAGIC - **Add synonyms:** Business users say "cholesterol", not "chol" — synonyms bridge the gap
# MAGIC - **Test and adjust:** Be your space's first user; examine the SQL Genie generates
