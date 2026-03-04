# Databricks notebook source
# MAGIC %md
# MAGIC # Event 2: Data Analytics — SQL + Genie Race
# MAGIC
# MAGIC ## Fastest Analyst Wins
# MAGIC **Time: 20 minutes** | **Max Points: 40**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Scenario
# MAGIC
# MAGIC > Great work building that Medallion pipeline in Event 1! The hospital now has clean,
# MAGIC > governed patient data flowing from **Bronze → Silver → Gold**.
# MAGIC >
# MAGIC > The **Chief Medical Officer** saw the data and is impressed — but now the leadership
# MAGIC > team has **8 urgent clinical questions** about the heart disease patient population.
# MAGIC > They need answers *today*, and they're watching your team in real-time.
# MAGIC >
# MAGIC > **You choose how to answer each question:**
# MAGIC > - **SQL query** in this notebook → **2 pts** per correct answer
# MAGIC > - **Genie space** (natural language) → **3 pts** per correct answer
# MAGIC > - **First team** with the correct answer → **+1 speed bonus**
# MAGIC >
# MAGIC > Better Genie setup = higher point ceiling. But if Genie fails, SQL is your safety net.
# MAGIC
# MAGIC ### Event Structure
# MAGIC
# MAGIC | Phase | Time | What |
# MAGIC |-------|------|------|
# MAGIC | **Build Phase** | 8 min | Write practice SQL + set up your Genie space |
# MAGIC | **Benchmark Race** | 12 min | 8 questions announced 1-by-1, answer via SQL or Genie |
# MAGIC
# MAGIC ### Data — From YOUR Event 1 Pipeline
# MAGIC
# MAGIC You'll use the tables you built in Event 1 (in your team catalog):
# MAGIC - `heart_silver` — cleaned patient-level data (~488 rows)
# MAGIC - `heart_gold` — aggregated metrics by age group and diagnosis (~8 rows)
# MAGIC
# MAGIC > If Event 1 tables aren't ready, run the **fallback notebook** first:
# MAGIC > `event1_data_engineering/fallback_generate_tables` (set your TEAM_NAME).
# MAGIC
# MAGIC > **Vibe Coding:** Use **Databricks Assistant** (`Cmd+I`) to generate your SQL!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Team Configuration
# MAGIC
# MAGIC **CHANGE THIS** to your team name. Your team name is also your catalog name.

# COMMAND ----------

TEAM_NAME = "team_XX"  # <-- CHANGE THIS (e.g., "team_01")
CATALOG = TEAM_NAME

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA default")
print(f"Working in: {CATALOG}.default")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 0: Verify Your Event 1 Tables
# MAGIC
# MAGIC Make sure your Silver and Gold tables exist before starting.

# COMMAND ----------

try:
    silver_count = spark.table(f"{CATALOG}.default.heart_silver").count()
    gold_count = spark.table(f"{CATALOG}.default.heart_gold").count()
    print(f"  heart_silver: {silver_count} rows")
    print(f"  heart_gold:   {gold_count} rows")
    print(f"\n  Ready to go!")
except Exception as e:
    print(f"  ERROR: {e}")
    print(f"\n  Run the fallback notebook first:")
    print(f"  event1_data_engineering/fallback_generate_tables (TEAM_NAME = {TEAM_NAME})")

# COMMAND ----------

display(spark.table(f"{CATALOG}.default.heart_silver").limit(5))

# COMMAND ----------

display(spark.table(f"{CATALOG}.default.heart_gold"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # BUILD PHASE (8 minutes)
# MAGIC
# MAGIC Split your team:
# MAGIC - **Person A:** Write the 3 practice SQL queries below (warm-up for the race)
# MAGIC - **Person B:** Set up the Genie space using the **`genie_setup_guide`** notebook
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Practice SQL Queries
# MAGIC
# MAGIC Write 3 analytical queries against `heart_silver` to warm up.
# MAGIC Use Databricks Assistant (`Cmd+I`) to generate them from the business descriptions.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1: Heart Disease Prevalence
# MAGIC
# MAGIC > What percentage of patients in the dataset have heart disease (`target = 1`)?
# MAGIC > Return the total patients, count with heart disease, and the percentage.

# COMMAND ----------

# YOUR CODE HERE — use Databricks Assistant (Cmd+I)!
# Prompt: "What percentage of patients have heart disease (target=1)? Show total, count, and percentage."


# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2: Age Profile by Diagnosis
# MAGIC
# MAGIC > What is the average, minimum, and maximum age of patients grouped by diagnosis
# MAGIC > (`target`: 1 = Heart Disease, 0 = Healthy)?

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 3: Chest Pain Distribution
# MAGIC
# MAGIC > How are the 4 chest pain types (`cp`) distributed among heart disease patients vs healthy?
# MAGIC > cp values: 0 = typical angina, 1 = atypical angina, 2 = non-anginal pain, 3 = asymptomatic.

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Set Up Your Genie Space
# MAGIC
# MAGIC **Open the `genie_setup_guide` notebook** (in this same folder) for detailed instructions.
# MAGIC
# MAGIC ### Quick Checklist
# MAGIC
# MAGIC 1. **Create space:** Sidebar → **+ New** → **Genie space**
# MAGIC 2. **Name:** `{TEAM_NAME} Heart Disease Analyst`
# MAGIC 3. **Add tables:**
# MAGIC    - `{CATALOG}.default.heart_silver`
# MAGIC    - `{CATALOG}.default.heart_gold`
# MAGIC 4. **Add instructions** (copy from the Genie setup guide):
# MAGIC    - Column descriptions and synonyms
# MAGIC    - Domain knowledge about clinical data
# MAGIC    - Example SQL queries
# MAGIC 5. **Test** with a simple question: "How many patients have heart disease?"
# MAGIC
# MAGIC > The Genie setup guide has **copy-paste instructions** ready for you.
# MAGIC > A well-configured Genie = higher score potential in the race!

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # BENCHMARK RACE (12 minutes)
# MAGIC
# MAGIC The organizer will announce **8 questions** one at a time.
# MAGIC
# MAGIC ### Rules
# MAGIC - Answer via **SQL** (write below) = **2 pts** per correct answer
# MAGIC - Answer via **Genie** (type in your Genie space) = **3 pts** per correct answer
# MAGIC - **First team** with the correct answer = **+1 speed bonus**
# MAGIC - Wrong answer = 0 pts, you can retry
# MAGIC - **Raise your hand** when you have an answer!
# MAGIC
# MAGIC ### Strategy
# MAGIC - Simple lookups → Genie is faster (if well-configured)
# MAGIC - Complex calculations → SQL may be more reliable
# MAGIC - You can use BOTH tools — try Genie first, fall back to SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1
# MAGIC *(Question will be announced by the organizer)*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR SQL HERE (if answering via SQL)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR SQL HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR SQL HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR SQL HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ### Q5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR SQL HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ### Q6

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR SQL HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ### Q7

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR SQL HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ### Q8

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR SQL HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Record Your Answers
# MAGIC
# MAGIC Fill in your answers below. The organizer will collect these for scoring.

# COMMAND ----------

answers = {
    "Q1": "",  # fill in your answer
    "Q2": "",
    "Q3": "",
    "Q4": "",
    "Q5": "",
    "Q6": "",
    "Q7": "",
    "Q8": "",
}

method_used = {
    "Q1": "",  # "SQL" or "Genie"
    "Q2": "",
    "Q3": "",
    "Q4": "",
    "Q5": "",
    "Q6": "",
    "Q7": "",
    "Q8": "",
}

print("=" * 60)
print(f"  BENCHMARK ANSWERS — {TEAM_NAME}")
print("=" * 60)
for q in sorted(answers.keys()):
    m = method_used.get(q, "?")
    a = answers.get(q, "")
    print(f"  {q} [{m:5s}]: {a}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Bonus Challenges (Extra Credit)
# MAGIC
# MAGIC ### Bonus 1: AI-Powered Summary (+3 pts)
# MAGIC
# MAGIC > Use `ai_query()` to generate a natural language executive summary of your Gold table.
# MAGIC > Save the result as a table called `heart_executive_summary`.
# MAGIC >
# MAGIC > ```sql
# MAGIC > CREATE OR REPLACE TABLE heart_executive_summary AS
# MAGIC > SELECT
# MAGIC >   age_group, diagnosis, patient_count, avg_cholesterol,
# MAGIC >   ai_query(
# MAGIC >     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC >     CONCAT(
# MAGIC >       'Summarize this patient cohort in 1-2 sentences for a hospital executive: ',
# MAGIC >       age_group, ' patients, ', diagnosis, ', n=', patient_count,
# MAGIC >       ', avg cholesterol=', avg_cholesterol, ' mg/dL',
# MAGIC >       ', avg BP=', avg_blood_pressure, ' mmHg',
# MAGIC >       ', avg max HR=', avg_max_heart_rate, ' bpm'
# MAGIC >     )
# MAGIC >   ) AS executive_summary
# MAGIC > FROM heart_gold
# MAGIC > ```
# MAGIC
# MAGIC ### Bonus 2: Advanced Cohort Analysis (+2 pts)
# MAGIC
# MAGIC > Write a query that compares heart disease patients vs. healthy patients across ALL
# MAGIC > clinical metrics. For each metric (`trestbps`, `chol`, `thalach`, `oldpeak`),
# MAGIC > show the average for disease vs. healthy, and the percentage difference.
# MAGIC > Save as table `heart_cohort_comparison`.
# MAGIC >
# MAGIC > ```sql
# MAGIC > CREATE OR REPLACE TABLE heart_cohort_comparison AS
# MAGIC > SELECT
# MAGIC >   'trestbps' AS metric,
# MAGIC >   ROUND(AVG(CASE WHEN target=1 THEN trestbps END), 1) AS disease_avg,
# MAGIC >   ROUND(AVG(CASE WHEN target=0 THEN trestbps END), 1) AS healthy_avg,
# MAGIC >   ROUND((AVG(CASE WHEN target=1 THEN trestbps END) -
# MAGIC >          AVG(CASE WHEN target=0 THEN trestbps END)) * 100.0 /
# MAGIC >          AVG(CASE WHEN target=0 THEN trestbps END), 1) AS pct_diff
# MAGIC > FROM heart_silver
# MAGIC > UNION ALL
# MAGIC > -- ... repeat for chol, thalach, oldpeak
# MAGIC > ```
# MAGIC
# MAGIC ### Bonus 3: Statistical Test (+3 pts)
# MAGIC
# MAGIC > Run a Chi-squared test to determine if chest pain type is statistically associated
# MAGIC > with heart disease diagnosis. Use `scipy.stats.chi2_contingency`. Save the p-value
# MAGIC > as a variable called `chi2_pvalue`. A p-value < 0.05 means statistically significant.
# MAGIC >
# MAGIC > ```python
# MAGIC > from scipy.stats import chi2_contingency
# MAGIC > import pandas as pd
# MAGIC > ct = pd.crosstab(df['cp'], df['target'])
# MAGIC > chi2, chi2_pvalue, dof, expected = chi2_contingency(ct)
# MAGIC > print(f"Chi-squared: {chi2:.2f}, p-value: {chi2_pvalue:.6f}")
# MAGIC > ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column Reference (for SQL queries)
# MAGIC
# MAGIC ### heart_silver (patient-level, ~488 rows)
# MAGIC
# MAGIC | Column | Description |
# MAGIC |--------|------------|
# MAGIC | `age` | Patient age in years (1-120) |
# MAGIC | `sex` | 0 = female, 1 = male |
# MAGIC | `cp` | Chest pain type: 0=typical angina, 1=atypical, 2=non-anginal, 3=asymptomatic |
# MAGIC | `trestbps` | Resting blood pressure (mmHg, 50-300) |
# MAGIC | `chol` | Serum cholesterol (mg/dL) |
# MAGIC | `fbs` | Fasting blood sugar > 120 mg/dL: 1=true, 0=false |
# MAGIC | `restecg` | Resting ECG: 0=normal, 1=ST-T abnormality, 2=LV hypertrophy |
# MAGIC | `thalach` | Maximum heart rate achieved during exercise |
# MAGIC | `exang` | Exercise-induced angina: 1=yes, 0=no |
# MAGIC | `oldpeak` | ST depression induced by exercise |
# MAGIC | `slope` | Slope of peak exercise ST segment: 0=upsloping, 1=flat, 2=downsloping |
# MAGIC | `ca` | Number of major vessels colored by fluoroscopy (0-3) |
# MAGIC | `thal` | Thalassemia: 1=normal, 2=fixed defect, 3=reversible defect |
# MAGIC | `target` | **Diagnosis: 1 = heart disease, 0 = healthy** |
# MAGIC | `event_id` | Unique event identifier |
# MAGIC | `patient_id` | Patient identifier |
# MAGIC
# MAGIC ### heart_gold (aggregated, ~8 rows)
# MAGIC
# MAGIC | Column | Description |
# MAGIC |--------|------------|
# MAGIC | `age_group` | "Under 40", "40-49", "50-59", "60+" |
# MAGIC | `diagnosis` | "Heart Disease" or "Healthy" |
# MAGIC | `patient_count` | Patients in this cohort |
# MAGIC | `avg_cholesterol` | Average cholesterol (mg/dL) |
# MAGIC | `avg_blood_pressure` | Average resting BP (mmHg) |
# MAGIC | `avg_max_heart_rate` | Average max heart rate |
