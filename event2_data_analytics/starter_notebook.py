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
# MAGIC Work together as a team through both parts:
# MAGIC 1. **SQL Warm-Up (4 min):** Write 3 practice SQL queries below to get familiar with the data
# MAGIC 2. **Genie Setup (4 min):** Set up your Genie space using the **`genie_setup_guide`** notebook
# MAGIC
# MAGIC > **Tip:** The SQL warm-up helps you understand the data schema, which makes
# MAGIC > configuring your Genie space faster and more accurate!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 1: SQL Warm-Up
# MAGIC
# MAGIC Write 3 analytical queries against `heart_silver` to get comfortable with the data.
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
# MAGIC ## Part 2: Set Up Your Genie Space
# MAGIC
# MAGIC Now that you understand the data, set up your Genie space.
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
# MAGIC ## Submit Your Answers
# MAGIC
# MAGIC Use the `submit_answer()` function below to record your answers.
# MAGIC Each call saves to `{CATALOG}.default.event2_submissions` — the scoring
# MAGIC notebook reads this table automatically. **No manual reporting needed!**
# MAGIC
# MAGIC You can submit multiple times per question — only the latest answer counts.

# COMMAND ----------

from datetime import datetime as _dt

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.default.event2_submissions (
        team STRING,
        question_id STRING,
        answer STRING,
        method STRING,
        submitted_at TIMESTAMP
    )
""")

def submit_answer(question_id: str, answer: str, method: str = "SQL"):
    """Submit an answer for a benchmark question.

    Args:
        question_id: "Q1" through "Q8"
        answer: Your answer (number, text, etc.)
        method: "SQL" or "Genie"
    """
    qid = question_id.upper()
    method = method.upper()
    assert qid in [f"Q{i}" for i in range(1, 9)], f"Invalid question_id: {qid}. Use Q1-Q8."
    assert method in ("SQL", "GENIE"), f"Invalid method: {method}. Use 'SQL' or 'Genie'."

    ts = _dt.now().isoformat()
    spark.sql(f"""
        INSERT INTO {CATALOG}.default.event2_submissions
        VALUES ('{TEAM_NAME}', '{qid}', '{answer}', '{method}', '{ts}')
    """)
    print(f"  Submitted {qid} [{method}]: {answer}")

print("submit_answer() ready — usage:")
print('  submit_answer("Q1", "373", "SQL")')
print('  submit_answer("Q2", "55.4, 44.4", "Genie")')

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to submit during the race
# MAGIC
# MAGIC After each question, run a cell like this:
# MAGIC ```python
# MAGIC submit_answer("Q1", "373", "SQL")
# MAGIC ```
# MAGIC
# MAGIC Or if you used Genie:
# MAGIC ```python
# MAGIC submit_answer("Q1", "373", "Genie")
# MAGIC ```

# COMMAND ----------

# Submit your answers here — one per question:

# submit_answer("Q1", "", "SQL")
# submit_answer("Q2", "", "SQL")
# submit_answer("Q3", "", "SQL")
# submit_answer("Q4", "", "SQL")
# submit_answer("Q5", "", "SQL")
# submit_answer("Q6", "", "SQL")
# submit_answer("Q7", "", "SQL")
# submit_answer("Q8", "", "SQL")

# COMMAND ----------

# MAGIC %md
# MAGIC ### View Your Submissions

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT question_id, answer, method, submitted_at
        FROM {CATALOG}.default.event2_submissions
        WHERE team = '{TEAM_NAME}'
        ORDER BY question_id, submitted_at DESC
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick Summary

# COMMAND ----------

from pyspark.sql import functions as F

latest = spark.sql(f"""
    SELECT question_id, answer, method, submitted_at,
        ROW_NUMBER() OVER (PARTITION BY question_id ORDER BY submitted_at DESC) AS rn
    FROM {CATALOG}.default.event2_submissions
    WHERE team = '{TEAM_NAME}'
""").filter("rn = 1").drop("rn").orderBy("question_id")

print("=" * 60)
print(f"  YOUR LATEST ANSWERS — {TEAM_NAME}")
print("=" * 60)
for row in latest.collect():
    print(f"  {row['question_id']} [{row['method']:5s}]: {row['answer']}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Bonus Challenges (Extra Credit)
# MAGIC
# MAGIC ### Bonus 1: AI-Powered Summary (+3 pts)
# MAGIC
# MAGIC > Use `ai_query()` with a foundation model to generate a **natural language executive
# MAGIC > summary** of each row in your Gold table. The summary should be 1-2 sentences
# MAGIC > that a hospital executive would find useful (cohort size, key metrics, risk level).
# MAGIC >
# MAGIC > Save the result as a table called **`heart_executive_summary`** with all the original
# MAGIC > Gold columns plus a new `executive_summary` column.
# MAGIC >
# MAGIC > *Hint: Look up `ai_query()` in the Databricks SQL docs. You'll need to pass a
# MAGIC > model name and a prompt that includes the row's data.*
# MAGIC
# MAGIC ### Bonus 2: Advanced Cohort Analysis (+2 pts)
# MAGIC
# MAGIC > Write a query that compares heart disease patients vs. healthy patients across
# MAGIC > **all 4 key clinical metrics** (`trestbps`, `chol`, `thalach`, `oldpeak`).
# MAGIC > For each metric, show the average for disease, average for healthy, and the
# MAGIC > percentage difference between the two groups.
# MAGIC >
# MAGIC > Save as a table called **`heart_cohort_comparison`** with columns:
# MAGIC > `metric`, `disease_avg`, `healthy_avg`, `pct_diff`.
# MAGIC >
# MAGIC > *Hint: Use conditional aggregation with `CASE WHEN target=1 THEN ... END`
# MAGIC > and `UNION ALL` to stack metrics vertically.*
# MAGIC
# MAGIC ### Bonus 3: Statistical Test (+3 pts)
# MAGIC
# MAGIC > Run a **Chi-squared test** to determine if chest pain type (`cp`) is statistically
# MAGIC > associated with heart disease diagnosis (`target`). Save the results as a table
# MAGIC > called **`heart_chi2_test`** with columns: `chi2_statistic`, `p_value`, `degrees_of_freedom`,
# MAGIC > and `is_significant` (boolean, True if p < 0.05).
# MAGIC >
# MAGIC > *Hint: Use Python's `scipy.stats.chi2_contingency` on a cross-tabulation of
# MAGIC > `cp` vs `target`. You'll need to convert the Spark DataFrame to pandas first.*

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

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## SUBMIT YOUR WORK
# MAGIC
# MAGIC **Run this cell when you're done!** It records your submission timestamp for speed tracking.
# MAGIC The first team to submit each correct answer gets a **+1 speed bonus**.

# COMMAND ----------

from datetime import datetime as _dt

_event_name = "Event 2: Data Analytics"
_submit_ts = _dt.now()

spark.sql(f"""
    INSERT INTO dataops_olympics.default.event_submissions
    VALUES ('{TEAM_NAME}', '{_event_name}', '{_submit_ts}', NULL)
""")

print("=" * 60)
print(f"  SUBMITTED! {TEAM_NAME} — {_event_name}")
print(f"  Timestamp: {_submit_ts.strftime('%H:%M:%S.%f')}")
print(f"  Signal the judges that you are DONE!")
print("=" * 60)
