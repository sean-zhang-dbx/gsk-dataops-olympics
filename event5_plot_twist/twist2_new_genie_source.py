# Databricks notebook source
# MAGIC %md
# MAGIC # Plot Twist 2: New Genie Data Source — Expand the Room!
# MAGIC
# MAGIC ## The Scenario
# MAGIC Users are asking your Genie space questions it can't answer.
# MAGIC The questions are about **drug effectiveness and side effects** — data that
# MAGIC wasn't in your original Genie room.
# MAGIC
# MAGIC **You have 10 minutes to add a new data source and make Genie smarter.**
# MAGIC
# MAGIC ## The New Questions Users Are Asking
# MAGIC 1. "What is the highest rated drug for diabetes?"
# MAGIC 2. "How many drug reviews mention side effects?"
# MAGIC 3. "Which drug has the most reviews?"
# MAGIC 4. "What conditions have the most drug reviews?"
# MAGIC 5. "Compare ratings of Metformin vs Lisinopril"
# MAGIC
# MAGIC ## Your Task
# MAGIC 1. Add the `drug_reviews` table to your Genie space
# MAGIC 2. Add appropriate instructions so Genie understands the data
# MAGIC 3. Verify Genie can now answer the benchmark questions above
# MAGIC 4. Optionally: Create a joined view connecting drugs to patients

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Explore the Drug Reviews Data

# COMMAND ----------

display(spark.sql("SELECT * FROM drug_reviews LIMIT 10"))

# COMMAND ----------

# Key stats about the data
spark.sql("""
    SELECT
        COUNT(*) as total_reviews,
        COUNT(DISTINCT drug_name) as unique_drugs,
        COUNT(DISTINCT condition) as unique_conditions,
        AVG(rating) as avg_rating,
        MIN(date) as earliest_review,
        MAX(date) as latest_review
    FROM drug_reviews
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Add to Your Genie Space
# MAGIC
# MAGIC **TODO:** In the Databricks UI:
# MAGIC 1. Open your Genie space from Event 2
# MAGIC 2. Click **Settings** (gear icon)
# MAGIC 3. Add `drug_reviews` as a data source
# MAGIC 4. Add these instructions:
# MAGIC
# MAGIC ```
# MAGIC The drug_reviews table contains patient reviews of medications.
# MAGIC - drug_name: Name of the medication
# MAGIC - condition: Medical condition being treated
# MAGIC - review: Patient's written review text
# MAGIC - rating: 1-10 scale (10 is best)
# MAGIC - date: Date of the review (YYYY-MM-DD)
# MAGIC - useful_count: How many people found this review helpful
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create a Joined View (Bonus)
# MAGIC
# MAGIC **TODO:** Create a view that connects drug reviews to patient conditions,
# MAGIC making Genie even smarter for cross-dataset questions.

# COMMAND ----------

TEAM_NAME = "_____"

# TODO: Create a useful joined view
# Example: Link drug conditions to heart disease risk factors

spark.sql(f"""
    CREATE OR REPLACE VIEW {TEAM_NAME}_drug_condition_summary AS
    SELECT
        drug_name,
        condition,
        COUNT(*) as review_count,
        ROUND(AVG(rating), 1) as avg_rating,
        ROUND(AVG(useful_count), 0) as avg_useful_count
    FROM drug_reviews
    GROUP BY drug_name, condition
    ORDER BY review_count DESC
""")

display(spark.table(f"{TEAM_NAME}_drug_condition_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Test the Benchmark Questions
# MAGIC
# MAGIC Verify your Genie space can answer these questions.
# MAGIC Record your answers:

# COMMAND ----------

answers = {
    "What is the highest rated drug for diabetes?": "_____",
    "How many drug reviews are there in total?": "_____",
    "Which drug has the most reviews?": "_____",
    "What conditions have the most drug reviews?": "_____",
    "Compare ratings of Metformin vs Lisinopril": "_____",
}

print("=" * 60)
print(f"  GENIE EXPANSION — {TEAM_NAME}")
print("=" * 60)
for q, a in answers.items():
    print(f"\n  Q: {q}")
    print(f"  A: {a}")
print("\n" + "=" * 60)
