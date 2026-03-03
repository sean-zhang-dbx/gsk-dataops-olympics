# Databricks notebook source
# MAGIC %md
# MAGIC # Agent Skills Guide — Supercharge the Databricks Assistant
# MAGIC
# MAGIC This guide covers two things:
# MAGIC 1. **Part A**: How to use the Databricks Assistant effectively (hands-on exercises)
# MAGIC 2. **Part B**: How to install **Agent Skills** that make the Assistant dramatically more capable
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Why This Matters
# MAGIC
# MAGIC The Databricks Assistant is not just autocomplete. In **Agent mode**, it can:
# MAGIC - Generate entire data pipelines from a description
# MAGIC - Create and deploy AI/BI dashboards
# MAGIC - Set up Genie spaces with proper instructions
# MAGIC - Build and evaluate AI agents
# MAGIC - Write and run DLT/Declarative Pipelines
# MAGIC
# MAGIC But it needs **skills** — structured knowledge files that teach it how to use
# MAGIC specific Databricks features. That's what we'll install.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part A: Databricks Assistant Basics
# MAGIC
# MAGIC ## How to Open the Assistant
# MAGIC
# MAGIC | Method | Action |
# MAGIC |--------|--------|
# MAGIC | **Sidebar** | Click the sparkle icon in the left sidebar |
# MAGIC | **Keyboard** | `Cmd+I` (Mac) or `Ctrl+I` (Windows) |
# MAGIC | **Inline** | Start typing a comment in a cell — Assistant offers to help |
# MAGIC | **Cell menu** | Click the "Generate" button that appears in empty cells |
# MAGIC
# MAGIC ### Standard Mode vs Agent Mode
# MAGIC
# MAGIC | Feature | Standard Mode | Agent Mode |
# MAGIC |---------|--------------|------------|
# MAGIC | Code generation | Yes | Yes |
# MAGIC | Code explanation | Yes | Yes |
# MAGIC | Multi-step tasks | No | **Yes** — can plan and execute sequences |
# MAGIC | Create dashboards | No | **Yes** (with skills installed) |
# MAGIC | Set up Genie spaces | No | **Yes** (with skills installed) |
# MAGIC | Deploy pipelines | No | **Yes** (with skills installed) |
# MAGIC
# MAGIC To switch to Agent mode: click the dropdown in the Assistant panel header.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Code Generation
# MAGIC
# MAGIC Open the Assistant and try this prompt:
# MAGIC
# MAGIC > "Write a PySpark function that reads a CSV file, removes rows with null values
# MAGIC > in any column, adds an `ingested_at` timestamp column, and saves it as a Delta table.
# MAGIC > The function should take `file_path`, `table_name` as parameters."
# MAGIC
# MAGIC Paste the result in the cell below and run it.

# COMMAND ----------

# Paste the Assistant's generated code here and run it


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Code Explanation
# MAGIC
# MAGIC Copy the code below and ask the Assistant:
# MAGIC > "Explain what this code does, step by step"
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC w = Window.partitionBy("country").orderBy("year")
# MAGIC df_enriched = (
# MAGIC     df.withColumn("prev_life_exp", F.lag("life_expectancy").over(w))
# MAGIC       .withColumn("yoy_change", F.col("life_expectancy") - F.col("prev_life_exp"))
# MAGIC       .withColumn("rank", F.dense_rank().over(
# MAGIC           Window.partitionBy("year").orderBy(F.desc("life_expectancy"))
# MAGIC       ))
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC Did the explanation make sense? Did it catch everything?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Debugging
# MAGIC
# MAGIC Run the broken cell below. It will fail. Then ask the Assistant:
# MAGIC > "Fix this error"
# MAGIC
# MAGIC The Assistant should see the error and suggest a fix.

# COMMAND ----------

# This cell has bugs — run it, then ask the Assistant to fix it!
from pyspark.sql import function as F  # wrong import name

df_test = spark.read.format("csv").option("headers", "true").load("file:/tmp/dataops_olympics/raw/heart_disease/heart.csv")  # wrong option name

df_test = df_test.withColumn("age_group"
    F.when(F.col("age") < 40, "Under 40")
     .when(F.col("age") < 50, "40-49")
     .otherwise("50+")
)  # missing comma after first arg

display(df_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: SQL from English
# MAGIC
# MAGIC Ask the Assistant:
# MAGIC > "Write a SQL query on the `heart_disease` table that finds the top 3 chest pain
# MAGIC > types (cp column) with the highest heart disease rate (target=1). Include the total
# MAGIC > patient count and disease percentage for each type. Map cp values to their names:
# MAGIC > 0=Typical Angina, 1=Atypical Angina, 2=Non-Anginal Pain, 3=Asymptomatic."

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Paste the Assistant's SQL here


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Visualization
# MAGIC
# MAGIC Ask the Assistant:
# MAGIC > "Create a Plotly scatter plot from the `heart_disease` table showing age (x-axis)
# MAGIC > vs max heart rate thalach (y-axis), colored by target (0=Healthy, 1=Disease).
# MAGIC > Add a title and use a clean template."

# COMMAND ----------

# Paste the Assistant's visualization code here


# COMMAND ----------

# MAGIC %md
# MAGIC ## Pro Tips for Better Results
# MAGIC
# MAGIC | Tip | Example |
# MAGIC |-----|---------|
# MAGIC | **Be specific about table/column names** | "Query the `heart_disease` table where `target = 1`" |
# MAGIC | **State the output format** | "Return as a Plotly bar chart" or "Save as Delta table" |
# MAGIC | **Give context about your data** | "The `cp` column is chest pain type: 0=typical, 1=atypical..." |
# MAGIC | **Iterate, don't restart** | "That's close, but also add a filter for age > 50" |
# MAGIC | **Ask for alternatives** | "Show me 3 different ways to do this" |
# MAGIC | **Request explanations with code** | "Write the query and explain each part" |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part B: Installing Agent Skills (AI Dev Kit)
# MAGIC
# MAGIC ## What Are Agent Skills?
# MAGIC
# MAGIC Agent Skills are **SKILL.md files** from the
# MAGIC [databricks-solutions/ai-dev-kit](https://github.com/databricks-solutions/ai-dev-kit/tree/main/databricks-skills)
# MAGIC repository. They teach the Databricks Assistant's **Agent mode** how to work with
# MAGIC specific features — complete with patterns, API details, and best practices.
# MAGIC
# MAGIC ### Available Skills (Most Relevant for Today)
# MAGIC
# MAGIC | Skill | What It Teaches the Assistant | Event |
# MAGIC |-------|------------------------------|-------|
# MAGIC | `databricks-spark-declarative-pipelines` | Build DLT/SDP pipelines with expectations | Event 1 |
# MAGIC | `databricks-unity-catalog` | Governance, table comments, system tables | Event 1 |
# MAGIC | `databricks-aibi-dashboards` | Create AI/BI dashboards programmatically | Event 2 |
# MAGIC | `databricks-genie` | Set up and configure Genie spaces | Event 2, 4 |
# MAGIC | `databricks-agent-bricks` | Build Knowledge Assistants, Supervisor Agents | Event 4 |
# MAGIC | `databricks-model-serving` | Deploy models to endpoints | Event 3 |
# MAGIC | `databricks-vector-search` | RAG and semantic search | Event 4 |
# MAGIC | `databricks-dbsql` | SQL warehouse queries and patterns | All |
# MAGIC
# MAGIC ### Full list: 25+ skills covering dashboards, Genie, DLT, agents, apps, and more.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option A: Pre-Installed by Organizer (Recommended)
# MAGIC
# MAGIC If the organizer ran the install script during setup, skills are already deployed
# MAGIC to your workspace. Check:

# COMMAND ----------

# Check if skills are already installed
import os

try:
    user_email = spark.sql("SELECT current_user()").collect()[0][0]
    skills_path = f"/Workspace/Users/{user_email}/.assistant/skills"
    
    files = dbutils.fs.ls(skills_path)
    skill_count = len([f for f in files if f.isDir()])
    print(f"Agent Skills found at: {skills_path}")
    print(f"Skills installed: {skill_count}")
    for f in files:
        if f.isDir():
            print(f"  - {f.name}")
    print("\n>> Skills are ready! Switch the Assistant to Agent mode to use them.")
except Exception as e:
    print(f"No pre-installed skills found.")
    print(f"Use Option B or C below to install them yourself.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option B: Install via Script
# MAGIC
# MAGIC Run these commands in a **terminal** or **notebook cell** to install the skills:
# MAGIC
# MAGIC ### Step 1: Install skills locally
# MAGIC ```bash
# MAGIC %sh
# MAGIC curl -sSL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/databricks-skills/install_skills.sh | bash
# MAGIC ```
# MAGIC
# MAGIC ### Step 2: Deploy to the Databricks Assistant
# MAGIC ```bash
# MAGIC %sh
# MAGIC curl -sSL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/databricks-skills/install_to_dbx_assistant.sh | bash
# MAGIC ```
# MAGIC
# MAGIC After installation, **reload the page** and switch the Assistant to **Agent mode**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option C: Add ai-dev-kit as a Git Folder
# MAGIC
# MAGIC If you want to browse all skills:
# MAGIC
# MAGIC 1. Go to **Workspace > Repos > Add Repo**
# MAGIC 2. Enter: `https://github.com/databricks-solutions/ai-dev-kit`
# MAGIC 3. Click **Create Repo**
# MAGIC 4. Navigate to `databricks-skills/` to see all skill folders
# MAGIC
# MAGIC **Note:** This clones the entire ai-dev-kit repo (Git Folders cannot target a subfolder).
# MAGIC The skills are in the `databricks-skills/` directory.
# MAGIC
# MAGIC To install a specific skill manually:
# MAGIC 1. Open the skill folder (e.g., `databricks-skills/databricks-aibi-dashboards/`)
# MAGIC 2. Read the `SKILL.md` to understand what it teaches
# MAGIC 3. Copy the folder to `/Users/{your_email}/.assistant/skills/` in your workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing Agent Mode with Skills
# MAGIC
# MAGIC Once skills are installed, switch to **Agent mode** and try these prompts:
# MAGIC
# MAGIC ### Test 1: Dashboard Creation
# MAGIC > "Create an AI/BI dashboard for the `heart_disease` table showing disease rate
# MAGIC > by age group, a cholesterol distribution, and a patient count KPI"
# MAGIC
# MAGIC ### Test 2: Genie Space Setup
# MAGIC > "Set up a Genie space called 'Heart Disease Analyst' with the heart_disease table.
# MAGIC > Add instructions explaining that target=1 means disease and the cp column values."
# MAGIC
# MAGIC ### Test 3: Pipeline Definition
# MAGIC > "Create a Spark Declarative Pipeline that reads heart_disease.csv into a Bronze table,
# MAGIC > cleans it in Silver with expectations for valid age and blood pressure,
# MAGIC > and aggregates by age group in Gold."
# MAGIC
# MAGIC If the Agent has the skills installed, it will follow the correct patterns
# MAGIC and API calls for each Databricks feature.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Quick Reference: What to Ask the Assistant During Each Event
# MAGIC
# MAGIC | Event | Great Prompts for the Assistant |
# MAGIC |-------|-------------------------------|
# MAGIC | **1: Data Engineering** | "Create a Delta table from this CSV", "Add governance comments", "Build a Silver table with data quality filters" |
# MAGIC | **2: Data Analytics** | "Write SQL to analyze heart disease rates", "Create a Plotly chart", "Set up a Genie space" |
# MAGIC | **3: Data Science / ML** | "Suggest feature engineering ideas", "Train a model and log to MLflow", "Show SHAP feature importance" |
# MAGIC | **4: GenAI / Agents** | "Write a system prompt for a clinical agent", "Create a function that routes questions to SQL", "Add error handling" |
# MAGIC | **5: Capstone** | "Build a complete pipeline from this CSV", "Create a dashboard", "Train and register an ML model" |
# MAGIC
# MAGIC **Remember**: The more context you give the Assistant, the better the output.
# MAGIC Include table names, column descriptions, and what you want the output to look like.
