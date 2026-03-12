# Databricks notebook source
# MAGIC %md
# MAGIC # Warm-Up: Vibe Coding Sprint
# MAGIC
# MAGIC ## The Rules Are Simple
# MAGIC **Time: 10 minutes** | **Tools: Databricks Assistant ONLY**
# MAGIC
# MAGIC ### How It Works
# MAGIC 1. You will be given a plain-English task description below
# MAGIC 2. Open the **Databricks Assistant** panel (click the Assistant icon in the sidebar, or press `Cmd+I`)
# MAGIC 3. Use the Assistant to **generate all your code** — type prompts, not Python
# MAGIC 4. You may edit the Assistant's output (fix small issues, change variable names)
# MAGIC 5. But the goal is: **let the AI do the heavy lifting**
# MAGIC
# MAGIC ### Why This Matters
# MAGIC The rest of today's competition is calibrated for teams that use AI assistance.
# MAGIC If you try to write everything by hand, you'll run out of time.
# MAGIC This warm-up teaches you the workflow before points are on the line.
# MAGIC
# MAGIC ### Not Scored
# MAGIC This is practice. First team to finish gets bragging rights and a round of applause.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Your Mission
# MAGIC
# MAGIC **Build a complete analytics notebook using only the Databricks Assistant.**
# MAGIC
# MAGIC Your notebook must:
# MAGIC 1. **Read** the heart disease CSV file from `/Volumes/dataops_olympics/default/raw_data/heart_disease/heart.csv`
# MAGIC 2. **Create** a Delta table called `{your_team_name}_warmup`
# MAGIC 3. **Run 3 SQL queries** that reveal something interesting about the data
# MAGIC 4. **Create 1 Plotly visualization** (any chart type)
# MAGIC 5. **Print a summary** of what you found
# MAGIC
# MAGIC That's it. Go.

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to Use the Databricks Assistant
# MAGIC
# MAGIC ### Opening the Assistant
# MAGIC - Click the **Assistant icon** (sparkle) in the left sidebar
# MAGIC - Or press **Cmd+I** (Mac) / **Ctrl+I** (Windows)
# MAGIC - Or click into an empty cell and start typing a comment — the Assistant will offer to help
# MAGIC
# MAGIC ### Prompting Tips
# MAGIC
# MAGIC **Be specific.** Instead of "read a file", say:
# MAGIC > "Read the CSV file at `/Volumes/dataops_olympics/default/raw_data/heart_disease/heart.csv` with headers and infer schema, and display the first 10 rows"
# MAGIC
# MAGIC **Give context.** Instead of "write a query", say:
# MAGIC > "Write a SQL query on the table `team_01_warmup` that shows the average age and cholesterol grouped by heart disease status (target column, where 1 = disease)"
# MAGIC
# MAGIC **Iterate.** If the first output isn't perfect:
# MAGIC > "That query is close, but also add a COUNT(*) column and order by target"
# MAGIC
# MAGIC ### Agent Mode (Advanced)
# MAGIC
# MAGIC If Agent Skills are installed (see the Agent Skills Guide notebook), you can switch
# MAGIC the Assistant to **Agent mode** for even more powerful capabilities — it can create
# MAGIC dashboards, set up Genie spaces, and deploy pipelines through natural language.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start Here
# MAGIC
# MAGIC Use the cells below to build your solution. Each cell should be generated
# MAGIC by the Databricks Assistant.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Prompt Idea 1: Load the Data
# MAGIC
# MAGIC Try asking the Assistant:
# MAGIC > "Read the CSV file at `/Volumes/dataops_olympics/default/raw_data/heart_disease/heart.csv`
# MAGIC > with headers and inferSchema into a Spark DataFrame called df_heart,
# MAGIC > then save it as a Delta table called `dataops_olympics.default.team_XX_warmup` and display it"

# COMMAND ----------

# Ask the Assistant to generate this cell!


# COMMAND ----------

# MAGIC %md
# MAGIC ### Prompt Idea 2: Run Analytical Queries
# MAGIC
# MAGIC Try asking the Assistant:
# MAGIC > "Write 3 SQL queries on the `team_XX_warmup` table:
# MAGIC > 1. Heart disease rate (% of patients where target=1)
# MAGIC > 2. Average age and cholesterol by heart disease status
# MAGIC > 3. Count of patients by chest pain type (cp column)"

# COMMAND ----------

# Ask the Assistant to generate this cell!


# COMMAND ----------

# Ask the Assistant to generate this cell!


# COMMAND ----------

# Ask the Assistant to generate this cell!


# COMMAND ----------

# MAGIC %md
# MAGIC ### Prompt Idea 3: Create a Visualization
# MAGIC
# MAGIC Try asking the Assistant:
# MAGIC > "Create a Plotly bar chart showing the number of patients in each age group
# MAGIC > (Under 40, 40-49, 50-59, 60+), colored by heart disease status (target).
# MAGIC > Use the `team_XX_warmup` table."

# COMMAND ----------

# Ask the Assistant to generate this cell!


# COMMAND ----------

# MAGIC %md
# MAGIC ### Prompt Idea 4: Summarize Your Findings
# MAGIC
# MAGIC Try asking the Assistant:
# MAGIC > "Write a Python print statement summarizing the key findings from the heart
# MAGIC > disease dataset — include the disease rate, average age difference between
# MAGIC > disease and healthy patients, and which chest pain type is most common."

# COMMAND ----------

# Ask the Assistant to generate this cell!


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Done?
# MAGIC
# MAGIC **Raise your hand!** The organizer will note your completion time.
# MAGIC
# MAGIC ### Reflection
# MAGIC - How many prompts did it take?
# MAGIC - Did you need to edit the Assistant's output?
# MAGIC - What worked well? What was frustrating?
# MAGIC
# MAGIC These insights will help you in the competitive events ahead.
