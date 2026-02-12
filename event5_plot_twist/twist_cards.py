# Databricks notebook source
# MAGIC %md
# MAGIC # 🎭 Plot Twist Cards
# MAGIC
# MAGIC ## Instructions for Judges
# MAGIC 1. Run the cell below to draw a random twist card
# MAGIC 2. Announce the twist to the team
# MAGIC 3. Teams have 20 minutes to adapt their base solution
# MAGIC 4. Judge adaptation speed, quality, and presentation
# MAGIC
# MAGIC **OR** use the pre-defined cards below and let teams draw physically.

# COMMAND ----------

import random

twist_cards = [
    {
        "id": 1,
        "title": "🔄 Format Shift",
        "challenge": "Your data source just changed format — adapt!",
        "details": """
Your CSV data source has been replaced with a JSON API response format.
You must:
1. Read the data from JSON format instead: /tmp/dataops_olympics/raw/life_expectancy/life_expectancy_sample.json
2. Map the new fields to your existing pipeline
3. Ensure your model still works with the new data schema
4. Show that predictions are still valid

Success criteria: Pipeline works end-to-end with JSON input, F1 > 0.55
        """,
        "difficulty": "Medium"
    },
    {
        "id": 2,
        "title": "📈 Scale 100x",
        "challenge": "CTO wants to see this scaled 100x — how?",
        "details": """
The CTO is impressed and wants to scale your solution to handle 100x more data.
You must:
1. Generate 100x synthetic data (replicate existing data with noise)
2. Switch from pandas to Spark ML for training
3. Show that your pipeline handles the larger dataset
4. Document your scaling strategy and any bottlenecks

Success criteria: Pipeline processes 100x data, training completes, scaling plan documented
        """,
        "difficulty": "Hard"
    },
    {
        "id": 3,
        "title": "🔒 PII Alert",
        "challenge": "Compliance just flagged PII in your data — fix it!",
        "details": """
The compliance team found that your data contains PII (Personally Identifiable Information).
You must:
1. Identify which columns could be considered PII/sensitive
2. Implement data masking/anonymization (hash, generalize, or remove)
3. Re-train your model on anonymized data
4. Show that model performance is maintained (F1 within 10% of original)
5. Add column-level access tags/comments for governance

Success criteria: PII masked, model retrained, F1 within 10% of original
        """,
        "difficulty": "Medium"
    },
    {
        "id": 4,
        "title": "💰 Budget Cut",
        "challenge": "CFO cut your compute budget 50% — optimize!",
        "details": """
Your compute budget has been cut in half. You must optimize your pipeline.
You must:
1. Profile your current pipeline (measure time for each step)
2. Reduce feature count to top-5 most important features
3. Use a simpler/faster model (e.g., Logistic Regression)
4. Implement caching where possible
5. Show total execution time reduction
6. Maintain F1 > 0.50

Success criteria: Demonstrate faster execution, F1 > 0.50, optimization documented
        """,
        "difficulty": "Medium"
    },
    {
        "id": 5,
        "title": "🌍 Multi-Region",
        "challenge": "We're expanding globally — support multiple datasets!",
        "details": """
Your pipeline needs to support multiple regional datasets simultaneously.
You must:
1. Load BOTH heart_disease AND life_expectancy datasets
2. Create a unified schema that works for both
3. Train separate models for each dataset
4. Create a comparison dashboard showing performance across datasets
5. Implement a model registry pattern using MLflow

Success criteria: Both datasets processed, both models trained, comparison shown
        """,
        "difficulty": "Hard"
    },
    {
        "id": 6,
        "title": "⚡ Real-Time Pivot",
        "challenge": "Product wants real-time predictions — add streaming!",
        "details": """
The product team wants predictions served in real-time.
You must:
1. Create a prediction function that takes a single patient record
2. Build a simple batch inference pipeline using the trained model
3. Create an interactive input form (using widgets or parameters)
4. Show predictions for 5 sample patients with explanations
5. Document how this would scale to a real-time endpoint

Success criteria: Interactive prediction function works, 5 demos shown, scaling plan
        """,
        "difficulty": "Medium"
    },
    {
        "id": 7,
        "title": "📊 Explain Everything",
        "challenge": "Board of directors wants explainability — add it now!",
        "details": """
The board needs to understand every prediction. Pure accuracy isn't enough.
You must:
1. Add SHAP or feature importance explanations for every prediction
2. Create a "model card" with performance metrics by subgroup
3. Identify any bias in the model (e.g., by sex, age group)
4. Build a fairness dashboard
5. Write a non-technical summary for the board

Success criteria: Explanations for all predictions, bias analysis, board summary
        """,
        "difficulty": "Hard"
    },
    {
        "id": 8,
        "title": "🔁 Version Control",
        "challenge": "Auditors need full lineage — add data versioning!",
        "details": """
Auditors require complete data lineage and version control.
You must:
1. Use Delta Lake time travel to show data history
2. Create versioned feature tables (v1, v2)
3. Log all transformations as MLflow parameters
4. Show how to reproduce any past prediction
5. Create an audit trail table

Success criteria: Delta time travel demo, versioned tables, audit trail created
        """,
        "difficulty": "Medium"
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Draw a Random Twist Card

# COMMAND ----------

# Draw a random card
card = random.choice(twist_cards)

print("=" * 60)
print(f"  🎭 PLOT TWIST CARD #{card['id']}")
print("=" * 60)
print(f"\n  {card['title']}")
print(f"\n  \"{card['challenge']}\"")
print(f"\n  Difficulty: {card['difficulty']}")
print(f"\n  Details:")
print(card['details'])
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## All Twist Cards (for reference)

# COMMAND ----------

# Print all cards for judges to review
for card in twist_cards:
    print("=" * 60)
    print(f"Card #{card['id']}: {card['title']}")
    print(f"  Challenge: {card['challenge']}")
    print(f"  Difficulty: {card['difficulty']}")
    print(f"  Details: {card['details'].strip()[:150]}...")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Printable Twist Cards
# MAGIC
# MAGIC Run this to generate formatted cards you can print and cut out.

# COMMAND ----------

for card in twist_cards:
    print("┌" + "─" * 58 + "┐")
    print(f"│  PLOT TWIST #{card['id']:<46}│")
    print(f"│  {card['title']:<56}│")
    print("├" + "─" * 58 + "┤")
    print(f"│  {card['challenge']:<56}│")
    print(f"│  Difficulty: {card['difficulty']:<44}│")
    print("└" + "─" * 58 + "┘")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring Rubric for Judges
# MAGIC
# MAGIC | Criteria | Points | Description |
# MAGIC |----------|--------|-------------|
# MAGIC | Adaptation Speed | 0-10 | How quickly did they understand and start adapting? |
# MAGIC | Solution Quality | 0-10 | Does the adapted solution work correctly? Meet success criteria? |
# MAGIC | Presentation | 0-5 | Clear explanation of approach, trade-offs, and reasoning |
# MAGIC
# MAGIC ### Rating Guide
# MAGIC - **9-10**: Exceptional — exceeded expectations, creative solution
# MAGIC - **7-8**: Strong — met all criteria efficiently
# MAGIC - **5-6**: Adequate — met most criteria with some issues
# MAGIC - **3-4**: Below average — significant gaps
# MAGIC - **1-2**: Minimal — barely attempted
