# Databricks notebook source
# MAGIC %md
# MAGIC # Event 4: GenAI/Agents — Automated Scoring
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY**
# MAGIC
# MAGIC ### Scoring Breakdown (40 pts + 8 bonus)
# MAGIC
# MAGIC | Category | Points |
# MAGIC |----------|--------|
# MAGIC | Data Exploration | 3 |
# MAGIC | System Prompt (200+ chars) | 5 |
# MAGIC | Agent Function (routing + SQL) | 12 |
# MAGIC | AI Functions (ai_query tables) | 10 |
# MAGIC | Test Prompt Evaluation | 10 |
# MAGIC | **Bonus: Semantic Search** | +3 |
# MAGIC | **Bonus: Safety Guardrails** | +2 |
# MAGIC | **Bonus: Multi-Step** | +3 |

# COMMAND ----------

TEAMS = ["team_01", "team_02", "team_03", "team_04"]
SCHEMA = "default"
SHARED_CATALOG = "dataops_olympics"

# COMMAND ----------

import pandas as pd


def _fqn(catalog, table):
    return f"{catalog}.{SCHEMA}.{table}"


def _table_exists(catalog, table):
    try:
        spark.table(_fqn(catalog, table))
        return True
    except Exception:
        return False


def _table_count(catalog, table):
    try:
        return spark.table(_fqn(catalog, table)).count()
    except Exception:
        return 0


def _has_column(catalog, table, col_name):
    try:
        cols = [c.lower() for c in spark.table(_fqn(catalog, table)).columns]
        return col_name.lower() in cols
    except Exception:
        return False


def score_team(team_name: str) -> dict:
    catalog = team_name
    scores = {
        "team": team_name, "exploration": 0, "system_prompt": 0,
        "agent_function": 0, "ai_functions": 0, "test_prompts": 0,
        "bonus": 0, "total": 0, "details": [],
    }

    def log(msg):
        scores["details"].append(msg)

    # ─── Data Exploration (3 pts) ───
    tables_accessible = 0
    for tbl, cat in [("heart_silver", catalog), ("heart_gold", catalog),
                     ("drug_reviews", SHARED_CATALOG), ("clinical_notes", SHARED_CATALOG)]:
        if _table_exists(cat, tbl):
            tables_accessible += 1

    if tables_accessible >= 4:
        scores["exploration"] = 3
        log(f"Exploration: all 4 tables accessible [+3]")
    elif tables_accessible >= 2:
        scores["exploration"] = 2
        log(f"Exploration: {tables_accessible}/4 tables accessible [+2]")
    elif tables_accessible >= 1:
        scores["exploration"] = 1
        log(f"Exploration: {tables_accessible}/4 tables accessible [+1]")
    else:
        log("Exploration: no tables accessible [+0]")

    # ─── System Prompt (5 pts) ───
    # Organizer checks notebook manually; auto-score based on artifacts
    # For automated scoring, give full points if agent tables exist (implies they ran the notebook)
    has_ai_tables = _table_exists(catalog, "heart_ai_insights") or _table_exists(catalog, "drug_ai_summary")
    if has_ai_tables:
        scores["system_prompt"] = 5
        log("System Prompt: agent artifacts found, likely well-defined [+5]")
    elif _table_exists(catalog, "heart_gold"):
        scores["system_prompt"] = 2
        log("System Prompt: basic setup detected [+2]")
    else:
        log("System Prompt: no evidence of agent setup [+0]")

    # ─── Agent Function (12 pts) ───
    # Score based on evidence of routing logic: check for multiple query results
    agent_score = 0

    # Heart routing: check if heart_silver is queried (has data)
    if _table_exists(catalog, "heart_silver") and _table_count(catalog, "heart_silver") > 0:
        agent_score += 4
        log("Agent: heart data routing works [+4]")

    # Drug routing: check for drug_ai_summary (evidence of drug queries)
    if _table_exists(catalog, "drug_ai_summary"):
        agent_score += 4
        log("Agent: drug data routing works [+4]")
    elif _table_exists(SHARED_CATALOG, "drug_reviews"):
        agent_score += 2
        log("Agent: drug_reviews accessible but no summary table [+2]")

    # Notes routing
    if _table_exists(SHARED_CATALOG, "clinical_notes"):
        agent_score += 2
        log("Agent: clinical_notes accessible [+2]")

    # Multi-table evidence
    if agent_score >= 8:
        agent_score = min(agent_score + 2, 12)
        log("Agent: multi-table routing demonstrated [+2]")

    scores["agent_function"] = agent_score

    # ─── AI Functions (10 pts) ───
    ai_score = 0

    # heart_ai_insights
    if _table_exists(catalog, "heart_ai_insights"):
        cnt = _table_count(catalog, "heart_ai_insights")
        has_insight = _has_column(catalog, "heart_ai_insights", "clinical_insight")
        if has_insight and cnt > 0:
            ai_score += 5
            log(f"AI Functions: heart_ai_insights with clinical_insight, {cnt} rows [+5]")
        elif cnt > 0:
            ai_score += 3
            log(f"AI Functions: heart_ai_insights exists, {cnt} rows, missing insight col [+3]")
        else:
            log("AI Functions: heart_ai_insights empty [+0]")
    else:
        log("AI Functions: heart_ai_insights not found [+0]")

    # drug_ai_summary
    if _table_exists(catalog, "drug_ai_summary"):
        cnt = _table_count(catalog, "drug_ai_summary")
        has_summary = _has_column(catalog, "drug_ai_summary", "ai_summary")
        if has_summary and cnt > 0:
            ai_score += 5
            log(f"AI Functions: drug_ai_summary with ai_summary, {cnt} rows [+5]")
        elif cnt > 0:
            ai_score += 3
            log(f"AI Functions: drug_ai_summary exists, {cnt} rows, missing summary col [+3]")
        else:
            log("AI Functions: drug_ai_summary empty [+0]")
    else:
        log("AI Functions: drug_ai_summary not found [+0]")

    scores["ai_functions"] = ai_score

    # ─── Test Prompts (10 pts) ───
    # Automated: give points based on overall completeness (agent tables + data quality)
    prompt_score = 0
    if ai_score >= 8:
        prompt_score = 10
        log("Test Prompts: comprehensive agent with AI [+10]")
    elif ai_score >= 5:
        prompt_score = 7
        log("Test Prompts: partial agent implementation [+7]")
    elif agent_score >= 6:
        prompt_score = 5
        log("Test Prompts: basic agent routing [+5]")
    elif agent_score > 0:
        prompt_score = 2
        log("Test Prompts: minimal agent [+2]")
    else:
        log("Test Prompts: no agent evidence [+0]")

    scores["test_prompts"] = prompt_score

    # ─── Bonus (8 pts max) ───
    # Semantic Search (+3): check for chromadb usage or search_notes function
    if _table_exists(catalog, "agent_audit_log"):
        scores["bonus"] += 2
        log("Bonus: audit log table found (safety guardrails) [+2]")

    # Multi-step (+3): check for cross-table results
    if _table_exists(catalog, "heart_ai_insights") and _table_exists(catalog, "drug_ai_summary"):
        if _table_count(catalog, "heart_ai_insights") > 0 and _table_count(catalog, "drug_ai_summary") > 0:
            scores["bonus"] += 3
            log("Bonus: multi-source AI analysis (heart + drugs) [+3]")

    scores["total"] = (
        scores["exploration"] + scores["system_prompt"] + scores["agent_function"]
        + scores["ai_functions"] + scores["test_prompts"] + scores["bonus"]
    )
    return scores

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Scoring

# COMMAND ----------

results = []
for team in TEAMS:
    print(f"\n{'='*60}")
    print(f"  SCORING: {team}")
    print(f"{'='*60}")
    r = score_team(team)
    results.append(r)
    print(f"  Explore:{r['exploration']}/3  Prompt:{r['system_prompt']}/5  Agent:{r['agent_function']}/12  AI:{r['ai_functions']}/10  Tests:{r['test_prompts']}/10  Bonus:{r['bonus']}/8")
    print(f"  TOTAL: {r['total']}/48")
    for d in r["details"]:
        print(f"    {d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leaderboard

# COMMAND ----------

import plotly.graph_objects as go

df_scores = pd.DataFrame([{
    "Team": r["team"],
    "Exploration": r["exploration"],
    "SystemPrompt": r["system_prompt"],
    "AgentFunction": r["agent_function"],
    "AIFunctions": r["ai_functions"],
    "TestPrompts": r["test_prompts"],
    "Bonus": r["bonus"],
    "Total": r["total"],
} for r in results]).sort_values("Total", ascending=False)

categories = ["Exploration", "SystemPrompt", "AgentFunction", "AIFunctions", "TestPrompts", "Bonus"]
cat_colors = {
    "Exploration": "#3498db", "SystemPrompt": "#e67e22", "AgentFunction": "#9b59b6",
    "AIFunctions": "#2ecc71", "TestPrompts": "#1abc9c", "Bonus": "#e74c3c",
}

ordered = df_scores.sort_values("Total", ascending=True)
fig = go.Figure()
for cat in categories:
    fig.add_trace(go.Bar(
        y=ordered["Team"],
        x=ordered[cat],
        name=cat.replace("_", " "),
        orientation="h",
        marker=dict(color=cat_colors.get(cat, "#636e72"), line=dict(color="#1a1a2e", width=1)),
        text=ordered[cat].astype(int),
        textposition="inside",
        textfont=dict(size=12, color="white"),
    ))

fig.update_layout(
    barmode="stack",
    title=dict(text="Event 4: GenAI/Agents — Scores", font=dict(size=24, color="white"), x=0.5),
    plot_bgcolor="#1a1a2e", paper_bgcolor="#16213e",
    font=dict(color="white", size=13),
    xaxis=dict(title="Points", gridcolor="#2d3436", range=[0, 50]),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
    height=max(350, len(df_scores) * 80 + 120),
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

spark.createDataFrame(df_scores).write.format("delta").mode("overwrite").saveAsTable(
    "dataops_olympics.default.event4_scores"
)
print("Saved to dataops_olympics.default.event4_scores")
