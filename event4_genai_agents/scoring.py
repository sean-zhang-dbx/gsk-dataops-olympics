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
# MAGIC | **Bonus: Genie Integration** | +3 |
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
    # Score based on evidence of agent artifacts (not just pre-existing tables)
    agent_score = 0

    # Heart routing: need AI insights table as evidence the agent actually queried heart data
    if _table_exists(catalog, "heart_ai_insights") and _table_count(catalog, "heart_ai_insights") > 0:
        agent_score += 4
        log("Agent: heart data routing demonstrated (heart_ai_insights) [+4]")
    elif has_ai_tables:
        agent_score += 2
        log("Agent: some AI tables exist, partial heart routing [+2]")

    # Drug routing: need drug_ai_summary as evidence
    if _table_exists(catalog, "drug_ai_summary") and _table_count(catalog, "drug_ai_summary") > 0:
        agent_score += 4
        log("Agent: drug data routing demonstrated (drug_ai_summary) [+4]")

    # Notes routing: check for agent_audit_log or clinical_notes evidence
    if _table_exists(catalog, "agent_audit_log"):
        agent_score += 2
        log("Agent: notes/audit routing demonstrated [+2]")
    elif _table_exists(SHARED_CATALOG, "clinical_notes") and has_ai_tables:
        agent_score += 1
        log("Agent: clinical_notes accessible, likely used [+1]")

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
    # Genie Integration (+3): check for genie_agent_log table
    if _table_exists(catalog, "genie_agent_log"):
        cnt = _table_count(catalog, "genie_agent_log")
        if cnt > 0:
            scores["bonus"] += 3
            log(f"Bonus: Genie integration log found, {cnt} entries [+3]")
        else:
            scores["bonus"] += 1
            log("Bonus: genie_agent_log exists but empty [+1]")
    else:
        log("Bonus: no Genie integration (genie_agent_log not found)")

    # Safety Guardrails (+2): check for audit log
    if _table_exists(catalog, "agent_audit_log"):
        scores["bonus"] += 2
        log("Bonus: audit log table found (safety guardrails) [+2]")

    # Multi-step Reasoning (+3): cross-table AI analysis
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

spark.createDataFrame(df_scores).write.format("delta").mode("overwrite").saveAsTable(
    "dataops_olympics.default.event4_scores"
)
print("Saved to dataops_olympics.default.event4_scores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Unified Leaderboard

# COMMAND ----------

_LB = "dataops_olympics.default.olympics_leaderboard"
_RT = "dataops_olympics.default.registered_teams"
spark.sql(f"CREATE TABLE IF NOT EXISTS {_LB} (team STRING, event STRING, category STRING, points DOUBLE, max_points DOUBLE, scored_at TIMESTAMP)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {_RT} (team STRING)")

from datetime import datetime as _dt
_now = _dt.now()
_event = "Event 4: GenAI Agents"

for r in results:
    _t = r["team"]
    if spark.sql(f"SELECT 1 FROM {_RT} WHERE team = '{_t}'").count() == 0:
        spark.sql(f"INSERT INTO {_RT} VALUES ('{_t}')")
    for cat, pts, mx in [
        ("Exploration", r["exploration"], 3), ("SystemPrompt", r["system_prompt"], 5),
        ("AgentFunction", r["agent_function"], 12), ("AIFunctions", r["ai_functions"], 10),
        ("TestPrompts", r["test_prompts"], 10), ("Bonus", r["bonus"], 8),
    ]:
        spark.sql(f"INSERT INTO {_LB} VALUES ('{_t}', '{_event}', '{cat}', {pts}, {mx}, '{_now}')")

print(f"Leaderboard updated: {len(results)} teams × 6 categories")
