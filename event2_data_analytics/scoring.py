# Databricks notebook source
# MAGIC %md
# MAGIC # Event 2: Automated Scoring
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY**
# MAGIC
# MAGIC ### How to Use
# MAGIC 1. Run the **benchmark_questions** notebook first to generate the answer key
# MAGIC 2. Fill in the `TEAM_RESULTS` dict below with each team's answers, methods, and speed wins
# MAGIC 3. Run All to compute scores and generate the leaderboard
# MAGIC
# MAGIC ### Scoring Breakdown
# MAGIC
# MAGIC | Category | Points |
# MAGIC |----------|--------|
# MAGIC | **Build: SQL** (3 practice queries executed) | 3 pts |
# MAGIC | **Build: Genie** (space exists + responds) | 5 pts |
# MAGIC | **Benchmark** (8 Qs: 2 pts SQL / 3 pts Genie) | 16-24 pts |
# MAGIC | **Speed bonus** (first correct per Q) | up to 8 pts |
# MAGIC | **Bonus: AI Summary** | +3 pts |
# MAGIC | **Bonus: Cohort Analysis** | +2 pts |
# MAGIC | **Bonus: Statistical Test** | +3 pts |
# MAGIC | **Max possible** | **40 pts (+8 bonus)** |

# COMMAND ----------

TEAMS = ["team_01", "team_02", "team_03", "team_04"]
SCHEMA = "default"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Answer Key

# COMMAND ----------

answer_key_df = spark.table("dataops_olympics.default.event2_answer_key").toPandas()
ANSWER_KEY = {row["question_id"]: row["answer"] for _, row in answer_key_df.iterrows()}

print("Answer Key Loaded:")
for q, a in sorted(ANSWER_KEY.items()):
    print(f"  {q}: {a}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enter Team Results
# MAGIC
# MAGIC Fill in after the race. For each team:
# MAGIC - `answers`: what the team reported for each question
# MAGIC - `methods`: "SQL" or "Genie" for each question
# MAGIC - `has_genie`: True if team created a working Genie space
# MAGIC - `sql_queries_run`: number of practice SQL queries executed (0-3)

# COMMAND ----------

TEAM_RESULTS = {
    "team_01": {
        "answers": {"Q1": "", "Q2": "", "Q3": "", "Q4": "", "Q5": "", "Q6": "", "Q7": "", "Q8": ""},
        "methods": {"Q1": "", "Q2": "", "Q3": "", "Q4": "", "Q5": "", "Q6": "", "Q7": "", "Q8": ""},
        "has_genie": False,
        "sql_queries_run": 0,
    },
    "team_02": {
        "answers": {"Q1": "", "Q2": "", "Q3": "", "Q4": "", "Q5": "", "Q6": "", "Q7": "", "Q8": ""},
        "methods": {"Q1": "", "Q2": "", "Q3": "", "Q4": "", "Q5": "", "Q6": "", "Q7": "", "Q8": ""},
        "has_genie": False,
        "sql_queries_run": 0,
    },
    "team_03": {
        "answers": {"Q1": "", "Q2": "", "Q3": "", "Q4": "", "Q5": "", "Q6": "", "Q7": "", "Q8": ""},
        "methods": {"Q1": "", "Q2": "", "Q3": "", "Q4": "", "Q5": "", "Q6": "", "Q7": "", "Q8": ""},
        "has_genie": False,
        "sql_queries_run": 0,
    },
    "team_04": {
        "answers": {"Q1": "", "Q2": "", "Q3": "", "Q4": "", "Q5": "", "Q6": "", "Q7": "", "Q8": ""},
        "methods": {"Q1": "", "Q2": "", "Q3": "", "Q4": "", "Q5": "", "Q6": "", "Q7": "", "Q8": ""},
        "has_genie": False,
        "sql_queries_run": 0,
    },
}

SPEED_WINNERS = {
    "Q1": "",  # team name
    "Q2": "",
    "Q3": "",
    "Q4": "",
    "Q5": "",
    "Q6": "",
    "Q7": "",
    "Q8": "",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring Engine

# COMMAND ----------

import pandas as pd


def _normalize_answer(raw):
    """Normalize an answer for comparison — strip whitespace, lowercase, remove % sign."""
    if raw is None:
        return ""
    s = str(raw).strip().lower().replace("%", "").replace(",", "")
    return s


def _check_answer(team_answer, correct_answer):
    """Check if team answer matches the correct answer (fuzzy on numbers)."""
    ta = _normalize_answer(team_answer)
    ca = _normalize_answer(correct_answer)

    if not ta:
        return False

    if ta == ca:
        return True

    # Try numeric comparison (within 0.5 tolerance for rounding differences)
    try:
        ta_nums = [float(x.strip()) for x in ta.replace("/", " ").split() if x.replace(".", "").replace("-", "").isdigit() or "." in x]
        ca_nums = [float(x.strip()) for x in ca.replace("/", " ").split() if x.replace(".", "").replace("-", "").isdigit() or "." in x]
        if ta_nums and ca_nums:
            return any(abs(t - c) <= 0.5 for t in ta_nums for c in ca_nums)
    except (ValueError, TypeError):
        pass

    # Check if key parts match
    return ca in ta or ta in ca


def score_team(team_name: str) -> dict:
    """Score a team's Event 2 performance."""
    data = TEAM_RESULTS.get(team_name, {})
    scores = {
        "team": team_name,
        "build_sql": 0,
        "build_genie": 0,
        "benchmark": 0,
        "speed_bonus": 0,
        "total": 0,
        "correct_count": 0,
        "genie_answers": 0,
        "details": [],
    }

    def log(msg):
        scores["details"].append(msg)

    # ─── BUILD: SQL (3 pts max) ───
    sql_count = min(data.get("sql_queries_run", 0), 3)
    scores["build_sql"] = sql_count
    if sql_count > 0:
        log(f"Build SQL: {sql_count}/3 practice queries [+{sql_count}]")
    else:
        log("Build SQL: no practice queries executed [+0]")

    # ─── BUILD: GENIE (5 pts max) ───
    if data.get("has_genie", False):
        scores["build_genie"] = 5
        log("Build Genie: space created and working [+5]")
    else:
        log("Build Genie: no Genie space detected [+0]")

    # ─── BENCHMARK (2 pts SQL, 3 pts Genie per correct answer) ───
    answers = data.get("answers", {})
    methods = data.get("methods", {})

    for qid in sorted(ANSWER_KEY.keys()):
        team_ans = answers.get(qid, "")
        method = methods.get(qid, "SQL").upper()
        correct = ANSWER_KEY.get(qid, "")

        if not team_ans:
            log(f"{qid}: no answer submitted")
            continue

        is_correct = _check_answer(team_ans, correct)

        if is_correct:
            pts = 3 if method == "GENIE" else 2
            scores["benchmark"] += pts
            scores["correct_count"] += 1
            if method == "GENIE":
                scores["genie_answers"] += 1
            log(f"{qid}: CORRECT via {method} [+{pts}] (answer: {team_ans})")
        else:
            log(f"{qid}: WRONG via {method} [+0] (answer: {team_ans}, expected: {correct})")

    # ─── SPEED BONUS ───
    for qid, winner in SPEED_WINNERS.items():
        if winner == team_name:
            scores["speed_bonus"] += 1
            log(f"{qid}: SPEED BONUS — first correct [+1]")

    # ─── BONUS (auto-detected from tables) ───
    catalog = team_name
    bonus = 0
    try:
        spark.table(f"{catalog}.default.heart_executive_summary")
        bonus += 3
        log("Bonus: AI-powered executive summary found [+3]")
    except Exception:
        pass

    try:
        spark.table(f"{catalog}.default.heart_cohort_comparison")
        bonus += 2
        log("Bonus: advanced cohort comparison found [+2]")
    except Exception:
        pass

    try:
        spark.table(f"{catalog}.default.heart_chi2_test")
        bonus += 3
        log("Bonus: chi-squared test results found [+3]")
    except Exception:
        pass

    scores["bonus"] = bonus
    scores["total"] = scores["build_sql"] + scores["build_genie"] + scores["benchmark"] + scores["speed_bonus"] + bonus
    return scores

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Scoring — All Teams

# COMMAND ----------

results = []
for team in TEAMS:
    print(f"\n{'='*60}")
    print(f"  SCORING: {team}")
    print(f"{'='*60}")
    r = score_team(team)
    results.append(r)
    print(f"  Build SQL: {r['build_sql']}/3  Build Genie: {r['build_genie']}/5  Benchmark: {r['benchmark']}/24  Speed: {r['speed_bonus']}/8  Bonus: {r['bonus']}/8")
    print(f"  Correct: {r['correct_count']}/8  ({r['genie_answers']} via Genie)")
    print(f"  TOTAL: {r['total']}/48")
    print()
    for d in r["details"]:
        print(f"    {d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leaderboard

# COMMAND ----------

import plotly.express as px
import plotly.graph_objects as go

df_scores = pd.DataFrame([{
    "Team": r["team"],
    "Build_SQL": r["build_sql"],
    "Build_Genie": r["build_genie"],
    "Benchmark": r["benchmark"],
    "Speed": r["speed_bonus"],
    "Bonus": r["bonus"],
    "Total": r["total"],
    "Correct": r["correct_count"],
    "Genie_Answers": r["genie_answers"],
} for r in results]).sort_values("Total", ascending=False)

print("=" * 72)
print("  EVENT 2: DATA ANALYTICS — FINAL LEADERBOARD")
print("=" * 72)
for rank, (_, row) in enumerate(df_scores.iterrows(), 1):
    medal = {1: "[GOLD]  ", 2: "[SILVER]", 3: "[BRONZE]"}.get(rank, "        ")
    print(f"  {rank}. {medal} {row['Team']:12s} | {int(row['Total']):2d}/48 pts | {int(row['Correct'])}/8 correct | {int(row['Genie_Answers'])} via Genie | Bonus:{int(row['Bonus'])}")
print("=" * 72)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Score Breakdown

# COMMAND ----------

categories = ["Build_SQL", "Build_Genie", "Benchmark", "Speed", "Bonus"]
cat_colors = {
    "Build_SQL": "#3498db",
    "Build_Genie": "#9b59b6",
    "Benchmark": "#2ecc71",
    "Speed": "#e74c3c",
    "Bonus": "#f39c12",
}
cat_max = {"Build_SQL": 3, "Build_Genie": 5, "Benchmark": 24, "Speed": 8, "Bonus": 8}

ordered = df_scores.sort_values("Total", ascending=True)

fig = go.Figure()
for cat in categories:
    fig.add_trace(go.Bar(
        y=ordered["Team"],
        x=ordered[cat],
        name=f"{cat.replace('_', ' ')} (/{cat_max[cat]})",
        orientation="h",
        marker=dict(color=cat_colors[cat], line=dict(color="#1a1a2e", width=1)),
        text=ordered[cat].astype(int),
        textposition="inside",
        textfont=dict(size=13, color="white"),
    ))

fig.update_layout(
    barmode="stack",
    title=dict(
        text="Event 2: Data Analytics — Score Breakdown",
        font=dict(size=24, color="white"),
        x=0.5,
    ),
    plot_bgcolor="#1a1a2e",
    paper_bgcolor="#16213e",
    font=dict(color="white", size=13),
    xaxis=dict(title="Points", gridcolor="#2d3436", range=[0, 52]),
    yaxis=dict(tickfont=dict(size=14)),
    legend=dict(
        orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5,
        font=dict(size=12),
    ),
    height=max(350, len(df_scores) * 80 + 120),
    margin=dict(l=120, r=40, t=100, b=60),
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL vs Genie Usage

# COMMAND ----------

fig2 = go.Figure()

for _, row in df_scores.iterrows():
    sql_answers = int(row["Correct"]) - int(row["Genie_Answers"])
    fig2.add_trace(go.Bar(
        x=[row["Team"]],
        y=[sql_answers],
        name="SQL",
        marker=dict(color="#3498db"),
        showlegend=row.name == df_scores.index[0],
        legendgroup="SQL",
    ))
    fig2.add_trace(go.Bar(
        x=[row["Team"]],
        y=[int(row["Genie_Answers"])],
        name="Genie",
        marker=dict(color="#9b59b6"),
        showlegend=row.name == df_scores.index[0],
        legendgroup="Genie",
    ))

fig2.update_layout(
    barmode="stack",
    title=dict(text="Answer Method: SQL vs Genie", font=dict(size=22, color="white"), x=0.5),
    plot_bgcolor="#1a1a2e",
    paper_bgcolor="#16213e",
    font=dict(color="white"),
    yaxis=dict(title="Correct Answers", gridcolor="#2d3436"),
    height=400,
)
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

spark.createDataFrame(df_scores).write.format("delta").mode("overwrite").saveAsTable(
    "dataops_olympics.default.event2_scores"
)
print("Results saved to dataops_olympics.default.event2_scores")
