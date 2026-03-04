# Databricks notebook source
# MAGIC %md
# MAGIC # Event 2: Automated Scoring
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY**
# MAGIC
# MAGIC ### How to Use
# MAGIC 1. Run the **benchmark_questions** notebook first to generate the answer key
# MAGIC 2. Enter each team's **Genie Space ID** in `GENIE_SPACE_IDS` (auto-evaluates via Conversation API)
# MAGIC 3. Optionally fill in `MANUAL_SQL_RESULTS` for SQL answers and `SPEED_WINNERS`
# MAGIC 4. Run All — the notebook automatically queries each team's Genie, grades answers, and builds a leaderboard
# MAGIC
# MAGIC ### Scoring Breakdown
# MAGIC
# MAGIC | Category | Points |
# MAGIC |----------|--------|
# MAGIC | **Build: SQL** (3 practice queries executed) | 3 pts |
# MAGIC | **Build: Genie** (space responds to questions) | 5 pts |
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
ANSWER_KEY = {}
ANSWER_QUESTIONS = {}
for _, row in answer_key_df.iterrows():
    ANSWER_KEY[row["question_id"]] = row["answer"]
    ANSWER_QUESTIONS[row["question_id"]] = row["question"]

print("Answer Key Loaded:")
for q, a in sorted(ANSWER_KEY.items()):
    print(f"  {q}: {ANSWER_QUESTIONS[q]}")
    print(f"       Expected: {a}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Genie Space IDs
# MAGIC
# MAGIC Enter the Genie Space ID for each team. Leave blank if the team didn't create one.
# MAGIC
# MAGIC > To find a Genie Space ID: open the Genie space in the workspace → the URL looks like
# MAGIC > `https://<workspace>/genie/rooms/<SPACE_ID>/...` → copy that `SPACE_ID`.

# COMMAND ----------

GENIE_SPACE_IDS = {
    "team_01": "",  # e.g. "01ef1234567890abcdef1234567890ab"
    "team_02": "",
    "team_03": "",
    "team_04": "",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Inputs
# MAGIC
# MAGIC SQL answers are still manual — fill in what teams reported during the race.
# MAGIC Genie answers are **auto-populated** by the Conversation API (next section).

# COMMAND ----------

MANUAL_SQL_RESULTS = {
    "team_01": {
        "sql_answers": {"Q1": "", "Q2": "", "Q3": "", "Q4": "", "Q5": "", "Q6": "", "Q7": "", "Q8": ""},
        "sql_queries_run": 0,
    },
    "team_02": {
        "sql_answers": {"Q1": "", "Q2": "", "Q3": "", "Q4": "", "Q5": "", "Q6": "", "Q7": "", "Q8": ""},
        "sql_queries_run": 0,
    },
    "team_03": {
        "sql_answers": {"Q1": "", "Q2": "", "Q3": "", "Q4": "", "Q5": "", "Q6": "", "Q7": "", "Q8": ""},
        "sql_queries_run": 0,
    },
    "team_04": {
        "sql_answers": {"Q1": "", "Q2": "", "Q3": "", "Q4": "", "Q5": "", "Q6": "", "Q7": "", "Q8": ""},
        "sql_queries_run": 0,
    },
}

SPEED_WINNERS = {
    "Q1": "",  # team name of first correct answer
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
# MAGIC ## Automated Genie Benchmark
# MAGIC
# MAGIC Sends all 8 benchmark questions to each team's Genie space using the
# MAGIC **Genie Conversation API**. Extracts text answers, SQL generated, and query results.

# COMMAND ----------

import datetime
import time
import pandas as pd

try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    SDK_AVAILABLE = True
    print("Databricks SDK loaded — Genie auto-evaluation enabled")
except ImportError:
    SDK_AVAILABLE = False
    print("WARNING: databricks-sdk not available. Install with: %pip install databricks-sdk")
    print("         Falling back to manual-only scoring.")

# COMMAND ----------

def _extract_genie_answer(msg):
    """Extract the best answer string from a GenieMessage's attachments."""
    text_answer = None
    sql_query = None
    query_data = None

    for att in (msg.attachments or []):
        if att.text and att.text.content:
            text_answer = att.text.content
        if att.query:
            sql_query = att.query.query
            if att.query.query_result_metadata and att.query.query_result_metadata.row_count:
                try:
                    result = w.genie.get_message_attachment_query_result(
                        space_id=msg.space_id,
                        conversation_id=msg.conversation_id,
                        message_id=msg.message_id,
                        attachment_id=att.attachment_id,
                    )
                    sr = result.statement_response
                    if sr and sr.result and sr.result.data_array:
                        query_data = sr.result.data_array
                except Exception as e:
                    query_data = f"ERROR fetching result: {e}"

    short_answer = _summarize_query_data(query_data) if query_data and isinstance(query_data, list) else text_answer
    return {
        "text": text_answer,
        "sql": sql_query,
        "query_data": query_data,
        "short_answer": short_answer or text_answer or "",
    }


def _summarize_query_data(data_array):
    """Flatten a small query result into a string for answer comparison."""
    if not data_array:
        return ""
    if len(data_array) == 1 and len(data_array[0]) == 1:
        return str(data_array[0][0])
    parts = []
    for row in data_array[:5]:
        parts.append(", ".join(str(v) for v in row if v is not None))
    return "; ".join(parts)


def run_genie_benchmark(space_id, team_name):
    """Send all benchmark questions to a Genie space and collect answers."""
    results = {}
    print(f"\n  Querying Genie space {space_id[:12]}... for {team_name}")

    for qid in sorted(ANSWER_QUESTIONS.keys()):
        question = ANSWER_QUESTIONS[qid]
        print(f"    {qid}: {question[:60]}...", end=" ", flush=True)
        try:
            msg = w.genie.start_conversation_and_wait(
                space_id=space_id,
                content=question,
                timeout=datetime.timedelta(minutes=3),
            )
            if msg.status and msg.status.value == "COMPLETED":
                answer_info = _extract_genie_answer(msg)
                results[qid] = answer_info
                print(f"-> {answer_info['short_answer'][:80]}")
            elif msg.status and msg.status.value == "FAILED":
                error_msg = msg.error.error if msg.error else "unknown"
                results[qid] = {"text": None, "sql": None, "query_data": None, "short_answer": f"FAILED: {error_msg}"}
                print(f"-> FAILED: {error_msg[:60]}")
            else:
                status = msg.status.value if msg.status else "unknown"
                results[qid] = {"text": None, "sql": None, "query_data": None, "short_answer": f"Status: {status}"}
                print(f"-> Status: {status}")
        except Exception as e:
            results[qid] = {"text": None, "sql": None, "query_data": None, "short_answer": f"ERROR: {e}"}
            print(f"-> ERROR: {str(e)[:60]}")

    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Genie Auto-Evaluation

# COMMAND ----------

GENIE_RESULTS = {}

if SDK_AVAILABLE:
    for team, space_id in GENIE_SPACE_IDS.items():
        if space_id and space_id.strip():
            print(f"\n{'='*60}")
            print(f"  AUTO-EVALUATING GENIE: {team}")
            print(f"{'='*60}")
            GENIE_RESULTS[team] = run_genie_benchmark(space_id.strip(), team)
        else:
            print(f"  {team}: no Genie space ID provided — skipping")
else:
    print("Skipping Genie auto-evaluation (SDK not available)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genie Evaluation Summary

# COMMAND ----------

if GENIE_RESULTS:
    print("=" * 72)
    print("  GENIE AUTO-EVALUATION RESULTS")
    print("=" * 72)
    for team, results in sorted(GENIE_RESULTS.items()):
        print(f"\n  {team}:")
        for qid, info in sorted(results.items()):
            expected = ANSWER_KEY.get(qid, "?")
            got = info["short_answer"][:60] if info["short_answer"] else "NO ANSWER"
            had_sql = "SQL" if info.get("sql") else "text"
            print(f"    {qid} [{had_sql:4s}]: {got:50s} | expected: {expected}")
    print("=" * 72)

    genie_detail_rows = []
    for team, results in GENIE_RESULTS.items():
        for qid, info in results.items():
            genie_detail_rows.append({
                "team": team,
                "question_id": qid,
                "question": ANSWER_QUESTIONS.get(qid, ""),
                "genie_answer": info.get("short_answer", ""),
                "genie_sql": info.get("sql", ""),
                "genie_text": info.get("text", ""),
                "expected_answer": ANSWER_KEY.get(qid, ""),
            })
    if genie_detail_rows:
        genie_df = spark.createDataFrame(pd.DataFrame(genie_detail_rows))
        genie_df.write.format("delta").mode("overwrite").saveAsTable(
            "dataops_olympics.default.event2_genie_benchmark"
        )
        print("\nDetailed Genie results saved to dataops_olympics.default.event2_genie_benchmark")
else:
    print("No Genie results to display. Enter Genie Space IDs above to auto-evaluate.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Scoring Engine

# COMMAND ----------

def _normalize_answer(raw):
    """Normalize an answer for comparison."""
    if raw is None:
        return ""
    s = str(raw).strip().lower().replace("%", "").replace(",", "").replace(";", " ")
    return s


def _check_answer(team_answer, correct_answer):
    """Check if team answer matches the correct answer (fuzzy on numbers)."""
    ta = _normalize_answer(team_answer)
    ca = _normalize_answer(correct_answer)

    if not ta or ta.startswith("error") or ta.startswith("failed"):
        return False

    if ta == ca:
        return True

    try:
        ta_nums = [float(x.strip()) for x in ta.replace("/", " ").split() if _is_number(x.strip())]
        ca_nums = [float(x.strip()) for x in ca.replace("/", " ").split() if _is_number(x.strip())]
        if ta_nums and ca_nums:
            return any(abs(t - c) <= 0.5 for t in ta_nums for c in ca_nums)
    except (ValueError, TypeError):
        pass

    return ca in ta or ta in ca


def _is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def score_team(team_name: str) -> dict:
    """Score a team's Event 2 performance using both manual and auto-Genie results."""
    manual = MANUAL_SQL_RESULTS.get(team_name, {})
    genie = GENIE_RESULTS.get(team_name, {})

    scores = {
        "team": team_name,
        "build_sql": 0,
        "build_genie": 0,
        "benchmark": 0,
        "speed_bonus": 0,
        "bonus": 0,
        "total": 0,
        "correct_count": 0,
        "genie_answers": 0,
        "genie_correct": 0,
        "sql_correct": 0,
        "details": [],
    }

    def log(msg):
        scores["details"].append(msg)

    # ─── BUILD: SQL (3 pts max) ───
    sql_count = min(manual.get("sql_queries_run", 0), 3)
    scores["build_sql"] = sql_count
    if sql_count > 0:
        log(f"Build SQL: {sql_count}/3 practice queries [+{sql_count}]")
    else:
        log("Build SQL: no practice queries executed [+0]")

    # ─── BUILD: GENIE (5 pts max) — auto-detected from API results ───
    has_genie = bool(genie)
    genie_responded = sum(1 for r in genie.values() if r.get("short_answer") and not r["short_answer"].startswith("ERROR"))
    if genie_responded >= 6:
        scores["build_genie"] = 5
        log(f"Build Genie: space responded to {genie_responded}/8 questions [+5]")
    elif genie_responded >= 3:
        scores["build_genie"] = 3
        log(f"Build Genie: space responded to {genie_responded}/8 questions [+3]")
    elif has_genie:
        scores["build_genie"] = 1
        log(f"Build Genie: space exists but poor response rate ({genie_responded}/8) [+1]")
    else:
        log("Build Genie: no Genie space provided [+0]")

    # ─── BENCHMARK (per question: best of SQL or Genie) ───
    sql_answers = manual.get("sql_answers", {})

    for qid in sorted(ANSWER_KEY.keys()):
        correct = ANSWER_KEY.get(qid, "")
        sql_ans = sql_answers.get(qid, "")
        genie_info = genie.get(qid, {})
        genie_ans = genie_info.get("short_answer", "")

        sql_correct = _check_answer(sql_ans, correct) if sql_ans else False
        genie_correct = _check_answer(genie_ans, correct) if genie_ans else False

        if genie_correct:
            scores["benchmark"] += 3
            scores["correct_count"] += 1
            scores["genie_answers"] += 1
            scores["genie_correct"] += 1
            genie_short = genie_ans[:50]
            log(f"{qid}: CORRECT via Genie [+3] (answer: {genie_short})")
        elif sql_correct:
            scores["benchmark"] += 2
            scores["correct_count"] += 1
            scores["sql_correct"] += 1
            log(f"{qid}: CORRECT via SQL [+2] (answer: {sql_ans})")
        else:
            attempts = []
            if sql_ans:
                attempts.append(f"SQL='{sql_ans}'")
            if genie_ans and not genie_ans.startswith("ERROR"):
                attempts.append(f"Genie='{genie_ans[:40]}'")
            attempt_str = ", ".join(attempts) if attempts else "no answer"
            log(f"{qid}: WRONG [+0] ({attempt_str}, expected: {correct})")

    # ─── SPEED BONUS ───
    for qid, winner in SPEED_WINNERS.items():
        if winner == team_name:
            scores["speed_bonus"] += 1
            log(f"{qid}: SPEED BONUS — first correct [+1]")

    # ─── BONUS (auto-detected from tables) ───
    catalog = team_name
    try:
        spark.table(f"{catalog}.default.heart_executive_summary")
        scores["bonus"] += 3
        log("Bonus: AI-powered executive summary found [+3]")
    except Exception:
        pass

    try:
        spark.table(f"{catalog}.default.heart_cohort_comparison")
        scores["bonus"] += 2
        log("Bonus: advanced cohort comparison found [+2]")
    except Exception:
        pass

    try:
        spark.table(f"{catalog}.default.heart_chi2_test")
        scores["bonus"] += 3
        log("Bonus: chi-squared test results found [+3]")
    except Exception:
        pass

    scores["total"] = scores["build_sql"] + scores["build_genie"] + scores["benchmark"] + scores["speed_bonus"] + scores["bonus"]
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
    print(f"  Correct: {r['correct_count']}/8  (Genie: {r['genie_correct']}, SQL: {r['sql_correct']})")
    print(f"  TOTAL: {r['total']}/48")
    print()
    for d in r["details"]:
        print(f"    {d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leaderboard

# COMMAND ----------

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
    "Genie_Correct": r["genie_correct"],
    "SQL_Correct": r["sql_correct"],
} for r in results]).sort_values("Total", ascending=False)

print("=" * 80)
print("  EVENT 2: DATA ANALYTICS — FINAL LEADERBOARD")
print("=" * 80)
for rank, (_, row) in enumerate(df_scores.iterrows(), 1):
    medal = {1: "[GOLD]  ", 2: "[SILVER]", 3: "[BRONZE]"}.get(rank, "        ")
    print(f"  {rank}. {medal} {row['Team']:12s} | {int(row['Total']):2d}/48 pts | {int(row['Correct'])}/8 correct | Genie:{int(row['Genie_Correct'])} SQL:{int(row['SQL_Correct'])} | Bonus:{int(row['Bonus'])}")
print("=" * 80)

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
# MAGIC ### SQL vs Genie Accuracy

# COMMAND ----------

fig2 = go.Figure()

for _, row in ordered.iterrows():
    fig2.add_trace(go.Bar(
        x=[row["Team"]],
        y=[int(row["SQL_Correct"])],
        name="SQL Correct",
        marker=dict(color="#3498db"),
        showlegend=row.name == ordered.index[0],
        legendgroup="SQL",
    ))
    fig2.add_trace(go.Bar(
        x=[row["Team"]],
        y=[int(row["Genie_Correct"])],
        name="Genie Correct",
        marker=dict(color="#9b59b6"),
        showlegend=row.name == ordered.index[0],
        legendgroup="Genie",
    ))

fig2.update_layout(
    barmode="stack",
    title=dict(text="Correct Answers: SQL vs Genie", font=dict(size=22, color="white"), x=0.5),
    plot_bgcolor="#1a1a2e",
    paper_bgcolor="#16213e",
    font=dict(color="white"),
    yaxis=dict(title="Correct Answers", gridcolor="#2d3436", range=[0, 9]),
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
