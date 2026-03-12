# Databricks notebook source
# MAGIC %md
# MAGIC # Event 2: Fully Automated Scoring
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY**
# MAGIC
# MAGIC ### How It Works — Zero Manual Input
# MAGIC
# MAGIC This notebook is **fully automated**:
# MAGIC
# MAGIC 1. **SQL answers** — read from each team's `event2_submissions` table (teams use `submit_answer()`)
# MAGIC 2. **Genie answers** — queried via the **Genie Conversation API** using each team's space ID
# MAGIC 3. **Answer checking** — fuzzy numeric match first, then **`ai_query()` LLM judge** for ambiguous cases
# MAGIC 4. **Bonus challenges** — auto-detected from team catalog tables
# MAGIC
# MAGIC Only thing the organizer fills in: `GENIE_SPACE_IDS` and `SPEED_WINNERS`.
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

# Only score teams that have submitted for this event
_event_filter = "event2"
_submitted = spark.sql(f"""
    SELECT DISTINCT team_name FROM dataops_olympics.default.event_submissions
    WHERE event_name = '{_event_filter}'
""").collect()
TEAMS = sorted([r.team_name for r in _submitted])
if not TEAMS:
    print("WARNING: No teams have submitted for this event yet!")
else:
    print(f"Scoring {len(TEAMS)} teams that submitted: {TEAMS}")

SCHEMA = "default"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Answer Key

# COMMAND ----------

# Answer key in instructor-only catalog (not visible to teams)
try:
    answer_key_df = spark.table("dataops_olympics_instructor.default.event2_answer_key").toPandas()
except Exception:
    answer_key_df = spark.table("dataops_olympics.default.event2_answer_key").toPandas()
ANSWER_KEY = {}
ANSWER_QUESTIONS = {}
for _, row in answer_key_df.iterrows():
    ANSWER_KEY[row["question_id"]] = row["answer"]
    ANSWER_QUESTIONS[row["question_id"]] = row["question"]

print("Answer Key Loaded:")
for q, a in sorted(ANSWER_KEY.items()):
    print(f"  {q}: {a}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Enter Genie Space IDs and speed winners — everything else is auto-detected.

# COMMAND ----------

GENIE_SPACE_IDS = {
    # "team_01": "01ef...",  # Enter Genie Space IDs here
}

# Speed bonus is AUTO-CALCULATED from submission timestamps.
# For each question, the first team to submit the correct answer gets +1.
# No manual input needed — computed in Step 4 below.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Collect SQL Submissions from Team Tables
# MAGIC
# MAGIC Reads each team's `event2_submissions` Delta table (written by `submit_answer()`).
# MAGIC Takes the **latest** answer per question.

# COMMAND ----------

import pandas as pd
from datetime import datetime

SQL_SUBMISSIONS = {}

for team in TEAMS:
    try:
        subs = spark.sql(f"""
            SELECT question_id, answer, method, submitted_at,
                ROW_NUMBER() OVER (PARTITION BY question_id ORDER BY submitted_at DESC) AS rn
            FROM {team}.default.event2_submissions
            WHERE team = '{team}'
        """).filter("rn = 1").drop("rn").toPandas()

        team_subs = {}
        for _, row in subs.iterrows():
            team_subs[row["question_id"]] = {
                "answer": row["answer"],
                "method": row["method"],
                "submitted_at": str(row["submitted_at"]),
            }
        SQL_SUBMISSIONS[team] = team_subs
        print(f"  {team}: {len(team_subs)} submissions loaded")
    except Exception as e:
        SQL_SUBMISSIONS[team] = {}
        print(f"  {team}: no submissions table ({str(e)[:60]})")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Auto-Evaluate Genie Spaces via Conversation API
# MAGIC
# MAGIC Sends all 8 benchmark questions to each team's Genie space programmatically.

# COMMAND ----------

import datetime as dt
import time

try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    SDK_AVAILABLE = True
    print("Databricks SDK loaded — Genie auto-evaluation enabled")
except Exception as e:
    SDK_AVAILABLE = False
    print(f"WARNING: Genie auto-evaluation disabled ({type(e).__name__}: {str(e)[:80]})")


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
                    query_data = f"ERROR: {e}"

    short_answer = _summarize_query_data(query_data) if isinstance(query_data, list) else text_answer
    return {
        "text": text_answer,
        "sql": sql_query,
        "query_data": query_data,
        "short_answer": short_answer or text_answer or "",
    }


def _summarize_query_data(data_array):
    """Flatten a small query result into a comparable string."""
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
                timeout=dt.timedelta(minutes=3),
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

GENIE_RESULTS = {}

if SDK_AVAILABLE:
    for team, space_id in GENIE_SPACE_IDS.items():
        if space_id and space_id.strip():
            print(f"\n{'='*60}")
            print(f"  AUTO-EVALUATING GENIE: {team}")
            print(f"{'='*60}")
            GENIE_RESULTS[team] = run_genie_benchmark(space_id.strip(), team)
        else:
            print(f"  {team}: no Genie space ID — skipping")
else:
    print("Skipping Genie auto-evaluation (SDK not available)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Answer Checking (Data Comparison Only)
# MAGIC
# MAGIC Compares answers using fuzzy numeric matching and substring containment.
# MAGIC No LLM judge — deterministic comparison only.

# COMMAND ----------

def _normalize(raw):
    if raw is None:
        return ""
    return str(raw).strip().lower().replace("%", "").replace(",", "").replace(";", " ")


def _is_number(s):
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def _fuzzy_numeric_match(team_answer, correct_answer, tolerance=0.5):
    """Compare answers using numeric matching and substring containment."""
    ta = _normalize(team_answer)
    ca = _normalize(correct_answer)

    if not ta or ta.startswith("error") or ta.startswith("failed"):
        return False

    # Exact string match
    if ta == ca:
        return True

    # Extract all numbers from both and compare
    try:
        ta_nums = [float(x) for x in ta.split() if _is_number(x)]
        ca_nums = [float(x) for x in ca.split() if _is_number(x)]
        if ta_nums and ca_nums:
            # All expected numbers must be matched
            matched = 0
            for c in ca_nums:
                if any(abs(t - c) <= tolerance for t in ta_nums):
                    matched += 1
            if matched == len(ca_nums):
                return True
            return False
    except Exception:
        pass

    # Substring containment (handles "3 (asymptomatic)" matching "3")
    if ca in ta or ta in ca:
        return True

    # Check if key parts match (e.g., "60+" in "60+ (65.2%)")
    ca_parts = [p.strip() for p in ca.replace(",", " ").split() if len(p.strip()) > 1]
    if ca_parts and all(p in ta for p in ca_parts):
        return True

    return False


def check_answer(team_answer, correct_answer, question):
    """Check answer using data comparison only. No LLM judge."""
    result = _fuzzy_numeric_match(team_answer, correct_answer)
    return result, "data_match"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Score All Teams

# COMMAND ----------

def score_team(team_name: str) -> dict:
    subs = SQL_SUBMISSIONS.get(team_name, {})
    genie = GENIE_RESULTS.get(team_name, {})

    scores = {
        "team": team_name,
        "build_sql": 0, "build_genie": 0, "benchmark": 0,
        "speed_bonus": 0, "bonus": 0, "total": 0,
        "correct_count": 0, "genie_correct": 0, "sql_correct": 0,
        "details": [],
    }

    def log(msg):
        scores["details"].append(msg)

    # ─── BUILD: SQL (3 pts) — count practice queries that were submitted ───
    practice_count = min(len(subs), 3)
    scores["build_sql"] = min(practice_count, 3)
    if practice_count > 0:
        log(f"Build SQL: {practice_count} answers submitted [+{min(practice_count, 3)}]")
    else:
        log("Build SQL: no submissions found [+0]")

    # ─── BUILD: GENIE (5 pts) — auto-detected from API ───
    has_genie = bool(genie)
    genie_responded = sum(1 for r in genie.values() if r.get("short_answer") and not str(r["short_answer"]).startswith("ERROR"))
    if genie_responded >= 6:
        scores["build_genie"] = 5
        log(f"Build Genie: responded to {genie_responded}/8 questions [+5]")
    elif genie_responded >= 3:
        scores["build_genie"] = 3
        log(f"Build Genie: responded to {genie_responded}/8 questions [+3]")
    elif has_genie:
        scores["build_genie"] = 1
        log(f"Build Genie: poor response rate ({genie_responded}/8) [+1]")
    else:
        log("Build Genie: no Genie space [+0]")

    # ─── BENCHMARK (best of SQL submission or Genie per question) ───
    for qid in sorted(ANSWER_KEY.keys()):
        correct = ANSWER_KEY[qid]
        question = ANSWER_QUESTIONS[qid]

        sub = subs.get(qid, {})
        sql_ans = sub.get("answer", "")
        sql_method = sub.get("method", "SQL")

        genie_info = genie.get(qid, {})
        genie_ans = genie_info.get("short_answer", "")

        # Try Genie first (higher points)
        genie_ok = False
        sql_ok = False
        judge_method = ""

        if genie_ans and not str(genie_ans).startswith("ERROR") and not str(genie_ans).startswith("FAILED"):
            genie_ok, judge_method = check_answer(genie_ans, correct, question)

        if sql_ans:
            sql_ok, jm = check_answer(sql_ans, correct, question)
            if not judge_method:
                judge_method = jm

        if genie_ok:
            scores["benchmark"] += 3
            scores["correct_count"] += 1
            scores["genie_correct"] += 1
            short = str(genie_ans)[:50]
            log(f"{qid}: CORRECT via Genie [{judge_method}] [+3] ({short})")
        elif sql_ok:
            pts = 3 if sql_method == "GENIE" else 2
            scores["benchmark"] += pts
            scores["correct_count"] += 1
            if sql_method == "GENIE":
                scores["genie_correct"] += 1
            else:
                scores["sql_correct"] += 1
            log(f"{qid}: CORRECT via {sql_method} [{judge_method}] [+{pts}] ({sql_ans})")
        else:
            attempts = []
            if sql_ans:
                attempts.append(f"SQL='{sql_ans}'")
            if genie_ans and not str(genie_ans).startswith("ERROR"):
                attempts.append(f"Genie='{str(genie_ans)[:40]}'")
            attempt_str = ", ".join(attempts) if attempts else "no answer"
            log(f"{qid}: WRONG [+0] ({attempt_str} | expected: {correct})")

    # ─── SPEED BONUS (auto-computed from timestamps) ───
    for qid, winner in SPEED_WINNERS.items():
        if winner == team_name:
            scores["speed_bonus"] += 1
            log(f"{qid}: SPEED BONUS — first correct answer [+1]")

    # ─── BONUS (auto-detected from tables) ───
    catalog = team_name
    try:
        spark.table(f"{catalog}.default.heart_executive_summary")
        scores["bonus"] += 3
        log("Bonus: AI executive summary found [+3]")
    except Exception:
        pass

    try:
        spark.table(f"{catalog}.default.heart_cohort_comparison")
        scores["bonus"] += 2
        log("Bonus: cohort comparison found [+2]")
    except Exception:
        pass

    try:
        spark.table(f"{catalog}.default.heart_chi2_test")
        scores["bonus"] += 3
        log("Bonus: chi-squared test found [+3]")
    except Exception:
        pass

    scores["total"] = scores["build_sql"] + scores["build_genie"] + scores["benchmark"] + scores["speed_bonus"] + scores["bonus"]
    return scores

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Scoring

# COMMAND ----------

# ─── Auto-compute SPEED_WINNERS from submission timestamps ───
print("Computing speed bonus from submission timestamps...")
SPEED_WINNERS = {}

# Collect all correct submissions with timestamps across all teams
all_correct = []  # [(team, qid, submitted_at)]
for team in TEAMS:
    subs = SQL_SUBMISSIONS.get(team, {})
    for qid, sub in subs.items():
        correct = ANSWER_KEY.get(qid, "")
        if correct and sub.get("answer"):
            is_correct, _ = check_answer(sub["answer"], correct, ANSWER_QUESTIONS.get(qid, ""))
            if is_correct:
                all_correct.append((team, qid, sub.get("submitted_at", "")))

# For each question, find the earliest correct submission
from collections import defaultdict
earliest = defaultdict(lambda: ("", "9999-99-99"))  # (team, timestamp)
for team, qid, ts in all_correct:
    if ts and ts < earliest[qid][1]:
        earliest[qid] = (team, ts)

for qid in sorted(ANSWER_KEY.keys()):
    winner_team, winner_ts = earliest[qid]
    if winner_team:
        SPEED_WINNERS[qid] = winner_team
        print(f"  {qid}: {winner_team} (first correct at {winner_ts})")
    else:
        SPEED_WINNERS[qid] = ""
        print(f"  {qid}: no correct submissions")

print(f"Speed bonus computed: {sum(1 for v in SPEED_WINNERS.values() if v)} winners\n")

# ─── Score all teams ───
results = []
for team in TEAMS:
    print(f"\n{'='*60}")
    print(f"  SCORING: {team}")
    print(f"{'='*60}")
    r = score_team(team)
    results.append(r)
    print(f"  SQL:{r['build_sql']}/3  Genie:{r['build_genie']}/5  Benchmark:{r['benchmark']}/24  Speed:{r['speed_bonus']}/8  Bonus:{r['bonus']}/8")
    print(f"  Correct: {r['correct_count']}/8  (Genie:{r['genie_correct']} SQL:{r['sql_correct']})")
    print(f"  TOTAL: {r['total']}/48")
    print()
    for d in r["details"]:
        print(f"    {d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leaderboard

# COMMAND ----------

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
# MAGIC ## Save Results

# COMMAND ----------

spark.createDataFrame(df_scores).write.format("delta").mode("overwrite").saveAsTable(
    "dataops_olympics.default.event2_scores"
)
print("Results saved to dataops_olympics.default.event2_scores")

if GENIE_RESULTS:
    genie_rows = []
    for team, results_dict in GENIE_RESULTS.items():
        for qid, info in results_dict.items():
            genie_rows.append({
                "team": team, "question_id": qid,
                "question": ANSWER_QUESTIONS.get(qid, ""),
                "genie_answer": info.get("short_answer", ""),
                "genie_sql": info.get("sql", ""),
                "expected_answer": ANSWER_KEY.get(qid, ""),
            })
    if genie_rows:
        spark.createDataFrame(pd.DataFrame(genie_rows)).write.format("delta").mode("overwrite").saveAsTable(
            "dataops_olympics.default.event2_genie_benchmark"
        )
        print("Genie details saved to dataops_olympics.default.event2_genie_benchmark")

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
_event = "Event 2: Data Analytics"

# Use MERGE to prevent duplicates (idempotent scoring)
for r in results:
    _t = r["team"]
    # Register team
    spark.sql(f"MERGE INTO {_RT} USING (SELECT '{_t}' AS team) src ON {_RT}.team = src.team WHEN NOT MATCHED THEN INSERT (team) VALUES (src.team)")

    # Merge each category score
    for cat, pts, mx in [
        ("Build_SQL", r["build_sql"], 3), ("Build_Genie", r["build_genie"], 5),
        ("Benchmark", r["benchmark"], 24), ("Speed", r["speed_bonus"], 8),
        ("Bonus", r["bonus"], 8),
    ]:
        spark.sql(f"""
            MERGE INTO {_LB} AS target
            USING (SELECT '{_t}' AS team, '{_event}' AS event, '{cat}' AS category, {pts} AS points, {mx} AS max_points, current_timestamp() AS scored_at) AS source
            ON target.team = source.team AND target.event = source.event AND target.category = source.category
            WHEN MATCHED THEN UPDATE SET points = source.points, max_points = source.max_points, scored_at = source.scored_at
            WHEN NOT MATCHED THEN INSERT (team, event, category, points, max_points, scored_at) VALUES (source.team, source.event, source.category, source.points, source.max_points, source.scored_at)
        """)

print(f"Leaderboard updated: {len(results)} teams (MERGE - no duplicates)")
