# Databricks notebook source
# MAGIC %md
# MAGIC # Event 2: Scoring — Option A (Submissions Table + Manual Verification)
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY**
# MAGIC
# MAGIC ### How It Works
# MAGIC
# MAGIC 1. **SQL answers** — read from each team's `event2_submissions` table (teams use `submit_answer()`)
# MAGIC 2. **Genie answers** — teams submit via `submit_answer()` with method="Genie" **AND post a screenshot in MS Teams** as proof
# MAGIC 3. **Answer checking** — deterministic fuzzy numeric match (single-value answers only)
# MAGIC 4. **Speed bonus** — auto-computed from submission timestamps
# MAGIC 5. **Bonus challenges** — auto-detected from team catalog tables
# MAGIC
# MAGIC ### Genie Verification Rule
# MAGIC
# MAGIC If a team submits with method="GENIE" but **no screenshot was posted in MS Teams**,
# MAGIC the organizer should override that submission to method="SQL" (2 pts instead of 3 pts).
# MAGIC Use the `override_method()` function below.
# MAGIC
# MAGIC ### Scoring Breakdown
# MAGIC
# MAGIC | Category | Points |
# MAGIC |----------|--------|
# MAGIC | **Build: SQL** (3 practice queries executed) | 3 pts |
# MAGIC | **Build: Genie** (space created and tested — verified by organizer) | 5 pts |
# MAGIC | **Benchmark** (8 Qs: 2 pts SQL / 3 pts Genie with screenshot proof) | 16-24 pts |
# MAGIC | **Speed bonus** (first correct per Q, auto from timestamps) | up to 8 pts |
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
# MAGIC ### Genie Build Verification
# MAGIC
# MAGIC Mark teams that created a working Genie space (verified by organizer walking around).

# COMMAND ----------

GENIE_VERIFIED_TEAMS = [
    # "team_01",  # Add teams whose Genie space you personally verified
]

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
# MAGIC ### Genie Method Override
# MAGIC
# MAGIC If a team claimed "Genie" but **did NOT post a screenshot in MS Teams**,
# MAGIC override their method to "SQL" here. This downgrades them from 3 pts to 2 pts.

# COMMAND ----------

def override_method(team: str, question_id: str, new_method: str = "SQL"):
    """Override a team's submission method (e.g. Genie -> SQL if no screenshot proof)."""
    qid = question_id.upper()
    new_method = new_method.upper()
    if team in SQL_SUBMISSIONS and qid in SQL_SUBMISSIONS[team]:
        old = SQL_SUBMISSIONS[team][qid]["method"]
        SQL_SUBMISSIONS[team][qid]["method"] = new_method
        print(f"  Override: {team} {qid} method changed {old} -> {new_method}")
    else:
        print(f"  No submission found for {team} {qid}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Answer Checking (Deterministic)
# MAGIC
# MAGIC All answers are single values. Uses fuzzy numeric matching and substring containment.

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


def check_answer(team_answer, correct_answer, question):
    """Check if team_answer matches correct_answer. All answers are single values."""
    ta = _normalize(team_answer)
    ca = _normalize(correct_answer)

    if not ta or ta.startswith("error") or ta.startswith("failed"):
        return False

    if ta == ca:
        return True

    ta_nums = [float(x) for x in ta.split() if _is_number(x)]
    ca_nums = [float(x) for x in ca.split() if _is_number(x)]
    if ta_nums and ca_nums:
        if any(abs(t - ca_nums[0]) <= 0.5 for t in ta_nums):
            return True
        return False

    if ca in ta or ta in ca:
        return True

    ca_parts = [p.strip() for p in ca.split() if len(p.strip()) > 1]
    if ca_parts and all(p in ta for p in ca_parts):
        return True

    return False

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Compute Speed Bonus from Timestamps

# COMMAND ----------

print("Computing speed bonus from submission timestamps...")
SPEED_WINNERS = {}

all_correct = []
for team in TEAMS:
    subs = SQL_SUBMISSIONS.get(team, {})
    for qid, sub in subs.items():
        correct = ANSWER_KEY.get(qid, "")
        if correct and sub.get("answer"):
            is_correct = check_answer(sub["answer"], correct, ANSWER_QUESTIONS.get(qid, ""))
            if is_correct:
                all_correct.append((team, qid, sub.get("submitted_at", "")))

from collections import defaultdict
earliest = defaultdict(lambda: ("", "9999-99-99"))
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

print(f"Speed bonus computed: {sum(1 for v in SPEED_WINNERS.values() if v)} winners")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Score All Teams

# COMMAND ----------

def score_team(team_name: str) -> dict:
    subs = SQL_SUBMISSIONS.get(team_name, {})

    scores = {
        "team": team_name,
        "build_sql": 0, "build_genie": 0, "benchmark": 0,
        "speed_bonus": 0, "bonus": 0, "total": 0,
        "correct_count": 0, "genie_correct": 0, "sql_correct": 0,
        "details": [],
    }

    def log(msg):
        scores["details"].append(msg)

    # ─── BUILD: SQL (3 pts) — at least 3 answers submitted means they practiced ───
    submission_count = len(subs)
    scores["build_sql"] = min(submission_count, 3)
    if submission_count > 0:
        log(f"Build SQL: {submission_count} answers submitted [+{min(submission_count, 3)}]")
    else:
        log("Build SQL: no submissions found [+0]")

    # ─── BUILD: GENIE (5 pts) — organizer-verified ───
    if team_name in GENIE_VERIFIED_TEAMS:
        scores["build_genie"] = 5
        log("Build Genie: space verified by organizer [+5]")
    else:
        log("Build Genie: not verified [+0]")

    # ─── BENCHMARK (8 questions, single-value answers) ───
    for qid in sorted(ANSWER_KEY.keys()):
        correct = ANSWER_KEY[qid]
        question = ANSWER_QUESTIONS[qid]

        sub = subs.get(qid, {})
        team_answer = sub.get("answer", "")
        method = sub.get("method", "SQL")

        if not team_answer:
            log(f"{qid}: NO ANSWER [+0]")
            continue

        is_correct = check_answer(team_answer, correct, question)

        if is_correct:
            pts = 3 if method == "GENIE" else 2
            scores["benchmark"] += pts
            scores["correct_count"] += 1
            if method == "GENIE":
                scores["genie_correct"] += 1
            else:
                scores["sql_correct"] += 1
            log(f"{qid}: CORRECT via {method} [+{pts}] (submitted: {team_answer})")
        else:
            log(f"{qid}: WRONG [+0] (submitted: {team_answer} | expected: {correct})")

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

    scores["total"] = (
        scores["build_sql"] + scores["build_genie"] + scores["benchmark"]
        + scores["speed_bonus"] + scores["bonus"]
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

for r in results:
    _t = r["team"]
    spark.sql(f"MERGE INTO {_RT} USING (SELECT '{_t}' AS team) src ON {_RT}.team = src.team WHEN NOT MATCHED THEN INSERT (team) VALUES (src.team)")

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
