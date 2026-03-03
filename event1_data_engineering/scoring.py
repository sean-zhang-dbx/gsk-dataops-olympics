# Databricks notebook source
# MAGIC %md
# MAGIC # Event 1: Automated Scoring
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY**
# MAGIC
# MAGIC Run this notebook to score all teams. Set the team names below and run all cells.
# MAGIC
# MAGIC ### Scoring Breakdown (50 pts max)
# MAGIC
# MAGIC ```
# MAGIC BRONZE (10 pts)
# MAGIC   Table exists with ~500 rows .............. 5 pts
# MAGIC   All 5 batches ingested (500 records) ...... 3 pts
# MAGIC   Correct schema (event_id, age, etc.) ...... 2 pts
# MAGIC
# MAGIC SILVER (15 pts — varies by path)
# MAGIC   SDP: Pipeline exists + expectations ........ 10 pts
# MAGIC   SDP: Dirty rows removed .................... 3 pts
# MAGIC   SDP: Deduplication on event_id ............. 2 pts
# MAGIC   --- OR ---
# MAGIC   SQL: Table exists with cleaned rows ........ 4 pts
# MAGIC   SQL: Quality filters applied ............... 2 pts
# MAGIC   SQL: Deduplication on event_id ............. 2 pts
# MAGIC
# MAGIC GOLD (15 pts — varies by path)
# MAGIC   SDP: Materialized view exists .............. 7 pts
# MAGIC   SDP: Correct aggregation ................... 5 pts
# MAGIC   SDP: Governance in pipeline code ........... 3 pts
# MAGIC   --- OR ---
# MAGIC   SQL: Table exists with aggregation ......... 4 pts
# MAGIC   SQL: Correct columns/logic ................. 2 pts
# MAGIC   SQL: Governance comments added ............. 2 pts
# MAGIC
# MAGIC DATA QUALITY REPORT (5 pts)
# MAGIC   DQ metrics computed/visible ................ 5 pts
# MAGIC
# MAGIC GOVERNANCE (5 pts)
# MAGIC   Table comment on 1+ tables ................. 2 pts
# MAGIC   Column comments on 3+ columns .............. 3 pts
# MAGIC ============================================
# MAGIC SDP MAX: 50 pts | SQL MAX: 31 pts
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dataops_olympics;
# MAGIC USE SCHEMA default;

# COMMAND ----------

TEAMS = [
    "team_01",
    "team_02",
    "team_03",
    "team_04",
    "team_05",
    "team_06",
    "team_07",
    "team_08",
    "team_09",
    "team_10",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring Engine

# COMMAND ----------

import pandas as pd

EXPECTED_BRONZE_COLS = {"event_id", "event_timestamp", "source_system", "patient_id",
                        "age", "sex", "cp", "trestbps", "chol", "target"}

EXPECTED_GOLD_COLS_PARTIAL = {"age_group", "patient_count"}

def score_team(team_name: str) -> dict:
    """Score a single team. Returns dict with category scores and details."""
    result = {"team": team_name, "bronze": 0, "silver": 0, "gold": 0,
              "dq": 0, "governance": 0, "total": 0, "path": "unknown", "details": []}

    # --- BRONZE ---
    try:
        bronze = spark.table(f"dataops_olympics.default.{team_name}_heart_bronze")
        b_cnt = bronze.count()
        b_cols = set(bronze.columns)

        if b_cnt >= 490:
            result["bronze"] += 5
            result["details"].append(f"Bronze: {b_cnt} rows [+5]")
        elif b_cnt > 0:
            result["bronze"] += 2
            result["details"].append(f"Bronze: {b_cnt} rows (low) [+2]")
        else:
            result["details"].append("Bronze: empty [+0]")

        if b_cnt >= 498:
            result["bronze"] += 3
            result["details"].append("Bronze: all batches ingested [+3]")
        elif b_cnt >= 300:
            result["bronze"] += 1
            result["details"].append(f"Bronze: partial batches ({b_cnt}) [+1]")

        if EXPECTED_BRONZE_COLS.issubset(b_cols):
            result["bronze"] += 2
            result["details"].append("Bronze: schema correct [+2]")
        else:
            missing = EXPECTED_BRONZE_COLS - b_cols
            result["details"].append(f"Bronze: missing columns {missing} [+0]")
    except Exception as e:
        result["details"].append(f"Bronze: TABLE NOT FOUND [{e}]")

    # --- DETECT SDP vs SQL ---
    is_sdp = False
    try:
        props = {r[0]: r[1] for r in
                 spark.sql(f"SHOW TBLPROPERTIES dataops_olympics.default.{team_name}_heart_silver").collect()
                 if r[0] and r[1]}
        if any("pipeline" in k.lower() for k in props.keys()):
            is_sdp = True
    except Exception:
        pass

    # Also check for pipeline-managed tables without team prefix
    if not is_sdp:
        try:
            props = {r[0]: r[1] for r in
                     spark.sql("SHOW TBLPROPERTIES dataops_olympics.default.heart_silver").collect()
                     if r[0] and r[1]}
            if any("pipeline" in k.lower() for k in props.keys()):
                is_sdp = True
        except Exception:
            pass

    result["path"] = "SDP" if is_sdp else "SQL"

    # --- SILVER ---
    silver_table = f"{team_name}_heart_silver" if not is_sdp else "heart_silver"
    try:
        silver_full = f"dataops_olympics.default.{silver_table}"
        silver = spark.table(silver_full)
        s_cnt = silver.count()

        if is_sdp:
            result["silver"] += 10
            result["details"].append(f"Silver (SDP pipeline detected): {s_cnt} rows [+10]")

            if s_cnt < 490:
                result["silver"] += 3
                result["details"].append(f"Silver: dirty rows removed ({500 - s_cnt} filtered) [+3]")

            dedup_check = silver.select("event_id").distinct().count()
            if dedup_check == s_cnt:
                result["silver"] += 2
                result["details"].append("Silver: no duplicate event_ids [+2]")
        else:
            if s_cnt > 0:
                result["silver"] += 4
                result["details"].append(f"Silver (SQL): {s_cnt} rows [+4]")

            if s_cnt < 490:
                result["silver"] += 2
                result["details"].append(f"Silver: quality filters applied ({500 - s_cnt} filtered) [+2]")

            dedup_check = silver.select("event_id").distinct().count()
            if dedup_check == s_cnt:
                result["silver"] += 2
                result["details"].append("Silver: deduplicated [+2]")
    except Exception as e:
        result["details"].append(f"Silver: TABLE NOT FOUND [{e}]")

    # --- GOLD ---
    gold_table = f"{team_name}_heart_gold" if not is_sdp else "heart_gold"
    try:
        gold_full = f"dataops_olympics.default.{gold_table}"
        gold = spark.table(gold_full)
        g_cnt = gold.count()
        g_cols = set(c.lower() for c in gold.columns)

        if is_sdp:
            # Check if it's a materialized view
            try:
                detail = spark.sql(f"DESCRIBE DETAIL {gold_full}").collect()[0]
                is_mv = "MATERIALIZED_VIEW" in str(detail).upper()
            except Exception:
                is_mv = False

            if is_mv or g_cnt > 0:
                result["gold"] += 7
                result["details"].append(f"Gold (SDP MV): {g_cnt} rows [+7]")

            has_agg = "patient_count" in g_cols or any("avg" in c for c in g_cols)
            if has_agg:
                result["gold"] += 5
                result["details"].append("Gold: correct aggregation columns [+5]")

            try:
                props = {r[0]: r[1] for r in spark.sql(f"SHOW TBLPROPERTIES {gold_full}").collect()}
                has_pipe_comment = any("comment" in str(v).lower() for v in props.values() if v and len(str(v)) > 5)
                if has_pipe_comment:
                    result["gold"] += 3
                    result["details"].append("Gold: governance in pipeline code [+3]")
            except Exception:
                pass
        else:
            if g_cnt > 0:
                result["gold"] += 4
                result["details"].append(f"Gold (SQL table): {g_cnt} rows [+4]")

            has_agg = "patient_count" in g_cols or any("avg" in c for c in g_cols)
            if has_agg:
                result["gold"] += 2
                result["details"].append("Gold: aggregation columns present [+2]")

            try:
                desc = spark.sql(f"DESCRIBE TABLE EXTENDED {gold_full}").collect()
                has_comment = any("comment" in str(r).lower() and r[1] and len(str(r[1])) > 5 for r in desc)
                if has_comment:
                    result["gold"] += 2
                    result["details"].append("Gold: governance comment [+2]")
            except Exception:
                pass
    except Exception as e:
        result["details"].append(f"Gold: TABLE NOT FOUND [{e}]")

    # --- DATA QUALITY REPORT ---
    # Award DQ points if Bronze exists (they can produce a DQ report)
    try:
        spark.table(f"dataops_olympics.default.{team_name}_heart_bronze")
        result["dq"] += 5
        result["details"].append("DQ: Bronze exists — DQ report can be produced [+5]")
    except Exception:
        result["details"].append("DQ: No bronze table — cannot produce DQ report [+0]")

    # --- GOVERNANCE ---
    check_tables = [f"{team_name}_heart_silver", f"{team_name}_heart_bronze"]
    if is_sdp:
        check_tables = ["heart_silver", "heart_bronze"] + check_tables

    gov_tbl_comment = False
    gov_col_comments = 0
    for tbl in check_tables:
        try:
            desc = spark.sql(f"DESCRIBE TABLE EXTENDED dataops_olympics.default.{tbl}").collect()
            if not gov_tbl_comment:
                has_tc = any("comment" in str(r).lower() and r[1] and len(str(r[1])) > 5
                             for r in desc if r[0] and r[0].strip() != "")
                if has_tc:
                    gov_tbl_comment = True
            cc = sum(1 for r in desc if r[2] and len(str(r[2])) > 5 and r[0] not in ["", "#", "# Detailed Table"])
            gov_col_comments = max(gov_col_comments, cc)
        except Exception:
            continue

    if gov_tbl_comment:
        result["governance"] += 2
        result["details"].append("Governance: table comment found [+2]")
    if gov_col_comments >= 3:
        result["governance"] += 3
        result["details"].append(f"Governance: {gov_col_comments} column comments [+3]")
    elif gov_col_comments > 0:
        result["governance"] += gov_col_comments
        result["details"].append(f"Governance: {gov_col_comments} column comments [+{gov_col_comments}]")

    result["total"] = result["bronze"] + result["silver"] + result["gold"] + result["dq"] + result["governance"]
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Scoring

# COMMAND ----------

results = []
for team in TEAMS:
    print(f"\nScoring {team}...")
    r = score_team(team)
    results.append(r)
    print(f"  Path: {r['path']} | Score: {r['total']}/50")
    for d in r["details"]:
        print(f"    {d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leaderboard

# COMMAND ----------

import plotly.express as px

df_scores = pd.DataFrame([{
    "Team": r["team"],
    "Path": r["path"],
    "Bronze": r["bronze"],
    "Silver": r["silver"],
    "Gold": r["gold"],
    "DQ": r["dq"],
    "Governance": r["governance"],
    "Total": r["total"],
} for r in results]).sort_values("Total", ascending=False)

print("=" * 70)
print("  EVENT 1: DATA ENGINEERING — FINAL LEADERBOARD")
print("=" * 70)
print()
print(df_scores.to_string(index=False))
print()
print("=" * 70)

# COMMAND ----------

fig = px.bar(
    df_scores.sort_values("Total"),
    x="Total", y="Team", color="Path",
    orientation="h",
    title="Event 1: Data Engineering Scores",
    text="Total",
    color_discrete_map={"SDP": "#2ecc71", "SQL": "#3498db", "unknown": "#95a5a6"},
)
fig.update_layout(template="plotly_white", yaxis=dict(categoryorder="total ascending"))
fig.show()

# COMMAND ----------

# Stacked breakdown
df_melt = df_scores.melt(
    id_vars=["Team", "Path", "Total"],
    value_vars=["Bronze", "Silver", "Gold", "DQ", "Governance"],
    var_name="Category", value_name="Points"
)

fig2 = px.bar(
    df_melt.sort_values("Total"),
    x="Points", y="Team", color="Category",
    orientation="h",
    title="Score Breakdown by Category",
    barmode="stack",
)
fig2.update_layout(template="plotly_white", yaxis=dict(categoryorder="total ascending"))
fig2.show()
