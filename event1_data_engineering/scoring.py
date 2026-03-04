# Databricks notebook source
# MAGIC %md
# MAGIC # Event 1: Automated Scoring
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY**
# MAGIC
# MAGIC Run this notebook to automatically score all teams.
# MAGIC Update the `TEAMS` list below, then **Run All**.
# MAGIC
# MAGIC ### Scoring Breakdown (50 pts max)
# MAGIC
# MAGIC | Category | SDP Path | SQL Path |
# MAGIC |----------|----------|----------|
# MAGIC | **Bronze** — table exists, row count, schema | 10 | 10 |
# MAGIC | **Silver** — cleaned, filtered, deduplicated | 15 | 8 |
# MAGIC | **Gold** — aggregation with correct columns | 15 | 8 |
# MAGIC | **Data Quality** — DQ report / expectations | 5 | 2 |
# MAGIC | **Governance** — table + column comments | 5 | 3 |
# MAGIC | **TOTAL** | **50** | **31** |

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

CATALOG = "dataops_olympics"
SCHEMA = "default"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring Engine

# COMMAND ----------

import pandas as pd

EXPECTED_BRONZE_COLS = {
    "event_id", "event_timestamp", "source_system", "patient_id",
    "age", "sex", "cp", "trestbps", "chol", "target",
}
EXPECTED_GOLD_COLS = {"age_group", "diagnosis", "patient_count"}
EXPECTED_GOLD_AGG_COLS = {"avg_cholesterol", "avg_blood_pressure", "avg_max_heart_rate"}


def _fqn(table):
    return f"{CATALOG}.{SCHEMA}.{table}"


def _table_exists(table):
    try:
        spark.table(_fqn(table))
        return True
    except Exception:
        return False


def _is_sdp_table(table):
    """Detect if a table was created by an SDP pipeline."""
    try:
        props = dict(spark.sql(f"SHOW TBLPROPERTIES {_fqn(table)}").collect())
        return any("pipeline" in str(k).lower() for k in props)
    except Exception:
        return False


def _get_table_type(table):
    """Return STREAMING_TABLE, MATERIALIZED_VIEW, MANAGED, etc."""
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {_fqn(table)}").collect()[0]
        return str(detail["format"]).upper() if detail["format"] else "DELTA"
    except Exception:
        return "UNKNOWN"


def _count_comments(table):
    """Return (has_table_comment, column_comment_count)."""
    try:
        desc = spark.sql(f"DESCRIBE TABLE EXTENDED {_fqn(table)}").collect()
        has_tbl = False
        col_count = 0
        in_detail = False
        for r in desc:
            if r[0] and r[0].strip() == "# Detailed Table Information":
                in_detail = True
                continue
            if in_detail and r[0] and r[0].strip().lower() == "comment" and r[1] and len(str(r[1]).strip()) > 3:
                has_tbl = True
            if not in_detail and r[0] and r[0].strip() not in ("", "#") and r[2] and len(str(r[2]).strip()) > 3:
                col_count += 1
        return has_tbl, col_count
    except Exception:
        return False, 0


def score_team(team_name: str) -> dict:
    """Score a single team's Event 1 submission."""
    scores = {"team": team_name, "bronze": 0, "silver": 0, "gold": 0,
              "dq": 0, "governance": 0, "total": 0, "path": "?", "details": []}

    def log(msg):
        scores["details"].append(msg)

    # ─── Detect path: SDP or SQL ───
    # SDP tables don't have team prefix; SQL tables do
    sdp_silver = _table_exists("heart_silver") and _is_sdp_table("heart_silver")
    sdp_bronze = _table_exists("heart_bronze") and _is_sdp_table("heart_bronze")
    sql_bronze = _table_exists(f"{team_name}_heart_bronze")

    is_sdp = sdp_silver or sdp_bronze
    scores["path"] = "SDP" if is_sdp else "SQL"

    bronze_name = "heart_bronze" if is_sdp else f"{team_name}_heart_bronze"
    silver_name = "heart_silver" if is_sdp else f"{team_name}_heart_silver"
    gold_name = "heart_gold" if is_sdp else f"{team_name}_heart_gold"

    # ═══════════════════════════════════════
    # BRONZE (10 pts)
    # ═══════════════════════════════════════
    if _table_exists(bronze_name):
        bronze = spark.table(_fqn(bronze_name))
        b_cnt = bronze.count()
        b_cols = set(bronze.columns)

        # Exists with rows (5 pts)
        if b_cnt >= 495:
            scores["bronze"] += 5
            log(f"Bronze: {b_cnt} rows (all batches) [+5]")
        elif b_cnt >= 400:
            scores["bronze"] += 4
            log(f"Bronze: {b_cnt} rows (most batches) [+4]")
        elif b_cnt > 0:
            scores["bronze"] += 2
            log(f"Bronze: {b_cnt} rows (partial) [+2]")
        else:
            log("Bronze: table exists but empty [+0]")

        # All 5 batches (3 pts)
        if b_cnt >= 498:
            scores["bronze"] += 3
            log("Bronze: all 5 batches ingested [+3]")
        elif b_cnt >= 300:
            scores["bronze"] += 1
            log(f"Bronze: partial batches [+1]")

        # Correct schema (2 pts)
        if EXPECTED_BRONZE_COLS.issubset(b_cols):
            scores["bronze"] += 2
            log("Bronze: schema correct [+2]")
        else:
            missing = EXPECTED_BRONZE_COLS - b_cols
            if len(missing) <= 2:
                scores["bronze"] += 1
                log(f"Bronze: mostly correct schema, missing {missing} [+1]")
            else:
                log(f"Bronze: missing columns {missing} [+0]")
    else:
        log(f"Bronze: TABLE NOT FOUND ({_fqn(bronze_name)})")

    # ═══════════════════════════════════════
    # SILVER (SDP: 15 pts / SQL: 8 pts)
    # ═══════════════════════════════════════
    if _table_exists(silver_name):
        silver = spark.table(_fqn(silver_name))
        s_cnt = silver.count()

        if is_sdp:
            # SDP pipeline detected (10 pts)
            scores["silver"] += 10
            log(f"Silver (SDP): pipeline-managed table, {s_cnt} rows [+10]")

            # Dirty rows removed (3 pts)
            if 480 <= s_cnt <= 495:
                scores["silver"] += 3
                log(f"Silver: correct row count after filtering [+3]")
            elif s_cnt < 498:
                scores["silver"] += 1
                log(f"Silver: some filtering applied [+1]")

            # Dedup check (2 pts)
            distinct_ids = silver.select("event_id").distinct().count()
            if distinct_ids == s_cnt:
                scores["silver"] += 2
                log("Silver: no duplicate event_ids [+2]")
            else:
                log(f"Silver: {s_cnt - distinct_ids} duplicates remain [+0]")
        else:
            # SQL table exists (4 pts)
            if s_cnt > 0:
                scores["silver"] += 4
                log(f"Silver (SQL): {s_cnt} rows [+4]")
            else:
                log("Silver (SQL): exists but empty [+0]")

            # Quality filters (2 pts)
            if 480 <= s_cnt <= 495:
                scores["silver"] += 2
                log("Silver: quality filters correctly applied [+2]")
            elif s_cnt < 498:
                scores["silver"] += 1
                log("Silver: some filtering applied [+1]")

            # Dedup (2 pts)
            distinct_ids = silver.select("event_id").distinct().count()
            if distinct_ids == s_cnt:
                scores["silver"] += 2
                log("Silver: deduplicated [+2]")
            else:
                log(f"Silver: {s_cnt - distinct_ids} duplicates remain [+0]")

        # Check for ingested_at column (bonus validation)
        if "ingested_at" in set(silver.columns):
            log("Silver: ingested_at column present [info]")
    else:
        log(f"Silver: TABLE NOT FOUND ({_fqn(silver_name)})")

    # ═══════════════════════════════════════
    # GOLD (SDP: 15 pts / SQL: 8 pts)
    # ═══════════════════════════════════════
    if _table_exists(gold_name):
        gold = spark.table(_fqn(gold_name))
        g_cnt = gold.count()
        g_cols = set(c.lower() for c in gold.columns)

        if is_sdp:
            # MV or table exists (7 pts)
            if g_cnt > 0:
                scores["gold"] += 7
                log(f"Gold (SDP): {g_cnt} rows [+7]")
            else:
                log("Gold (SDP): exists but empty [+0]")

            # Correct aggregation columns (5 pts)
            has_expected = EXPECTED_GOLD_COLS.issubset(g_cols)
            has_agg = EXPECTED_GOLD_AGG_COLS.issubset(g_cols)
            if has_expected and has_agg:
                scores["gold"] += 5
                log("Gold: all expected columns present [+5]")
            elif has_expected or any("avg" in c for c in g_cols):
                scores["gold"] += 3
                log("Gold: some aggregation columns [+3]")

            # Governance in pipeline code (3 pts)
            has_tbl_comment, _ = _count_comments(gold_name)
            if has_tbl_comment:
                scores["gold"] += 3
                log("Gold: table comment from pipeline code [+3]")
        else:
            # Table exists (4 pts)
            if g_cnt > 0:
                scores["gold"] += 4
                log(f"Gold (SQL): {g_cnt} rows [+4]")

            # Correct columns (2 pts)
            has_expected = EXPECTED_GOLD_COLS.issubset(g_cols)
            has_agg = any("avg" in c for c in g_cols) or "patient_count" in g_cols
            if has_expected and has_agg:
                scores["gold"] += 2
                log("Gold: correct columns [+2]")
            elif has_agg:
                scores["gold"] += 1
                log("Gold: partial columns [+1]")

            # Governance (2 pts)
            has_tbl_comment, _ = _count_comments(gold_name)
            if has_tbl_comment:
                scores["gold"] += 2
                log("Gold: governance comment [+2]")

        # Validate aggregation logic
        if g_cnt > 0 and "age_group" in g_cols:
            age_groups = set(gold.select("age_group").distinct().toPandas()["age_group"])
            expected_groups = {"Under 40", "40-49", "50-59", "60+"}
            if age_groups == expected_groups:
                log("Gold: age groups correct [info]")
            else:
                log(f"Gold: age groups = {age_groups} (expected {expected_groups}) [info]")
    else:
        log(f"Gold: TABLE NOT FOUND ({_fqn(gold_name)})")

    # ═══════════════════════════════════════
    # DATA QUALITY (SDP: 5 pts / SQL: 2 pts)
    # ═══════════════════════════════════════
    if is_sdp:
        scores["dq"] += 5
        log("DQ: SDP expectations provide automatic DQ tracking [+5]")
    else:
        if _table_exists(bronze_name) and _table_exists(silver_name):
            try:
                b_cnt = spark.table(_fqn(bronze_name)).count()
                s_cnt = spark.table(_fqn(silver_name)).count()
                if s_cnt < b_cnt:
                    scores["dq"] += 2
                    log(f"DQ: filtering evidence (Bronze {b_cnt} → Silver {s_cnt}) [+2]")
                else:
                    log("DQ: no filtering evidence [+0]")
            except Exception:
                log("DQ: cannot verify [+0]")
        else:
            log("DQ: tables missing [+0]")

    # ═══════════════════════════════════════
    # GOVERNANCE (SDP: 5 pts / SQL: 3 pts)
    # ═══════════════════════════════════════
    best_tbl_comment = False
    best_col_count = 0

    for tbl in [silver_name, bronze_name, gold_name]:
        if _table_exists(tbl):
            has_tc, cc = _count_comments(tbl)
            if has_tc:
                best_tbl_comment = True
            best_col_count = max(best_col_count, cc)

    if is_sdp:
        if best_tbl_comment:
            scores["governance"] += 2
            log("Governance: table comment found [+2]")
        if best_col_count >= 3:
            scores["governance"] += 3
            log(f"Governance: {best_col_count} column comments [+3]")
        elif best_col_count > 0:
            scores["governance"] += best_col_count
            log(f"Governance: {best_col_count} column comments [+{best_col_count}]")
    else:
        if best_tbl_comment:
            scores["governance"] += 1
            log("Governance: table comment found [+1]")
        if best_col_count >= 3:
            scores["governance"] += 2
            log(f"Governance: {best_col_count} column comments [+2]")
        elif best_col_count > 0:
            scores["governance"] += 1
            log(f"Governance: {best_col_count} column comments [+1]")

    scores["total"] = scores["bronze"] + scores["silver"] + scores["gold"] + scores["dq"] + scores["governance"]
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
    print(f"  Path: {r['path']}  |  TOTAL: {r['total']}/50")
    print(f"  Bronze: {r['bronze']}/10  Silver: {r['silver']}/15  Gold: {r['gold']}/15  DQ: {r['dq']}/5  Gov: {r['governance']}/5")
    print()
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

print("=" * 72)
print("  EVENT 1: DATA ENGINEERING — FINAL LEADERBOARD")
print("=" * 72)
for rank, (_, row) in enumerate(df_scores.iterrows(), 1):
    medal = {1: "[GOLD]  ", 2: "[SILVER]", 3: "[BRONZE]"}.get(rank, "        ")
    print(f"  {rank}. {medal} {row['Team']:12s} | {row['Path']:3s} | {int(row['Total']):2d}/50 pts")
print("=" * 72)

# COMMAND ----------

fig = px.bar(
    df_scores.sort_values("Total"),
    x="Total", y="Team", color="Path",
    orientation="h",
    title="Event 1: Data Engineering — Final Scores",
    text="Total",
    color_discrete_map={"SDP": "#2ecc71", "SQL": "#3498db", "?": "#95a5a6"},
)
fig.update_layout(template="plotly_white", yaxis=dict(categoryorder="total ascending"))
fig.show()

# COMMAND ----------

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
    color_discrete_map={
        "Bronze": "#cd7f32", "Silver": "#c0c0c0", "Gold": "#ffd700",
        "DQ": "#3498db", "Governance": "#9b59b6",
    },
)
fig2.update_layout(template="plotly_white", yaxis=dict(categoryorder="total ascending"))
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed Results Table

# COMMAND ----------

display(spark.createDataFrame(df_scores))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

spark.createDataFrame(df_scores).write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA}.event1_scores"
)
print(f"Results saved to {CATALOG}.{SCHEMA}.event1_scores")
