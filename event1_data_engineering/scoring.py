# Databricks notebook source
# MAGIC %md
# MAGIC # Event 1: Automated Scoring
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY**
# MAGIC
# MAGIC Run this notebook to automatically score all teams.
# MAGIC Update the `TEAMS` list below, then **Run All**.
# MAGIC
# MAGIC Each team name **is** the catalog name (e.g., `team_01` → catalog `team_01`).
# MAGIC Tables are always `heart_bronze`, `heart_silver`, `heart_gold` — no prefixes.
# MAGIC
# MAGIC ### Scoring Breakdown (50 pts max + 5 bonus)
# MAGIC
# MAGIC | Category | SDP Path | SQL Path |
# MAGIC |----------|----------|----------|
# MAGIC | **Bronze** — table exists, row count, schema | 10 | 10 |
# MAGIC | **Silver** — cleaned, filtered, deduplicated | 15 | 8 |
# MAGIC | **Gold** — aggregation with correct columns | 15 | 8 |
# MAGIC | **Data Quality** — DQ report / expectations | 5 | 2 |
# MAGIC | **Governance** — table + column comments | 5 | 3 |
# MAGIC | **TOTAL** | **50** | **31** |
# MAGIC | --- | --- | --- |
# MAGIC | **Bonus: UC Tags** — 3+ tags across tables | +3 | +3 |
# MAGIC | **Bonus: AI Functions** → `heart_gold_ai` with `cardiovascular_risk` col | +2 | +2 |

# COMMAND ----------

TEAMS = [
    "team_01",
    "team_02",
    "team_03",
    "team_04",
]

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


def _fqn(catalog, table):
    return f"{catalog}.{SCHEMA}.{table}"


def _table_exists(catalog, table):
    try:
        spark.table(_fqn(catalog, table))
        return True
    except Exception:
        return False


def _is_sdp_table(catalog, table):
    """Detect if a table was created by an SDP pipeline."""
    try:
        rows = spark.sql(f"SHOW TBLPROPERTIES {_fqn(catalog, table)}").collect()
        props = {str(r[0]).lower(): str(r[1]) for r in rows}
        return any("pipeline" in k for k in props)
    except Exception:
        return False


def _count_comments(catalog, table):
    """Return (has_table_comment, column_comment_count)."""
    try:
        desc = spark.sql(f"DESCRIBE TABLE EXTENDED {_fqn(catalog, table)}").collect()
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


def _count_uc_tags(catalog):
    """Count Unity Catalog tags across all heart_* tables in the team catalog."""
    try:
        rows = spark.sql(f"""
            SELECT COUNT(*) AS cnt FROM system.information_schema.table_tags
            WHERE catalog_name = '{catalog}' AND schema_name = '{SCHEMA}'
              AND table_name IN ('heart_bronze', 'heart_silver', 'heart_gold')
        """).collect()
        return rows[0][0] if rows else 0
    except Exception:
        return 0


def _check_ai_functions(catalog):
    """Check for heart_gold_ai table with cardiovascular_risk column. Returns (exists, has_column, row_count)."""
    try:
        if not _table_exists(catalog, "heart_gold_ai"):
            return False, False, 0
        df = spark.table(_fqn(catalog, "heart_gold_ai"))
        cols = set(c.lower() for c in df.columns)
        has_col = "cardiovascular_risk" in cols
        return True, has_col, df.count()
    except Exception:
        return False, False, 0


def score_team(team_name: str) -> dict:
    """Score a single team's Event 1 submission. Catalog = team_name."""
    catalog = team_name
    scores = {
        "team": team_name, "bronze": 0, "silver": 0, "gold": 0,
        "dq": 0, "governance": 0, "bonus": 0, "total": 0,
        "path": "?", "details": [],
    }

    def log(msg):
        scores["details"].append(msg)

    # ─── Detect path: SDP or SQL ───
    is_sdp = (
        _table_exists(catalog, "heart_bronze") and _is_sdp_table(catalog, "heart_bronze")
    ) or (
        _table_exists(catalog, "heart_silver") and _is_sdp_table(catalog, "heart_silver")
    )
    scores["path"] = "SDP" if is_sdp else "SQL"

    # ═══════════════════════════════════════
    # BRONZE (10 pts)
    # ═══════════════════════════════════════
    if _table_exists(catalog, "heart_bronze"):
        bronze = spark.table(_fqn(catalog, "heart_bronze"))
        b_cnt = bronze.count()
        b_cols = set(bronze.columns)

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

        if b_cnt >= 498:
            scores["bronze"] += 3
            log("Bronze: all 5 batches ingested [+3]")
        elif b_cnt >= 300:
            scores["bronze"] += 1
            log(f"Bronze: partial batches [+1]")

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
        log(f"Bronze: TABLE NOT FOUND ({_fqn(catalog, 'heart_bronze')})")

    # ═══════════════════════════════════════
    # SILVER (SDP: 15 pts / SQL: 8 pts)
    # ═══════════════════════════════════════
    if _table_exists(catalog, "heart_silver"):
        silver = spark.table(_fqn(catalog, "heart_silver"))
        s_cnt = silver.count()

        if is_sdp:
            scores["silver"] += 10
            log(f"Silver (SDP): pipeline-managed table, {s_cnt} rows [+10]")

            if 480 <= s_cnt <= 495:
                scores["silver"] += 3
                log(f"Silver: correct row count after filtering [+3]")
            elif s_cnt < 498:
                scores["silver"] += 1
                log(f"Silver: some filtering applied [+1]")

            distinct_ids = silver.select("event_id").distinct().count()
            if distinct_ids == s_cnt:
                scores["silver"] += 2
                log("Silver: no duplicate event_ids [+2]")
            else:
                log(f"Silver: {s_cnt - distinct_ids} duplicates remain [+0]")
        else:
            if s_cnt > 0:
                scores["silver"] += 4
                log(f"Silver (SQL): {s_cnt} rows [+4]")
            else:
                log("Silver (SQL): exists but empty [+0]")

            if 480 <= s_cnt <= 495:
                scores["silver"] += 2
                log("Silver: quality filters correctly applied [+2]")
            elif s_cnt < 498:
                scores["silver"] += 1
                log("Silver: some filtering applied [+1]")

            distinct_ids = silver.select("event_id").distinct().count()
            if distinct_ids == s_cnt:
                scores["silver"] += 2
                log("Silver: deduplicated [+2]")
            else:
                log(f"Silver: {s_cnt - distinct_ids} duplicates remain [+0]")

        if "ingested_at" in set(silver.columns):
            log("Silver: ingested_at column present [info]")
    else:
        log(f"Silver: TABLE NOT FOUND ({_fqn(catalog, 'heart_silver')})")

    # ═══════════════════════════════════════
    # GOLD (SDP: 15 pts / SQL: 8 pts)
    # ═══════════════════════════════════════
    if _table_exists(catalog, "heart_gold"):
        gold = spark.table(_fqn(catalog, "heart_gold"))
        g_cnt = gold.count()
        g_cols = set(c.lower() for c in gold.columns)

        if is_sdp:
            if g_cnt > 0:
                scores["gold"] += 7
                log(f"Gold (SDP): {g_cnt} rows [+7]")
            else:
                log("Gold (SDP): exists but empty [+0]")

            has_expected = EXPECTED_GOLD_COLS.issubset(g_cols)
            has_agg = EXPECTED_GOLD_AGG_COLS.issubset(g_cols)
            if has_expected and has_agg:
                scores["gold"] += 5
                log("Gold: all expected columns present [+5]")
            elif has_expected or any("avg" in c for c in g_cols):
                scores["gold"] += 3
                log("Gold: some aggregation columns [+3]")

            has_tbl_comment, _ = _count_comments(catalog, "heart_gold")
            if has_tbl_comment:
                scores["gold"] += 3
                log("Gold: table comment from pipeline code [+3]")
        else:
            if g_cnt > 0:
                scores["gold"] += 4
                log(f"Gold (SQL): {g_cnt} rows [+4]")

            has_expected = EXPECTED_GOLD_COLS.issubset(g_cols)
            has_agg = any("avg" in c for c in g_cols) or "patient_count" in g_cols
            if has_expected and has_agg:
                scores["gold"] += 2
                log("Gold: correct columns [+2]")
            elif has_agg:
                scores["gold"] += 1
                log("Gold: partial columns [+1]")

            has_tbl_comment, _ = _count_comments(catalog, "heart_gold")
            if has_tbl_comment:
                scores["gold"] += 2
                log("Gold: governance comment [+2]")

        if g_cnt > 0 and "age_group" in g_cols:
            age_groups = set(gold.select("age_group").distinct().toPandas()["age_group"])
            expected_groups = {"Under 40", "40-49", "50-59", "60+"}
            if age_groups == expected_groups:
                log("Gold: age groups correct [info]")
            else:
                log(f"Gold: age groups = {age_groups} (expected {expected_groups}) [info]")
    else:
        log(f"Gold: TABLE NOT FOUND ({_fqn(catalog, 'heart_gold')})")

    # ═══════════════════════════════════════
    # DATA QUALITY (SDP: 5 pts / SQL: 2 pts)
    # ═══════════════════════════════════════
    if is_sdp:
        scores["dq"] += 5
        log("DQ: SDP expectations provide automatic DQ tracking [+5]")
    else:
        if _table_exists(catalog, "heart_bronze") and _table_exists(catalog, "heart_silver"):
            try:
                b_cnt = spark.table(_fqn(catalog, "heart_bronze")).count()
                s_cnt = spark.table(_fqn(catalog, "heart_silver")).count()
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

    for tbl in ["heart_silver", "heart_bronze", "heart_gold"]:
        if _table_exists(catalog, tbl):
            has_tc, cc = _count_comments(catalog, tbl)
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

    # ═══════════════════════════════════════
    # BONUS (+5 max)
    # ═══════════════════════════════════════

    # UC Tags (+3): check for tags across heart_* tables
    tag_count = _count_uc_tags(catalog)
    if tag_count >= 3:
        scores["bonus"] += 3
        log(f"Bonus: {tag_count} UC Tags across tables [+3]")
    elif tag_count > 0:
        scores["bonus"] += tag_count
        log(f"Bonus: {tag_count} UC Tags (need 3+ for full credit) [+{tag_count}]")

    # AI Functions (+2): check heart_gold_ai table with cardiovascular_risk column
    ai_exists, ai_has_col, ai_rows = _check_ai_functions(catalog)
    if ai_exists and ai_has_col and ai_rows > 0:
        scores["bonus"] += 2
        log(f"Bonus: heart_gold_ai with cardiovascular_risk column, {ai_rows} rows [+2]")
    elif ai_exists and ai_rows > 0:
        scores["bonus"] += 1
        log(f"Bonus: heart_gold_ai exists ({ai_rows} rows) but missing cardiovascular_risk column [+1]")
    elif ai_exists:
        log("Bonus: heart_gold_ai exists but is empty [+0]")

    scores["total"] = (
        scores["bronze"] + scores["silver"] + scores["gold"]
        + scores["dq"] + scores["governance"] + scores["bonus"]
    )
    return scores

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Scoring — All Teams

# COMMAND ----------

results = []
for team in TEAMS:
    print(f"\n{'='*60}")
    print(f"  SCORING: {team}  (catalog: {team}.{SCHEMA})")
    print(f"{'='*60}")
    r = score_team(team)
    results.append(r)
    print(f"  Path: {r['path']}  |  TOTAL: {r['total']}/50 (+{r['bonus']} bonus)")
    print(f"  Bronze: {r['bronze']}/10  Silver: {r['silver']}/15  Gold: {r['gold']}/15  DQ: {r['dq']}/5  Gov: {r['governance']}/5  Bonus: {r['bonus']}/5")
    print()
    for d in r["details"]:
        print(f"    {d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leaderboard

# COMMAND ----------

df_scores = pd.DataFrame([{
    "Team": r["team"],
    "Path": r["path"],
    "Bronze": r["bronze"],
    "Silver": r["silver"],
    "Gold": r["gold"],
    "DQ": r["dq"],
    "Governance": r["governance"],
    "Bonus": r["bonus"],
    "Total": r["total"],
} for r in results]).sort_values("Total", ascending=False)

print("=" * 72)
print("  EVENT 1: DATA ENGINEERING — FINAL LEADERBOARD")
print("=" * 72)
for rank, (_, row) in enumerate(df_scores.iterrows(), 1):
    medal = {1: "[GOLD]  ", 2: "[SILVER]", 3: "[BRONZE]"}.get(rank, "        ")
    print(f"  {rank}. {medal} {row['Team']:12s} | {row['Path']:3s} | {int(row['Total']):2d}/55 pts")
print("=" * 72)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed Results Table

# COMMAND ----------

display(spark.createDataFrame(df_scores))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

spark.sql("USE CATALOG dataops_olympics")
spark.sql("USE SCHEMA default")

spark.createDataFrame(df_scores).write.format("delta").mode("overwrite").saveAsTable(
    "dataops_olympics.default.event1_scores"
)
print("Results saved to dataops_olympics.default.event1_scores")

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
_event = "Event 1: Data Engineering"

for r in results:
    _t = r["team"]
    if spark.sql(f"SELECT 1 FROM {_RT} WHERE team = '{_t}'").count() == 0:
        spark.sql(f"INSERT INTO {_RT} VALUES ('{_t}')")
    for cat, pts, mx in [
        ("Bronze", r["bronze"], 10), ("Silver", r["silver"], 15),
        ("Gold", r["gold"], 15), ("DQ", r["dq"], 5),
        ("Governance", r["governance"], 5), ("Bonus", r["bonus"], 5),
    ]:
        spark.sql(f"INSERT INTO {_LB} VALUES ('{_t}', '{_event}', '{cat}', {pts}, {mx}, '{_now}')")

print(f"Leaderboard updated: {len(results)} teams × 6 categories")
