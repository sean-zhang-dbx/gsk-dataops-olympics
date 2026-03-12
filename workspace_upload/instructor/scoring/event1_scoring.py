# Databricks notebook source
# MAGIC %md
# MAGIC # Event 1: Automated Scoring
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY**
# MAGIC
# MAGIC Scores each team's Event 1 submission by comparing their Bronze, Silver, and Gold
# MAGIC tables against pre-generated answer tables in `dataops_olympics.default`.
# MAGIC
# MAGIC ### Scoring Breakdown (50 pts max + 5 bonus)
# MAGIC
# MAGIC | Category | SDP Path | SQL Path |
# MAGIC |----------|----------|----------|
# MAGIC | **Bronze** — row count, schema, data match | 10 | 10 |
# MAGIC | **Silver** — row count, data match, DQ, dedup | 15 | 8 |
# MAGIC | **Gold** — row count, schema, value accuracy | 15 | 8 |
# MAGIC | **Data Quality** — DQ expectations / evidence | 5 | 2 |
# MAGIC | **Governance** — table + column comments | 5 | 3 |
# MAGIC | **TOTAL** | **50** | **31** |
# MAGIC | **Bonus: UC Tags** | +3 | +3 |
# MAGIC | **Bonus: AI Functions** (`heart_gold_ai`) | +2 | +2 |

# COMMAND ----------

_event_filter = "event1"
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
SHARED = "dataops_olympics"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Answer Keys

# COMMAND ----------

ANSWER_BRONZE = f"{SHARED}.{SCHEMA}.event1_answer_bronze"
ANSWER_SILVER = f"{SHARED}.{SCHEMA}.event1_answer_silver"
ANSWER_GOLD = f"{SHARED}.{SCHEMA}.event1_answer_gold"

EXPECTED_BRONZE = spark.table(ANSWER_BRONZE).count()
EXPECTED_SILVER = spark.table(ANSWER_SILVER).count()
EXPECTED_GOLD = spark.table(ANSWER_GOLD).count()
print(f"Answer key: Bronze={EXPECTED_BRONZE}, Silver={EXPECTED_SILVER}, Gold={EXPECTED_GOLD}")

BRONZE_REQUIRED = {"event_id", "event_timestamp", "source_system", "patient_id",
                   "age", "sex", "cp", "trestbps", "chol", "target"}
GOLD_REQUIRED = {"age_group", "diagnosis", "patient_count",
                 "avg_cholesterol", "avg_blood_pressure", "avg_max_heart_rate"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring Engine

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


def _is_sdp_table(catalog, table):
    try:
        rows = spark.sql(f"SHOW TBLPROPERTIES {_fqn(catalog, table)}").collect()
        props = {str(r[0]).lower(): str(r[1]) for r in rows}
        return any("pipeline" in k for k in props)
    except Exception:
        return False


def _count_comments(catalog, table):
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
    try:
        rows = spark.sql(f"""
            SELECT COUNT(*) AS cnt FROM system.information_schema.table_tags
            WHERE catalog_name = '{catalog}' AND schema_name = '{SCHEMA}'
              AND table_name IN ('heart_bronze', 'heart_silver', 'heart_gold')
        """).collect()
        return rows[0][0] if rows else 0
    except Exception:
        return 0


def _event_id_match(team_table, answer_table):
    """Returns (missing_from_team, extra_in_team) counts based on event_id EXCEPT."""
    missing = spark.sql(f"""
        SELECT COUNT(*) FROM (
            SELECT event_id FROM {answer_table}
            EXCEPT
            SELECT event_id FROM {team_table}
        )
    """).collect()[0][0]
    extra = spark.sql(f"""
        SELECT COUNT(*) FROM (
            SELECT event_id FROM {team_table}
            EXCEPT
            SELECT event_id FROM {answer_table}
        )
    """).collect()[0][0]
    return missing, extra


def _compare_gold_values(catalog):
    """Join team gold with answer gold on (age_group, diagnosis) and compare values.
    Returns (matched_rows, count_matches, avg_matches, details_list)."""
    team_table = _fqn(catalog, "heart_gold")
    rows = spark.sql(f"""
        SELECT
            a.age_group, a.diagnosis,
            a.patient_count AS exp_count, t.patient_count AS act_count,
            a.avg_cholesterol AS exp_chol, t.avg_cholesterol AS act_chol,
            a.avg_blood_pressure AS exp_bp, t.avg_blood_pressure AS act_bp,
            a.avg_max_heart_rate AS exp_hr, t.avg_max_heart_rate AS act_hr
        FROM {ANSWER_GOLD} a
        LEFT JOIN {team_table} t
            ON a.age_group = t.age_group AND a.diagnosis = t.diagnosis
        ORDER BY a.age_group, a.diagnosis
    """).collect()

    matched = 0
    count_ok = 0
    avg_ok = 0
    details = []
    for r in rows:
        if r.act_count is None:
            details.append(f"  MISSING: {r.age_group} / {r.diagnosis}")
            continue
        matched += 1
        c_ok = r.exp_count == r.act_count
        a_ok = (abs(r.exp_chol - r.act_chol) <= 0.5 and
                abs(r.exp_bp - r.act_bp) <= 0.5 and
                abs(r.exp_hr - r.act_hr) <= 0.5)
        if c_ok:
            count_ok += 1
        if a_ok:
            avg_ok += 1
        if not c_ok or not a_ok:
            parts = []
            if not c_ok:
                parts.append(f"count {r.act_count} != {r.exp_count}")
            if not a_ok:
                parts.append(f"avgs off")
            details.append(f"  MISMATCH: {r.age_group}/{r.diagnosis}: {', '.join(parts)}")

    return matched, count_ok, avg_ok, details


def score_team(team_name: str) -> dict:
    catalog = team_name
    scores = {
        "team": team_name, "bronze": 0, "silver": 0, "gold": 0,
        "dq": 0, "governance": 0, "bonus": 0, "total": 0,
        "path": "?", "details": [],
    }

    def log(msg):
        scores["details"].append(msg)

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
        b_cols = set(c.lower() for c in bronze.columns)

        # Schema check (2 pts)
        if BRONZE_REQUIRED.issubset(b_cols):
            scores["bronze"] += 2
            log("Bronze: schema correct [+2]")
        elif len(BRONZE_REQUIRED - b_cols) <= 2:
            scores["bronze"] += 1
            log(f"Bronze: mostly correct schema, missing {BRONZE_REQUIRED - b_cols} [+1]")
        else:
            log(f"Bronze: missing columns {BRONZE_REQUIRED - b_cols} [+0]")

        # Row count (4 pts): exact = 4, close = 2, partial = 1
        if b_cnt == EXPECTED_BRONZE:
            scores["bronze"] += 4
            log(f"Bronze: {b_cnt} rows — exact match [+4]")
        elif abs(b_cnt - EXPECTED_BRONZE) <= 10:
            scores["bronze"] += 3
            log(f"Bronze: {b_cnt} rows — close (expected {EXPECTED_BRONZE}) [+3]")
        elif b_cnt >= 400:
            scores["bronze"] += 2
            log(f"Bronze: {b_cnt} rows — most batches loaded [+2]")
        elif b_cnt > 0:
            scores["bronze"] += 1
            log(f"Bronze: {b_cnt} rows — partial [+1]")
        else:
            log("Bronze: table empty [+0]")

        # Data accuracy via EXCEPT (4 pts)
        if "event_id" in b_cols:
            missing, extra = _event_id_match(_fqn(catalog, "heart_bronze"), ANSWER_BRONZE)
            match_pct = (EXPECTED_BRONZE - missing) / EXPECTED_BRONZE * 100
            if missing == 0 and extra == 0:
                scores["bronze"] += 4
                log(f"Bronze: all event_ids match answer key [+4]")
            elif match_pct >= 95:
                scores["bronze"] += 3
                log(f"Bronze: {match_pct:.0f}% event_id match (missing {missing}, extra {extra}) [+3]")
            elif match_pct >= 80:
                scores["bronze"] += 2
                log(f"Bronze: {match_pct:.0f}% event_id match [+2]")
            elif match_pct >= 50:
                scores["bronze"] += 1
                log(f"Bronze: {match_pct:.0f}% event_id match [+1]")
            else:
                log(f"Bronze: {match_pct:.0f}% event_id match [+0]")
        else:
            log("Bronze: no event_id column — cannot verify data accuracy [+0]")
    else:
        log(f"Bronze: TABLE NOT FOUND ({_fqn(catalog, 'heart_bronze')})")

    # ═══════════════════════════════════════
    # SILVER (SDP: 15 pts / SQL: 8 pts)
    # ═══════════════════════════════════════
    if _table_exists(catalog, "heart_silver"):
        silver = spark.table(_fqn(catalog, "heart_silver"))
        s_cnt = silver.count()
        s_cols = set(c.lower() for c in silver.columns)

        if is_sdp:
            # SDP path: 15 pts
            scores["silver"] += 5
            log(f"Silver (SDP): pipeline-managed table [+5]")

            if s_cnt == EXPECTED_SILVER:
                scores["silver"] += 4
                log(f"Silver: {s_cnt} rows — exact match [+4]")
            elif abs(s_cnt - EXPECTED_SILVER) <= 5:
                scores["silver"] += 3
                log(f"Silver: {s_cnt} rows — close (expected {EXPECTED_SILVER}) [+3]")
            elif s_cnt < EXPECTED_BRONZE:
                scores["silver"] += 1
                log(f"Silver: {s_cnt} rows — some filtering applied [+1]")
            else:
                log(f"Silver: {s_cnt} rows — no filtering detected [+0]")

            if "event_id" in s_cols:
                missing, extra = _event_id_match(_fqn(catalog, "heart_silver"), ANSWER_SILVER)
                match_pct = (EXPECTED_SILVER - missing) / EXPECTED_SILVER * 100
                if missing == 0 and extra == 0:
                    scores["silver"] += 4
                    log(f"Silver: all event_ids match answer key [+4]")
                elif match_pct >= 95:
                    scores["silver"] += 3
                    log(f"Silver: {match_pct:.0f}% match [+3]")
                elif match_pct >= 80:
                    scores["silver"] += 2
                    log(f"Silver: {match_pct:.0f}% match [+2]")
                else:
                    scores["silver"] += 1
                    log(f"Silver: {match_pct:.0f}% match [+1]")

                dup_cnt = s_cnt - silver.select("event_id").distinct().count()
                if dup_cnt == 0:
                    scores["silver"] += 2
                    log("Silver: no duplicate event_ids [+2]")
                else:
                    log(f"Silver: {dup_cnt} duplicates remain [+0]")
            else:
                log("Silver: no event_id column — cannot verify [+0]")
        else:
            # SQL path: 8 pts
            if s_cnt == EXPECTED_SILVER:
                scores["silver"] += 3
                log(f"Silver (SQL): {s_cnt} rows — exact match [+3]")
            elif abs(s_cnt - EXPECTED_SILVER) <= 5:
                scores["silver"] += 2
                log(f"Silver (SQL): {s_cnt} rows — close (expected {EXPECTED_SILVER}) [+2]")
            elif 0 < s_cnt < EXPECTED_BRONZE:
                scores["silver"] += 1
                log(f"Silver (SQL): {s_cnt} rows — some filtering [+1]")
            else:
                log(f"Silver (SQL): {s_cnt} rows [+0]")

            if "event_id" in s_cols:
                missing, extra = _event_id_match(_fqn(catalog, "heart_silver"), ANSWER_SILVER)
                match_pct = (EXPECTED_SILVER - missing) / EXPECTED_SILVER * 100
                if missing == 0 and extra == 0:
                    scores["silver"] += 3
                    log(f"Silver: all event_ids match answer key [+3]")
                elif match_pct >= 90:
                    scores["silver"] += 2
                    log(f"Silver: {match_pct:.0f}% match [+2]")
                elif match_pct >= 70:
                    scores["silver"] += 1
                    log(f"Silver: {match_pct:.0f}% match [+1]")
                else:
                    log(f"Silver: {match_pct:.0f}% match [+0]")

                dup_cnt = s_cnt - silver.select("event_id").distinct().count()
                if dup_cnt == 0:
                    scores["silver"] += 2
                    log("Silver: no duplicate event_ids [+2]")
                else:
                    log(f"Silver: {dup_cnt} duplicates remain [+0]")
            else:
                log("Silver: no event_id column — cannot verify [+0]")
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
            # SDP path: 15 pts
            if GOLD_REQUIRED.issubset(g_cols):
                scores["gold"] += 3
                log("Gold (SDP): all required columns present [+3]")
            elif {"age_group", "diagnosis", "patient_count"}.issubset(g_cols):
                scores["gold"] += 2
                log("Gold (SDP): core columns present, missing some avg columns [+2]")
            elif g_cnt > 0:
                scores["gold"] += 1
                log(f"Gold (SDP): exists with {g_cnt} rows but schema incomplete [+1]")

            if g_cnt == EXPECTED_GOLD:
                scores["gold"] += 2
                log(f"Gold: {g_cnt} rows — exact match [+2]")
            elif g_cnt > 0:
                scores["gold"] += 1
                log(f"Gold: {g_cnt} rows (expected {EXPECTED_GOLD}) [+1]")

            if GOLD_REQUIRED.issubset(g_cols) and g_cnt > 0:
                matched, count_ok, avg_ok, g_details = _compare_gold_values(catalog)
                if count_ok == EXPECTED_GOLD:
                    scores["gold"] += 5
                    log(f"Gold: all {EXPECTED_GOLD} patient_counts match exactly [+5]")
                elif count_ok >= 6:
                    scores["gold"] += 4
                    log(f"Gold: {count_ok}/{EXPECTED_GOLD} patient_counts match [+4]")
                elif count_ok >= 4:
                    scores["gold"] += 3
                    log(f"Gold: {count_ok}/{EXPECTED_GOLD} patient_counts match [+3]")
                elif count_ok > 0:
                    scores["gold"] += 1
                    log(f"Gold: {count_ok}/{EXPECTED_GOLD} patient_counts match [+1]")

                if avg_ok == EXPECTED_GOLD:
                    scores["gold"] += 5
                    log(f"Gold: all averages within tolerance [+5]")
                elif avg_ok >= 6:
                    scores["gold"] += 4
                    log(f"Gold: {avg_ok}/{EXPECTED_GOLD} rows have correct averages [+4]")
                elif avg_ok >= 4:
                    scores["gold"] += 3
                    log(f"Gold: {avg_ok}/{EXPECTED_GOLD} rows have correct averages [+3]")
                elif avg_ok > 0:
                    scores["gold"] += 1
                    log(f"Gold: {avg_ok}/{EXPECTED_GOLD} rows have correct averages [+1]")

                for d in g_details:
                    log(d)
        else:
            # SQL path: 8 pts
            if GOLD_REQUIRED.issubset(g_cols):
                scores["gold"] += 2
                log("Gold (SQL): all required columns present [+2]")
            elif {"age_group", "patient_count"}.issubset(g_cols):
                scores["gold"] += 1
                log("Gold (SQL): partial columns [+1]")

            if g_cnt == EXPECTED_GOLD:
                scores["gold"] += 1
                log(f"Gold: {g_cnt} rows — exact match [+1]")
            elif g_cnt > 0:
                log(f"Gold: {g_cnt} rows (expected {EXPECTED_GOLD}) [+0]")

            if GOLD_REQUIRED.issubset(g_cols) and g_cnt > 0:
                matched, count_ok, avg_ok, g_details = _compare_gold_values(catalog)
                if count_ok == EXPECTED_GOLD:
                    scores["gold"] += 3
                    log(f"Gold: all {EXPECTED_GOLD} patient_counts match exactly [+3]")
                elif count_ok >= 6:
                    scores["gold"] += 2
                    log(f"Gold: {count_ok}/{EXPECTED_GOLD} patient_counts match [+2]")
                elif count_ok > 0:
                    scores["gold"] += 1
                    log(f"Gold: {count_ok}/{EXPECTED_GOLD} patient_counts match [+1]")

                if avg_ok == EXPECTED_GOLD:
                    scores["gold"] += 2
                    log(f"Gold: all averages within tolerance [+2]")
                elif avg_ok >= 6:
                    scores["gold"] += 1
                    log(f"Gold: {avg_ok}/{EXPECTED_GOLD} rows have correct averages [+1]")

                for d in g_details:
                    log(d)
    else:
        log(f"Gold: TABLE NOT FOUND ({_fqn(catalog, 'heart_gold')})")

    # ═══════════════════════════════════════
    # DATA QUALITY (SDP: 5 pts / SQL: 2 pts)
    # ═══════════════════════════════════════
    if is_sdp:
        scores["dq"] += 5
        log("DQ: SDP expectations provide automatic DQ tracking [+5]")
    else:
        if _table_exists(catalog, "heart_silver"):
            silver = spark.table(_fqn(catalog, "heart_silver"))
            null_ages = silver.filter("age IS NULL").count()
            bad_bp = silver.filter("trestbps NOT BETWEEN 50 AND 300").count()
            if null_ages == 0 and bad_bp == 0:
                scores["dq"] += 2
                log("DQ: silver has no null ages or invalid BP — filters correct [+2]")
            elif null_ages == 0 or bad_bp == 0:
                scores["dq"] += 1
                log(f"DQ: partial filtering (null_ages={null_ages}, bad_bp={bad_bp}) [+1]")
            else:
                log(f"DQ: dirty rows remain in silver (null_ages={null_ages}, bad_bp={bad_bp}) [+0]")
        else:
            log("DQ: silver table missing [+0]")

    # ═══════════════════════════════════════
    # GOVERNANCE (SDP: 5 pts / SQL: 3 pts)
    # ═══════════════════════════════════════
    best_tbl_comment = False
    best_col_count = 0

    for tbl in ["heart_bronze", "heart_silver", "heart_gold"]:
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
    tag_count = _count_uc_tags(catalog)
    if tag_count >= 3:
        scores["bonus"] += 3
        log(f"Bonus: {tag_count} UC Tags across tables [+3]")
    elif tag_count > 0:
        scores["bonus"] += tag_count
        log(f"Bonus: {tag_count} UC Tags (need 3+ for full credit) [+{tag_count}]")

    if _table_exists(catalog, "heart_gold_ai"):
        ai_df = spark.table(_fqn(catalog, "heart_gold_ai"))
        ai_cols = set(c.lower() for c in ai_df.columns)
        ai_cnt = ai_df.count()
        if "cardiovascular_risk" in ai_cols and ai_cnt > 0:
            scores["bonus"] += 2
            log(f"Bonus: heart_gold_ai with cardiovascular_risk, {ai_cnt} rows [+2]")
        elif ai_cnt > 0:
            scores["bonus"] += 1
            log(f"Bonus: heart_gold_ai exists ({ai_cnt} rows) but missing cardiovascular_risk [+1]")
        else:
            log("Bonus: heart_gold_ai exists but is empty [+0]")

    scores["total"] = sum(scores[k] for k in ["bronze", "silver", "gold", "dq", "governance", "bonus"])
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
    sdp_max = 55 if r["path"] == "SDP" else 36
    print(f"  Path: {r['path']}  |  TOTAL: {r['total']}/{sdp_max}")
    print(f"  Bronze:{r['bronze']}/10  Silver:{r['silver']}/{'15' if r['path']=='SDP' else '8'}  Gold:{r['gold']}/{'15' if r['path']=='SDP' else '8'}  DQ:{r['dq']}/{'5' if r['path']=='SDP' else '2'}  Gov:{r['governance']}/{'5' if r['path']=='SDP' else '3'}  Bonus:{r['bonus']}/5")
    for d in r["details"]:
        print(f"    {d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leaderboard

# COMMAND ----------

df_scores = pd.DataFrame([{
    "Team": r["team"], "Path": r["path"],
    "Bronze": r["bronze"], "Silver": r["silver"], "Gold": r["gold"],
    "DQ": r["dq"], "Governance": r["governance"], "Bonus": r["bonus"],
    "Total": r["total"],
} for r in results]).sort_values("Total", ascending=False)

print("=" * 72)
print("  EVENT 1: DATA ENGINEERING — FINAL LEADERBOARD")
print("=" * 72)
for rank, (_, row) in enumerate(df_scores.iterrows(), 1):
    medal = {1: "[GOLD]  ", 2: "[SILVER]", 3: "[BRONZE]"}.get(rank, "        ")
    print(f"  {rank}. {medal} {row['Team']:12s} | {row['Path']:3s} | {int(row['Total']):2d} pts")
print("=" * 72)

# COMMAND ----------

display(spark.createDataFrame(df_scores))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results & Update Leaderboard

# COMMAND ----------

spark.createDataFrame(df_scores).write.format("delta").mode("overwrite").saveAsTable(
    "dataops_olympics.default.event1_scores"
)
print("Results saved to dataops_olympics.default.event1_scores")

# COMMAND ----------

_LB = "dataops_olympics.default.olympics_leaderboard"
_RT = "dataops_olympics.default.registered_teams"
spark.sql(f"CREATE TABLE IF NOT EXISTS {_LB} (team STRING, event STRING, category STRING, points DOUBLE, max_points DOUBLE, scored_at TIMESTAMP)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {_RT} (team STRING)")

_event = "Event 1: Data Engineering"

for r in results:
    _t = r["team"]
    spark.sql(f"MERGE INTO {_RT} USING (SELECT '{_t}' AS team) src ON {_RT}.team = src.team WHEN NOT MATCHED THEN INSERT (team) VALUES (src.team)")

    _is_sdp = r["path"] == "SDP"
    for cat, pts, mx in [
        ("Bronze", r["bronze"], 10),
        ("Silver", r["silver"], 15 if _is_sdp else 8),
        ("Gold", r["gold"], 15 if _is_sdp else 8),
        ("DQ", r["dq"], 5 if _is_sdp else 2),
        ("Governance", r["governance"], 5 if _is_sdp else 3),
        ("Bonus", r["bonus"], 5),
    ]:
        spark.sql(f"""
            MERGE INTO {_LB} AS target
            USING (SELECT '{_t}' AS team, '{_event}' AS event, '{cat}' AS category, {pts} AS points, {mx} AS max_points, current_timestamp() AS scored_at) AS source
            ON target.team = source.team AND target.event = source.event AND target.category = source.category
            WHEN MATCHED THEN UPDATE SET points = source.points, max_points = source.max_points, scored_at = source.scored_at
            WHEN NOT MATCHED THEN INSERT (team, event, category, points, max_points, scored_at) VALUES (source.team, source.event, source.category, source.points, source.max_points, source.scored_at)
        """)

print(f"Leaderboard updated: {len(results)} teams")
