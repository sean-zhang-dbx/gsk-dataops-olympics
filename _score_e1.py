# Databricks notebook source
# Simplified Event 1 scoring for team_01 only

TEAMS = ["team_01"]
SCHEMA = "default"

# COMMAND ----------

import pandas as pd

EXPECTED_BRONZE_COLS = {"event_id", "event_timestamp", "source_system", "patient_id", "age", "sex", "cp", "trestbps", "chol", "target"}
EXPECTED_GOLD_COLS = {"age_group", "diagnosis", "patient_count"}
EXPECTED_GOLD_AGG_COLS = {"avg_cholesterol", "avg_blood_pressure", "avg_max_heart_rate"}

def _fqn(catalog, table): return f"{catalog}.{SCHEMA}.{table}"
def _table_exists(catalog, table):
    try:
        spark.table(_fqn(catalog, table))
        return True
    except: return False

def _is_sdp_table(catalog, table):
    try:
        rows = spark.sql(f"SHOW TBLPROPERTIES {_fqn(catalog, table)}").collect()
        props = {str(r[0]).lower(): str(r[1]) for r in rows}
        return any("pipeline" in k for k in props)
    except: return False

def _count_comments(catalog, table):
    try:
        desc = spark.sql(f"DESCRIBE TABLE EXTENDED {_fqn(catalog, table)}").collect()
        has_tbl = False; col_count = 0; in_detail = False
        for r in desc:
            if r[0] and r[0].strip() == "# Detailed Table Information": in_detail = True; continue
            if in_detail and r[0] and r[0].strip().lower() == "comment" and r[1] and len(str(r[1]).strip()) > 3: has_tbl = True
            if not in_detail and r[0] and r[0].strip() not in ("", "#") and r[2] and len(str(r[2]).strip()) > 3: col_count += 1
        return has_tbl, col_count
    except: return False, 0

def _has_liquid_clustering(catalog, table):
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {_fqn(catalog, table)}").collect()[0]
        clustering = detail["clusteringColumns"]
        return clustering is not None and len(clustering) > 0
    except: return False

def _check_ai_functions(catalog):
    try:
        if not _table_exists(catalog, "heart_gold_ai"): return False, False, 0
        df = spark.table(_fqn(catalog, "heart_gold_ai"))
        cols = set(c.lower() for c in df.columns)
        has_col = "cardiovascular_risk" in cols
        return True, has_col, df.count()
    except: return False, False, 0

def score_team(team_name):
    catalog = team_name
    scores = {"team": team_name, "bronze": 0, "silver": 0, "gold": 0, "dq": 0, "governance": 0, "bonus": 0, "total": 0, "path": "?", "details": []}
    def log(msg): scores["details"].append(msg)

    is_sdp = (_table_exists(catalog, "heart_bronze") and _is_sdp_table(catalog, "heart_bronze")) or (_table_exists(catalog, "heart_silver") and _is_sdp_table(catalog, "heart_silver"))
    scores["path"] = "SDP" if is_sdp else "SQL"

    if _table_exists(catalog, "heart_bronze"):
        bronze = spark.table(_fqn(catalog, "heart_bronze"))
        b_cnt = bronze.count(); b_cols = set(bronze.columns)
        if b_cnt >= 495: scores["bronze"] += 5; log(f"Bronze: {b_cnt} rows [+5]")
        elif b_cnt >= 400: scores["bronze"] += 4
        elif b_cnt > 0: scores["bronze"] += 2
        if b_cnt >= 498: scores["bronze"] += 3; log(f"Bronze: all batches [+3]")
        elif b_cnt >= 300: scores["bronze"] += 1
        if EXPECTED_BRONZE_COLS.issubset(b_cols): scores["bronze"] += 2; log("Bronze: schema correct [+2]")
    else: log("Bronze: NOT FOUND")

    if _table_exists(catalog, "heart_silver"):
        silver = spark.table(_fqn(catalog, "heart_silver")); s_cnt = silver.count()
        if is_sdp:
            scores["silver"] += 10
            if 480 <= s_cnt <= 495: scores["silver"] += 3
            distinct_ids = silver.select("event_id").distinct().count()
            if distinct_ids == s_cnt: scores["silver"] += 2
        else:
            if s_cnt > 0: scores["silver"] += 4; log(f"Silver (SQL): {s_cnt} rows [+4]")
            if 480 <= s_cnt <= 495: scores["silver"] += 2; log("Silver: filters applied [+2]")
            elif s_cnt < 498: scores["silver"] += 1
            distinct_ids = silver.select("event_id").distinct().count()
            if distinct_ids == s_cnt: scores["silver"] += 2; log("Silver: deduplicated [+2]")

    if _table_exists(catalog, "heart_gold"):
        gold = spark.table(_fqn(catalog, "heart_gold")); g_cnt = gold.count(); g_cols = set(c.lower() for c in gold.columns)
        if is_sdp:
            if g_cnt > 0: scores["gold"] += 7
            if EXPECTED_GOLD_COLS.issubset(g_cols) and EXPECTED_GOLD_AGG_COLS.issubset(g_cols): scores["gold"] += 5
            has_tbl_comment, _ = _count_comments(catalog, "heart_gold")
            if has_tbl_comment: scores["gold"] += 3
        else:
            if g_cnt > 0: scores["gold"] += 4; log(f"Gold (SQL): {g_cnt} rows [+4]")
            if EXPECTED_GOLD_COLS.issubset(g_cols) and any("avg" in c for c in g_cols): scores["gold"] += 2; log("Gold: correct columns [+2]")
            has_tbl_comment, _ = _count_comments(catalog, "heart_gold")
            if has_tbl_comment: scores["gold"] += 2; log("Gold: governance comment [+2]")

    if is_sdp: scores["dq"] += 5
    else:
        if _table_exists(catalog, "heart_bronze") and _table_exists(catalog, "heart_silver"):
            b_cnt = spark.table(_fqn(catalog, "heart_bronze")).count()
            s_cnt = spark.table(_fqn(catalog, "heart_silver")).count()
            if s_cnt < b_cnt: scores["dq"] += 2; log(f"DQ: filtering evidence ({b_cnt} -> {s_cnt}) [+2]")

    best_tbl_comment = False; best_col_count = 0
    for tbl in ["heart_silver", "heart_bronze", "heart_gold"]:
        if _table_exists(catalog, tbl):
            has_tc, cc = _count_comments(catalog, tbl)
            if has_tc: best_tbl_comment = True
            best_col_count = max(best_col_count, cc)
    if is_sdp:
        if best_tbl_comment: scores["governance"] += 2
        if best_col_count >= 3: scores["governance"] += 3
        elif best_col_count > 0: scores["governance"] += best_col_count
    else:
        if best_tbl_comment: scores["governance"] += 1; log(f"Governance: table comment [+1]")
        if best_col_count >= 3: scores["governance"] += 2; log(f"Governance: {best_col_count} column comments [+2]")
        elif best_col_count > 0: scores["governance"] += 1

    if _table_exists(catalog, "heart_silver") and _has_liquid_clustering(catalog, "heart_silver"):
        scores["bonus"] += 3; log("Bonus: Liquid Clustering [+3]")

    ai_exists, ai_has_col, ai_rows = _check_ai_functions(catalog)
    if ai_exists and ai_has_col and ai_rows > 0: scores["bonus"] += 2; log(f"Bonus: heart_gold_ai with cardiovascular_risk [+2]")
    elif ai_exists and ai_rows > 0: scores["bonus"] += 1

    scores["total"] = scores["bronze"] + scores["silver"] + scores["gold"] + scores["dq"] + scores["governance"] + scores["bonus"]
    return scores

# COMMAND ----------

for team in TEAMS:
    r = score_team(team)
    print(f"\n{'='*60}")
    print(f"  {team} | Path: {r['path']} | TOTAL: {r['total']}/55")
    print(f"  Bronze:{r['bronze']}/10  Silver:{r['silver']}/15  Gold:{r['gold']}/15  DQ:{r['dq']}/5  Gov:{r['governance']}/5  Bonus:{r['bonus']}/5")
    for d in r["details"]:
        print(f"    {d}")

# COMMAND ----------

dbutils.notebook.exit(str(r["total"]))
