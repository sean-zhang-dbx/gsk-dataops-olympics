# Databricks notebook source
# MAGIC %md
# MAGIC # Event 1: Self-Check
# MAGIC
# MAGIC Run this notebook to validate your Event 1 tables against the official answer key.
# MAGIC It checks schema, row counts, data quality, and compares values row-by-row.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_submit

# COMMAND ----------

ANSWER_BRONZE = f"{SHARED_CATALOG}.{SHARED_SCHEMA}.event1_answer_bronze"
ANSWER_SILVER = f"{SHARED_CATALOG}.{SHARED_SCHEMA}.event1_answer_silver"
ANSWER_GOLD = f"{SHARED_CATALOG}.{SHARED_SCHEMA}.event1_answer_gold"

EXP_BRONZE = spark.table(ANSWER_BRONZE).count()
EXP_SILVER = spark.table(ANSWER_SILVER).count()
EXP_GOLD = spark.table(ANSWER_GOLD).count()

BRONZE_REQUIRED = {"event_id", "event_timestamp", "source_system", "patient_id",
                   "age", "sex", "cp", "trestbps", "chol", "fbs", "restecg",
                   "thalach", "exang", "oldpeak", "slope", "ca", "thal", "target"}
GOLD_REQUIRED = {"age_group", "diagnosis", "patient_count",
                 "avg_cholesterol", "avg_blood_pressure", "avg_max_heart_rate"}

passes = 0
fails = 0
warnings = 0

def _pass(msg):
    global passes
    passes += 1
    print(f"  [PASS] {msg}")

def _fail(msg):
    global fails
    fails += 1
    print(f"  [FAIL] {msg}")

def _warn(msg):
    global warnings
    warnings += 1
    print(f"  [WARN] {msg}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Table Check

# COMMAND ----------

print("=" * 60)
print(f"  BRONZE — {CATALOG}.default.heart_bronze")
print(f"  Expected: {EXP_BRONZE} rows, {len(BRONZE_REQUIRED)} required columns")
print("=" * 60)

try:
    bronze = spark.table(f"{CATALOG}.default.heart_bronze")
    b_cols = set(c.lower() for c in bronze.columns)
    b_cnt = bronze.count()

    missing = BRONZE_REQUIRED - b_cols
    if not missing:
        _pass(f"Schema: all {len(BRONZE_REQUIRED)} required columns present")
    else:
        _fail(f"Schema: missing columns {missing}")

    extra = b_cols - BRONZE_REQUIRED - {"record_version", "ingested_at", "_rn", "_rescued_data"}
    if extra:
        _warn(f"Extra columns (not required but OK): {extra}")

    if b_cnt == EXP_BRONZE:
        _pass(f"Row count: {b_cnt} — exact match")
    elif abs(b_cnt - EXP_BRONZE) <= 10:
        _warn(f"Row count: {b_cnt} (expected {EXP_BRONZE}) — close but not exact")
    else:
        _fail(f"Row count: {b_cnt} (expected {EXP_BRONZE})")

    if "event_id" in b_cols:
        missing_ids = spark.sql(f"""
            SELECT COUNT(*) FROM (
                SELECT event_id FROM {ANSWER_BRONZE}
                EXCEPT
                SELECT event_id FROM {CATALOG}.default.heart_bronze
            )
        """).collect()[0][0]
        if missing_ids == 0:
            _pass("Data: all event_ids match the answer key")
        else:
            _fail(f"Data: {missing_ids} event_ids from the answer key are missing in your table")

except Exception as e:
    _fail(f"Table not found: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table Check

# COMMAND ----------

print("=" * 60)
print(f"  SILVER — {CATALOG}.default.heart_silver")
print(f"  Expected: {EXP_SILVER} rows")
print("=" * 60)

try:
    silver = spark.table(f"{CATALOG}.default.heart_silver")
    s_cols = set(c.lower() for c in silver.columns)
    s_cnt = silver.count()

    core_silver = {"event_id", "age", "sex", "cp", "trestbps", "chol",
                   "thalach", "exang", "oldpeak", "slope", "ca", "thal", "target"}
    missing = core_silver - s_cols
    if not missing:
        _pass(f"Schema: all core clinical columns present")
    else:
        _fail(f"Schema: missing columns {missing}")

    if s_cnt == EXP_SILVER:
        _pass(f"Row count: {s_cnt} — exact match")
    elif abs(s_cnt - EXP_SILVER) <= 5:
        _warn(f"Row count: {s_cnt} (expected {EXP_SILVER}) — close")
    else:
        _fail(f"Row count: {s_cnt} (expected {EXP_SILVER})")

    null_ages = silver.filter("age IS NULL").count()
    if null_ages == 0:
        _pass("DQ: no null ages")
    else:
        _fail(f"DQ: {null_ages} rows have null age — these should be filtered out")

    bad_bp = silver.filter("trestbps NOT BETWEEN 50 AND 300").count()
    if bad_bp == 0:
        _pass("DQ: no invalid blood pressure values")
    else:
        _fail(f"DQ: {bad_bp} rows have invalid trestbps — these should be filtered out")

    neg_chol = silver.filter("chol < 0").count()
    if neg_chol == 0:
        _pass("DQ: no negative cholesterol values")
    else:
        _fail(f"DQ: {neg_chol} rows have negative chol — these should be filtered out")

    if "event_id" in s_cols:
        dup_cnt = s_cnt - silver.select("event_id").distinct().count()
        if dup_cnt == 0:
            _pass("Dedup: no duplicate event_ids")
        else:
            _fail(f"Dedup: {dup_cnt} duplicate event_ids remain — deduplicate on event_id")

        missing_ids = spark.sql(f"""
            SELECT COUNT(*) FROM (
                SELECT event_id FROM {ANSWER_SILVER}
                EXCEPT
                SELECT event_id FROM {CATALOG}.default.heart_silver
            )
        """).collect()[0][0]
        extra_ids = spark.sql(f"""
            SELECT COUNT(*) FROM (
                SELECT event_id FROM {CATALOG}.default.heart_silver
                EXCEPT
                SELECT event_id FROM {ANSWER_SILVER}
            )
        """).collect()[0][0]
        if missing_ids == 0 and extra_ids == 0:
            _pass("Data: event_ids match the answer key exactly")
        else:
            if missing_ids > 0:
                _fail(f"Data: {missing_ids} expected event_ids are missing (rows over-filtered or lost)")
            if extra_ids > 0:
                _fail(f"Data: {extra_ids} unexpected event_ids present (dirty rows not filtered)")

except Exception as e:
    _fail(f"Table not found: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table Check

# COMMAND ----------

print("=" * 60)
print(f"  GOLD — {CATALOG}.default.heart_gold")
print(f"  Expected: {EXP_GOLD} rows with columns: {sorted(GOLD_REQUIRED)}")
print("=" * 60)

try:
    gold = spark.table(f"{CATALOG}.default.heart_gold")
    g_cols = set(c.lower() for c in gold.columns)
    g_cnt = gold.count()

    missing = GOLD_REQUIRED - g_cols
    if not missing:
        _pass(f"Schema: all 6 required columns present")
    else:
        _fail(f"Schema: missing columns {missing}")
        if missing:
            print(f"         Required: {sorted(GOLD_REQUIRED)}")
            print(f"         You have: {sorted(g_cols)}")

    if g_cnt == EXP_GOLD:
        _pass(f"Row count: {g_cnt} — exact match (4 age groups × 2 diagnoses)")
    else:
        _fail(f"Row count: {g_cnt} (expected {EXP_GOLD})")

    if not missing and g_cnt > 0:
        expected_groups = {"Under 40", "40-49", "50-59", "60+"}
        actual_groups = set(gold.select("age_group").distinct().toPandas()["age_group"])
        if actual_groups == expected_groups:
            _pass(f"Age groups: {sorted(actual_groups)}")
        else:
            _fail(f"Age groups: {sorted(actual_groups)} (expected {sorted(expected_groups)})")

        expected_diag = {"Heart Disease", "Healthy"}
        actual_diag = set(gold.select("diagnosis").distinct().toPandas()["diagnosis"])
        if actual_diag == expected_diag:
            _pass(f"Diagnoses: {sorted(actual_diag)}")
        else:
            _fail(f"Diagnoses: {sorted(actual_diag)} (expected {sorted(expected_diag)})")

        print()
        print("  Row-by-row comparison against answer key:")
        comparison = spark.sql(f"""
            SELECT
                a.age_group, a.diagnosis,
                a.patient_count AS expected_count, t.patient_count AS actual_count,
                a.avg_cholesterol AS exp_chol, t.avg_cholesterol AS act_chol,
                a.avg_blood_pressure AS exp_bp, t.avg_blood_pressure AS act_bp,
                a.avg_max_heart_rate AS exp_hr, t.avg_max_heart_rate AS act_hr
            FROM {ANSWER_GOLD} a
            LEFT JOIN {CATALOG}.default.heart_gold t
                ON a.age_group = t.age_group AND a.diagnosis = t.diagnosis
            ORDER BY a.age_group, a.diagnosis
        """).collect()

        all_match = True
        for r in comparison:
            if r.actual_count is None:
                print(f"    MISSING: {r.age_group} / {r.diagnosis}")
                all_match = False
                continue
            count_ok = r.expected_count == r.actual_count
            chol_ok = abs(r.exp_chol - r.act_chol) <= 0.5 if r.act_chol is not None else False
            bp_ok = abs(r.exp_bp - r.act_bp) <= 0.5 if r.act_bp is not None else False
            hr_ok = abs(r.exp_hr - r.act_hr) <= 0.5 if r.act_hr is not None else False
            status = "OK" if (count_ok and chol_ok and bp_ok and hr_ok) else "MISMATCH"
            if status != "OK":
                all_match = False
            details = []
            if not count_ok:
                details.append(f"count {r.actual_count}!={r.expected_count}")
            if not chol_ok:
                details.append(f"chol {r.act_chol}!={r.exp_chol}")
            if not bp_ok:
                details.append(f"bp {r.act_bp}!={r.exp_bp}")
            if not hr_ok:
                details.append(f"hr {r.act_hr}!={r.exp_hr}")
            suffix = f" ({', '.join(details)})" if details else ""
            print(f"    [{status:8s}] {r.age_group:10s} / {r.diagnosis:15s}  count={r.actual_count}{suffix}")

        if all_match:
            _pass("All gold values match the answer key")
        else:
            _fail("Some gold values do not match — check the details above")

except Exception as e:
    _fail(f"Table not found: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance Check

# COMMAND ----------

print("=" * 60)
print("  GOVERNANCE — Table & Column Comments")
print("=" * 60)

for tbl in ["heart_bronze", "heart_silver", "heart_gold"]:
    try:
        desc = spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.default.{tbl}").collect()
        has_comment = False
        col_comments = 0
        in_detail = False
        for r in desc:
            if r[0] and r[0].strip() == "# Detailed Table Information":
                in_detail = True
                continue
            if in_detail and r[0] and r[0].strip().lower() == "comment" and r[1] and len(str(r[1]).strip()) > 3:
                has_comment = True
            if not in_detail and r[0] and r[0].strip() not in ("", "#") and r[2] and len(str(r[2]).strip()) > 3:
                col_comments += 1
        if has_comment:
            _pass(f"{tbl}: has table comment")
        else:
            _fail(f"{tbl}: no table comment — add one with ALTER TABLE SET TBLPROPERTIES('comment'='...')")
        if col_comments > 0:
            _pass(f"{tbl}: {col_comments} column comments")
    except Exception:
        _warn(f"{tbl}: could not check (table may not exist)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print()
print("=" * 60)
print(f"  SELF-CHECK SUMMARY — {TEAM_NAME}")
print("=" * 60)
print(f"  Passed:   {passes}")
print(f"  Failed:   {fails}")
print(f"  Warnings: {warnings}")
print()
if fails == 0:
    print("  ALL CHECKS PASSED — you're ready to submit!")
elif fails <= 3:
    print("  ALMOST THERE — fix the failures above before submitting.")
else:
    print("  NEEDS WORK — review the failures above.")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Submission Status

# COMMAND ----------

check_submission("event1")
