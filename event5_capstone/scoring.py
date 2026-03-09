# Databricks notebook source
# MAGIC %md
# MAGIC # Event 5: Capstone — Automated Scoring
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY**
# MAGIC
# MAGIC ### Scoring Breakdown (30-35 pts + 5 bonus)
# MAGIC
# MAGIC | Category | Points | How Scored |
# MAGIC |----------|--------|------------|
# MAGIC | Artifacts verified | 3 | Auto: table existence |
# MAGIC | Executive briefing | 5 | Auto: briefing table + LLM quality check |
# MAGIC | AI/BI Dashboard (Option A) | 10 | Semi-auto: Lakeview API + judge rubric |
# MAGIC | OR Databricks App (Option B) | **15** | Auto: app deployment + judge rubric |
# MAGIC | Genie space | 5 | Auto: Conversation API test |
# MAGIC | Presentation | 7 | Manual: judge scores |
# MAGIC | **Bonus** | +5 | Auto: published, filters, schedule |

# COMMAND ----------

TEAMS = ["team_01", "team_02", "team_03", "team_04"]
SCHEMA = "default"
SHARED_CATALOG = "dataops_olympics"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Enter Genie Space IDs and dashboard names. Presentation scores are manual.

# COMMAND ----------

GENIE_SPACE_IDS = {
    "team_01": "",
    "team_02": "",
    "team_03": "",
    "team_04": "",
}

PRESENTATION_SCORES = {
    "team_01": 0,  # 0-7 pts, judged live
    "team_02": 0,
    "team_03": 0,
    "team_04": 0,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring Engine

# COMMAND ----------

import pandas as pd
import datetime as dt

try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    SDK_AVAILABLE = True
except ImportError:
    SDK_AVAILABLE = False
    print("WARNING: databricks-sdk not available")


def _table_exists(catalog, table):
    try:
        spark.table(f"{catalog}.{SCHEMA}.{table}")
        return True
    except Exception:
        return False


def _table_count(catalog, table):
    try:
        return spark.table(f"{catalog}.{SCHEMA}.{table}").count()
    except Exception:
        return 0


def _test_genie(space_id, question="How many patients have heart disease?"):
    """Send a test question to a Genie space and check if it responds."""
    if not SDK_AVAILABLE or not space_id:
        return False, "SDK not available or no space ID"
    try:
        msg = w.genie.start_conversation_and_wait(
            space_id=space_id,
            content=question,
            timeout=dt.timedelta(minutes=2),
        )
        if msg.status and msg.status.value == "COMPLETED":
            for att in (msg.attachments or []):
                if att.text and att.text.content:
                    return True, att.text.content[:100]
                if att.query and att.query.query:
                    return True, f"SQL: {att.query.query[:80]}"
            return True, "Completed (no text)"
        return False, f"Status: {msg.status.value if msg.status else 'unknown'}"
    except Exception as e:
        return False, str(e)[:100]


def _check_dashboard_exists(team_name):
    """Search for a dashboard with the team name in the title."""
    if not SDK_AVAILABLE:
        return False, None
    try:
        dashboards = w.lakeview.list()
        for d in dashboards:
            if d.display_name and team_name.lower() in d.display_name.lower():
                return True, d.display_name
        return False, None
    except Exception:
        return False, None


def _check_app_exists(team_name):
    """Search for a deployed Databricks App with the team name."""
    if not SDK_AVAILABLE:
        return False, None
    try:
        apps = w.apps.list()
        for app in apps:
            if app.name and team_name.lower().replace("_", "-") in app.name.lower():
                is_running = (
                    hasattr(app, "active_deployment")
                    and app.active_deployment is not None
                )
                return True, app.name, is_running
        return False, None, False
    except Exception:
        return False, None, False


def score_team(team_name: str) -> dict:
    catalog = team_name
    scores = {
        "team": team_name,
        "artifacts": 0, "briefing": 0, "deliverable": 0,
        "deliverable_type": "none",
        "genie": 0, "presentation": 0, "bonus": 0, "total": 0,
        "details": [],
    }

    def log(msg):
        scores["details"].append(msg)

    # ─── ARTIFACTS (3 pts) ───
    required_tables = ["heart_silver", "heart_gold", "heart_ai_insights", "drug_ai_summary"]
    found = sum(1 for t in required_tables if _table_exists(catalog, t))
    if found >= 4:
        scores["artifacts"] = 3
        log(f"Artifacts: all {found}/4 tables present [+3]")
    elif found >= 2:
        scores["artifacts"] = 2
        log(f"Artifacts: {found}/4 tables present [+2]")
    elif found >= 1:
        scores["artifacts"] = 1
        log(f"Artifacts: {found}/4 tables present [+1]")
    else:
        log("Artifacts: no tables found [+0]")

    # ─── EXECUTIVE BRIEFING (5 pts) ───
    if _table_exists(catalog, "executive_briefing"):
        cnt = _table_count(catalog, "executive_briefing")
        if cnt > 0:
            try:
                briefing = spark.table(f"{catalog}.{SCHEMA}.executive_briefing").select("briefing_text").collect()[0][0]
                if briefing and len(briefing) > 200:
                    scores["briefing"] = 5
                    log(f"Briefing: found, {len(briefing)} chars (comprehensive) [+5]")
                elif briefing and len(briefing) > 50:
                    scores["briefing"] = 3
                    log(f"Briefing: found but short ({len(briefing)} chars) [+3]")
                else:
                    scores["briefing"] = 1
                    log("Briefing: table exists but content is minimal [+1]")
            except Exception:
                scores["briefing"] = 2
                log("Briefing: table exists, couldn't read content [+2]")
        else:
            log("Briefing: table exists but empty [+0]")
    else:
        log("Briefing: executive_briefing table not found [+0]")

    # ─── BUSINESS DELIVERABLE: App (15 pts) or Dashboard (10 pts) ───
    app_found, app_name, app_running = _check_app_exists(team_name)
    dash_found, dash_name = _check_dashboard_exists(team_name)

    if app_found:
        scores["deliverable_type"] = "app"
        if app_running:
            scores["deliverable"] = 12
            log(f"App: '{app_name}' deployed and running [+12 auto, judge adds 0-3]")
        else:
            scores["deliverable"] = 8
            log(f"App: '{app_name}' exists but not actively running [+8 auto, judge adds 0-3]")
    elif dash_found:
        scores["deliverable_type"] = "dashboard"
        scores["deliverable"] = 7
        log(f"Dashboard: found '{dash_name}' [+7 auto, judge adds 0-3]")
    else:
        log("Deliverable: no app or dashboard found. Judge may award points manually.")

    # ─── GENIE (5 pts) ───
    space_id = GENIE_SPACE_IDS.get(team_name, "")
    if space_id:
        genie_ok, genie_response = _test_genie(space_id)
        if genie_ok:
            scores["genie"] = 5
            log(f"Genie: responded correctly [+5] ({genie_response})")
        else:
            scores["genie"] = 2
            log(f"Genie: space exists but failed test [+2] ({genie_response})")
    else:
        log("Genie: no space ID provided [+0]")

    # ─── PRESENTATION (7 pts) — manual ───
    scores["presentation"] = PRESENTATION_SCORES.get(team_name, 0)
    if scores["presentation"] > 0:
        log(f"Presentation: judge scored [+{scores['presentation']}]")
    else:
        log("Presentation: not yet scored (enter in PRESENTATION_SCORES)")

    # ─── BONUS (5 pts) ───
    # Published dashboard / deployed app (+1)
    if scores["deliverable_type"] == "app" and app_running:
        scores["bonus"] += 1
        log("Bonus: app is actively deployed [+1]")
    elif dash_found:
        try:
            for d in w.lakeview.list():
                if d.display_name and team_name.lower() in d.display_name.lower():
                    pub = w.lakeview.get_published(d.dashboard_id)
                    if pub:
                        scores["bonus"] += 1
                        log("Bonus: dashboard is published [+1]")
                    break
        except Exception:
            pass

    # executive_briefing with disease_prevalence_pct column (+2 proxy for filters/quality)
    if _table_exists(catalog, "executive_briefing"):
        try:
            cols = [c.lower() for c in spark.table(f"{catalog}.{SCHEMA}.executive_briefing").columns]
            if "disease_prevalence_pct" in cols and "total_patients" in cols:
                scores["bonus"] += 2
                log("Bonus: briefing has structured metrics (dashboard filter-ready) [+2]")
        except Exception:
            pass

    scores["total"] = sum(scores[k] for k in ["artifacts", "briefing", "deliverable", "genie", "presentation", "bonus"])
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
    d_max = 15 if r["deliverable_type"] == "app" else 10
    print(f"  Artifacts:{r['artifacts']}/3  Briefing:{r['briefing']}/5  {r['deliverable_type'].title()}:{r['deliverable']}/{d_max}  Genie:{r['genie']}/5  Pres:{r['presentation']}/7  Bonus:{r['bonus']}/5")
    t_max = 30 if r["deliverable_type"] != "app" else 35
    print(f"  TOTAL: {r['total']}/{t_max}")
    for d in r["details"]:
        print(f"    {d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leaderboard

# COMMAND ----------

df_scores = pd.DataFrame([{
    "Team": r["team"],
    "Artifacts": r["artifacts"],
    "Briefing": r["briefing"],
    "Deliverable_Type": r["deliverable_type"],
    "Deliverable": r["deliverable"],
    "Genie": r["genie"],
    "Presentation": r["presentation"],
    "Bonus": r["bonus"],
    "Total": r["total"],
} for r in results]).sort_values("Total", ascending=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

spark.createDataFrame(df_scores).write.format("delta").mode("overwrite").saveAsTable(
    "dataops_olympics.default.event5_scores"
)
print("Saved to dataops_olympics.default.event5_scores")

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
_event = "Event 5: Capstone"

for r in results:
    _t = r["team"]
    if spark.sql(f"SELECT 1 FROM {_RT} WHERE team = '{_t}'").count() == 0:
        spark.sql(f"INSERT INTO {_RT} VALUES ('{_t}')")
    _d_mx = 15 if r["deliverable_type"] == "app" else 10
    for cat, pts, mx in [
        ("Artifacts", r["artifacts"], 3), ("Briefing", r["briefing"], 5),
        ("Deliverable", r["deliverable"], _d_mx), ("Genie", r["genie"], 5),
        ("Presentation", r["presentation"], 7), ("Bonus", r["bonus"], 5),
    ]:
        spark.sql(f"INSERT INTO {_LB} VALUES ('{_t}', '{_event}', '{cat}', {pts}, {mx}, '{_now}')")

print(f"Leaderboard updated: {len(results)} teams × 6 categories")
