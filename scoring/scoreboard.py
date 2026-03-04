# Databricks notebook source
# MAGIC %md
# MAGIC # DataOps Olympics -- Live Scoreboard
# MAGIC
# MAGIC **Run this notebook ONCE to create the AI/BI Dashboard, then it auto-refreshes.**
# MAGIC
# MAGIC ### How It Works
# MAGIC
# MAGIC 1. Each event's `scoring.py` **appends** component-level rows to `olympics_leaderboard`
# MAGIC 2. The dashboard queries this table live -- always showing the **best score** per category
# MAGIC 3. Teams with no submissions appear with 0 (via `registered_teams` CROSS JOIN)
# MAGIC 4. Re-run this notebook only if you need to change the dashboard structure

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Auto-Discover Teams from Catalogs

# COMMAND ----------

CATALOG = "dataops_olympics"
SCHEMA = "default"
DASHBOARD_NAME = "DataOps Olympics -- Live Scoreboard"
EXCLUDE_CATALOGS = {"dataops_olympics", "workspace", "system", "samples", "hive_metastore", "main", "__databricks_internal"}

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

spark.sql("CREATE TABLE IF NOT EXISTS registered_teams (team STRING)")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS olympics_leaderboard (
        team STRING, event STRING, category STRING,
        points DOUBLE, max_points DOUBLE, scored_at TIMESTAMP
    )
""")

catalogs = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
teams = sorted([c for c in catalogs if c not in EXCLUDE_CATALOGS])

for t in teams:
    if spark.sql(f"SELECT 1 FROM registered_teams WHERE team = '{t}'").count() == 0:
        spark.sql(f"INSERT INTO registered_teams VALUES ('{t}')")

registered = spark.sql("SELECT DISTINCT team FROM registered_teams ORDER BY team").toPandas()["team"].tolist()
print(f"Registered teams ({len(registered)}): {registered}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Preview Current Standings

# COMMAND ----------

display(spark.sql("""
    WITH best AS (
        SELECT team, event, category, MAX(points) AS points, MAX(max_points) AS max_points
        FROM olympics_leaderboard
        GROUP BY team, event, category
    ),
    by_event AS (
        SELECT team, event, ROUND(SUM(points), 1) AS points, ROUND(SUM(max_points), 1) AS max_points
        FROM best GROUP BY team, event
    ),
    all_teams AS (SELECT DISTINCT team FROM registered_teams),
    all_events AS (
        SELECT 'Event 1: Data Engineering' AS event, 55.0 AS max_total UNION ALL
        SELECT 'Event 2: Data Analytics', 48.0 UNION ALL
        SELECT 'Event 3: Data Science', 48.0 UNION ALL
        SELECT 'Event 4: GenAI Agents', 48.0 UNION ALL
        SELECT 'Event 5: Capstone', 35.0
    )
    SELECT t.team,
           COALESCE(ROUND(SUM(b.points), 1), 0) AS total_points,
           234.0 AS max_possible,
           COALESCE(ROUND(SUM(b.points) * 100.0 / 234.0, 1), 0) AS pct,
           COUNT(DISTINCT b.event) AS events_scored
    FROM all_teams t
    LEFT JOIN by_event b ON t.team = b.team
    GROUP BY t.team
    ORDER BY total_points DESC
"""))

# COMMAND ----------

display(spark.sql("""
    WITH best AS (
        SELECT team, event, category, MAX(points) AS points, MAX(max_points) AS max_points
        FROM olympics_leaderboard GROUP BY team, event, category
    )
    SELECT team, event, category, points, max_points
    FROM best ORDER BY team, event, category
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Build Dashboard JSON

# COMMAND ----------

import json, uuid

def _uid():
    return uuid.uuid4().hex[:12]

LB = f"{CATALOG}.{SCHEMA}.olympics_leaderboard"
RT = f"{CATALOG}.{SCHEMA}.registered_teams"

ALL_EVENTS_SQL = ("SELECT 'Event 1: Data Engineering' AS event UNION ALL "
    "SELECT 'Event 2: Data Analytics' UNION ALL SELECT 'Event 3: Data Science' UNION ALL "
    "SELECT 'Event 4: GenAI Agents' UNION ALL SELECT 'Event 5: Capstone'")

BEST_CTE = f"WITH best AS (SELECT team, event, category, MAX(points) AS points FROM {LB} GROUP BY team, event, category)"

datasets = [
    {"name": "ds_cumulative", "displayName": "Cumulative Leaderboard",
     "query": f"{BEST_CTE}, totals AS (SELECT team, ROUND(SUM(points), 1) AS total_points FROM best GROUP BY team), last_sub AS (SELECT team, MAX(scored_at) AS last_scored FROM {LB} GROUP BY team), all_teams AS (SELECT DISTINCT team FROM {RT}) SELECT t.team AS Team, COALESCE(s.total_points, 0) AS total_points, 234 AS max_possible, ls.last_scored AS Last_Submission FROM all_teams t LEFT JOIN totals s ON t.team = s.team LEFT JOIN last_sub ls ON t.team = ls.team ORDER BY total_points DESC",
     "columns": [{"fieldName": "Team", "displayName": "Team"}, {"fieldName": "total_points", "displayName": "Total Points"}, {"fieldName": "max_possible", "displayName": "Max Possible"}, {"fieldName": "Last_Submission", "displayName": "Last Submission"}]},
    {"name": "ds_by_event", "displayName": "Points by Event",
     "query": f"{BEST_CTE}, by_event AS (SELECT team, event, ROUND(SUM(points), 1) AS points FROM best GROUP BY team, event), all_teams AS (SELECT DISTINCT team FROM {RT}), all_events AS ({ALL_EVENTS_SQL}) SELECT t.team AS Team, e.event AS Event, COALESCE(b.points, 0) AS Points FROM all_teams t CROSS JOIN all_events e LEFT JOIN by_event b ON t.team = b.team AND b.event = e.event ORDER BY t.team, e.event",
     "columns": [{"fieldName": "Team", "displayName": "Team"}, {"fieldName": "Event", "displayName": "Event"}, {"fieldName": "Points", "displayName": "Points"}]},
    {"name": "ds_breakdown", "displayName": "Component Breakdown",
     "query": f"WITH best AS (SELECT team, event, category, MAX(points) AS points, MAX(max_points) AS max_points FROM {LB} GROUP BY team, event, category) SELECT team AS Team, event AS Event, category AS Category, points AS Points, max_points AS Max_Points FROM best ORDER BY team, event, category",
     "columns": [{"fieldName": "Team", "displayName": "Team"}, {"fieldName": "Event", "displayName": "Event"}, {"fieldName": "Category", "displayName": "Category"}, {"fieldName": "Points", "displayName": "Points"}, {"fieldName": "Max_Points", "displayName": "Max Points"}]},
    {"name": "ds_history", "displayName": "Score History",
     "query": f"SELECT team AS Team, event AS Event, category AS Category, points AS Points, max_points AS Max_Points, scored_at FROM {LB} ORDER BY scored_at DESC LIMIT 200",
     "columns": [{"fieldName": "Team", "displayName": "Team"}, {"fieldName": "Event", "displayName": "Event"}, {"fieldName": "Category", "displayName": "Category"}, {"fieldName": "Points", "displayName": "Points"}, {"fieldName": "Max_Points", "displayName": "Max Points"}, {"fieldName": "scored_at", "displayName": "Scored At"}]},
    {"name": "ds_kpi_teams", "displayName": "KPI: Total Teams",
     "query": f"SELECT COUNT(DISTINCT team) AS total_teams FROM {RT}"},
    {"name": "ds_kpi_points", "displayName": "KPI: Points Awarded",
     "query": f"{BEST_CTE} SELECT COALESCE(ROUND(SUM(points), 1), 0) AS total_points_awarded FROM best"},
    {"name": "ds_kpi_events", "displayName": "KPI: Events Scored",
     "query": f"SELECT COUNT(DISTINCT event) AS events_scored FROM {LB}"},
    {"name": "ds_kpi_last_sub", "displayName": "KPI: Last Submission",
     "query": f"SELECT MAX(scored_at) AS last_submission FROM {LB}"},
    {"name": "ds_rankings", "displayName": "Team Rankings",
     "query": f"{BEST_CTE}, totals AS (SELECT team, ROUND(SUM(points), 1) AS total_points, COUNT(DISTINCT event) AS events_completed FROM best GROUP BY team), last_sub AS (SELECT team, MAX(scored_at) AS last_scored FROM {LB} GROUP BY team), all_teams AS (SELECT DISTINCT team FROM {RT}) SELECT ROW_NUMBER() OVER (ORDER BY COALESCE(s.total_points, 0) DESC) AS Rank, t.team AS Team, COALESCE(s.total_points, 0) AS Points, ROUND(COALESCE(s.total_points, 0) * 100.0 / 234.0, 1) AS Pct, COALESCE(s.events_completed, 0) AS Events_Done, COALESCE(s.total_points, 0) - FIRST_VALUE(COALESCE(s.total_points, 0)) OVER (ORDER BY COALESCE(s.total_points, 0) DESC) AS Gap_To_Leader, ls.last_scored AS Last_Submission FROM all_teams t LEFT JOIN totals s ON t.team = s.team LEFT JOIN last_sub ls ON t.team = ls.team ORDER BY Points DESC",
     "columns": [{"fieldName": "Rank", "displayName": "#"}, {"fieldName": "Team", "displayName": "Team"}, {"fieldName": "Points", "displayName": "Points"}, {"fieldName": "Pct", "displayName": "% of Max"}, {"fieldName": "Events_Done", "displayName": "Events Done"}, {"fieldName": "Gap_To_Leader", "displayName": "Gap to 1st"}, {"fieldName": "Last_Submission", "displayName": "Last Submission"}]},
    {"name": "ds_completion", "displayName": "Event Completion",
     "query": f"{BEST_CTE}, by_event AS (SELECT team, event, ROUND(SUM(points), 1) AS points FROM best GROUP BY team, event), evt_times AS (SELECT team, event, MIN(scored_at) AS scored_at FROM {LB} GROUP BY team, event), all_teams AS (SELECT DISTINCT team FROM {RT}), all_events AS ({ALL_EVENTS_SQL}) SELECT t.team AS Team, e.event AS Event, CASE WHEN b.points > 0 THEN 'Submitted' ELSE 'Not Started' END AS Status, COALESCE(b.points, 0) AS Points, et.scored_at AS Submitted_At FROM all_teams t CROSS JOIN all_events e LEFT JOIN by_event b ON t.team = b.team AND b.event = e.event LEFT JOIN evt_times et ON t.team = et.team AND e.event = et.event ORDER BY e.event, t.team",
     "columns": [{"fieldName": "Team", "displayName": "Team"}, {"fieldName": "Event", "displayName": "Event"}, {"fieldName": "Status", "displayName": "Status"}, {"fieldName": "Points", "displayName": "Points"}, {"fieldName": "Submitted_At", "displayName": "Submitted At"}]},
    {"name": "ds_completion_pct", "displayName": "Completion Rate by Event",
     "query": f"{BEST_CTE}, by_event AS (SELECT team, event, ROUND(SUM(points), 1) AS points FROM best GROUP BY team, event), team_count AS (SELECT COUNT(DISTINCT team) AS cnt FROM {RT}), all_events AS ({ALL_EVENTS_SQL}) SELECT e.event AS Event, COUNT(DISTINCT b.team) AS Teams_Submitted, tc.cnt AS Total_Teams, ROUND(COUNT(DISTINCT b.team) * 100.0 / tc.cnt, 0) AS Completion_Pct FROM all_events e CROSS JOIN team_count tc LEFT JOIN by_event b ON b.event = e.event AND b.points > 0 GROUP BY e.event, tc.cnt ORDER BY e.event"},
    {"name": "ds_gap_to_leader", "displayName": "Gap to Leader",
     "query": f"{BEST_CTE}, totals AS (SELECT team, ROUND(SUM(points), 1) AS total_points FROM best GROUP BY team), all_teams AS (SELECT DISTINCT team FROM {RT}), ranked AS (SELECT t.team AS Team, COALESCE(s.total_points, 0) AS Points FROM all_teams t LEFT JOIN totals s ON t.team = s.team), leader AS (SELECT MAX(Points) AS max_pts FROM ranked) SELECT r.Team, r.Points, r.Points - l.max_pts AS Gap FROM ranked r CROSS JOIN leader l WHERE r.Points < l.max_pts ORDER BY r.Points DESC"},
    {"name": "ds_timeline", "displayName": "Scoring Timeline",
     "query": f"SELECT DATE_TRUNC('MINUTE', scored_at) AS scored_minute, COUNT(*) AS submissions FROM {LB} GROUP BY DATE_TRUNC('MINUTE', scored_at) ORDER BY scored_minute"},
    {"name": "ds_e1_paths", "displayName": "Event 1 Path Distribution",
     "query": f"SELECT Path, COUNT(*) AS team_count FROM {CATALOG}.{SCHEMA}.event1_scores GROUP BY Path"},
    {"name": "ds_e3_f1", "displayName": "Event 3 F1 Scores",
     "query": f"SELECT Team, F1 FROM {CATALOG}.{SCHEMA}.event3_scores WHERE F1 IS NOT NULL ORDER BY F1 DESC"},
]

event_tables = {
    "Event 1 Detail": {"table": "event1_scores", "columns": [
        {"fieldName": "Team", "displayName": "Team"}, {"fieldName": "Path", "displayName": "Path"},
        {"fieldName": "Bronze", "displayName": "Bronze"}, {"fieldName": "Silver", "displayName": "Silver"},
        {"fieldName": "Gold", "displayName": "Gold"}, {"fieldName": "DQ", "displayName": "DQ"},
        {"fieldName": "Governance", "displayName": "Governance"}, {"fieldName": "Bonus", "displayName": "Bonus"},
        {"fieldName": "Total", "displayName": "Total"}]},
    "Event 2 Detail": {"table": "event2_scores", "columns": [
        {"fieldName": "Team", "displayName": "Team"}, {"fieldName": "Build_SQL", "displayName": "Build SQL"},
        {"fieldName": "Build_Genie", "displayName": "Build Genie"}, {"fieldName": "Benchmark", "displayName": "Benchmark"},
        {"fieldName": "Speed", "displayName": "Speed"}, {"fieldName": "Bonus", "displayName": "Bonus"},
        {"fieldName": "Total", "displayName": "Total"}]},
    "Event 3 Detail": {"table": "event3_scores", "columns": [
        {"fieldName": "Team", "displayName": "Team"}, {"fieldName": "EDA", "displayName": "EDA"},
        {"fieldName": "Features", "displayName": "Features"}, {"fieldName": "MLflow", "displayName": "MLflow"},
        {"fieldName": "Performance", "displayName": "Performance"}, {"fieldName": "Registration", "displayName": "Registration"},
        {"fieldName": "Bonus", "displayName": "Bonus"}, {"fieldName": "Total", "displayName": "Total"},
        {"fieldName": "F1", "displayName": "F1 Score"}]},
    "Event 4 Detail": {"table": "event4_scores", "columns": [
        {"fieldName": "Team", "displayName": "Team"}, {"fieldName": "Exploration", "displayName": "Exploration"},
        {"fieldName": "SystemPrompt", "displayName": "System Prompt"}, {"fieldName": "AgentFunction", "displayName": "Agent Function"},
        {"fieldName": "AIFunctions", "displayName": "AI Functions"}, {"fieldName": "TestPrompts", "displayName": "Test Prompts"},
        {"fieldName": "Bonus", "displayName": "Bonus"}, {"fieldName": "Total", "displayName": "Total"}]},
    "Event 5 Detail": {"table": "event5_scores", "columns": [
        {"fieldName": "Team", "displayName": "Team"}, {"fieldName": "Artifacts", "displayName": "Artifacts"},
        {"fieldName": "Briefing", "displayName": "Briefing"}, {"fieldName": "Dashboard", "displayName": "Dashboard"},
        {"fieldName": "Genie", "displayName": "Genie"}, {"fieldName": "Presentation", "displayName": "Presentation"},
        {"fieldName": "Bonus", "displayName": "Bonus"}, {"fieldName": "Total", "displayName": "Total"}]},
}
for label, info in event_tables.items():
    try:
        cnt = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{info['table']}").collect()[0][0]
        if cnt > 0:
            datasets.append({"name": f"ds_{_uid()}", "displayName": label,
                "query": f"SELECT * FROM {CATALOG}.{SCHEMA}.{info['table']} ORDER BY Total DESC",
                "columns": info["columns"]})
    except Exception:
        pass

print(f"Dashboard datasets: {len(datasets)}")
for ds in datasets:
    print(f"  - {ds['displayName']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widget Helpers

# COMMAND ----------

def _make_text(name, text, x, y, w=6, h=1):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": [text]}},
            "position": {"x": x, "y": y, "width": w, "height": h}}

def _make_bar(name, ds_name, x_field, y_field, title, color_field=None, x=0, y=0, w=6, h=4, sort_x=None, label=False):
    fields = [{"name": x_field, "expression": f"`{x_field}`"}, {"name": y_field, "expression": f"`{y_field}`"}]
    x_scale = {"type": "categorical"}
    if sort_x:
        x_scale["sort"] = {"by": sort_x}
    enc = {"x": {"fieldName": x_field, "scale": x_scale, "displayName": x_field},
           "y": {"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y_field}}
    if label:
        enc["label"] = {"show": True}
    if color_field:
        fields.append({"name": color_field, "expression": f"`{color_field}`"})
        enc["color"] = {"fieldName": color_field, "scale": {"type": "categorical"}, "displayName": color_field}
    return {"widget": {"name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": ds_name, "fields": fields, "disaggregated": True}}],
        "spec": {"version": 3, "widgetType": "bar", "encodings": enc, "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

def _make_table(name, ds_name, title, columns=None, x=0, y=0, w=6, h=4):
    ds = next((d for d in datasets if d["name"] == ds_name), None)
    cols = columns or (ds.get("columns") if ds else None) or []
    fields = [{"name": c["fieldName"], "expression": f'`{c["fieldName"]}`'} for c in cols] if cols else [{"name": "*", "expression": "*"}]
    enc = {"columns": cols} if cols else {}
    return {"widget": {"name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": ds_name, "fields": fields, "disaggregated": True}}],
        "spec": {"version": 2, "widgetType": "table", "encodings": enc, "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

def _make_counter(name, ds_name, field, title, x=0, y=0, w=2, h=3):
    return {"widget": {"name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": ds_name, "fields": [{"name": field, "expression": f"`{field}`"}], "disaggregated": True}}],
        "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": {"fieldName": field, "displayName": title}}, "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

def _make_pie(name, ds_name, angle_field, color_field, title, x=0, y=0, w=3, h=5):
    return {"widget": {"name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": ds_name,
            "fields": [{"name": angle_field, "expression": f"`{angle_field}`"}, {"name": color_field, "expression": f"`{color_field}`"}], "disaggregated": True}}],
        "spec": {"version": 3, "widgetType": "pie", "encodings": {
            "angle": {"fieldName": angle_field, "scale": {"type": "quantitative"}, "displayName": angle_field},
            "color": {"fieldName": color_field, "scale": {"type": "categorical"}, "displayName": color_field}
        }, "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

def _make_line(name, ds_name, x_field, y_field, title, x=0, y=0, w=6, h=5):
    return {"widget": {"name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": ds_name,
            "fields": [{"name": x_field, "expression": f"`{x_field}`"}, {"name": y_field, "expression": f"`{y_field}`"}], "disaggregated": True}}],
        "spec": {"version": 3, "widgetType": "line", "encodings": {
            "x": {"fieldName": x_field, "scale": {"type": "temporal"}, "displayName": "Time"},
            "y": {"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y_field}
        }, "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Page 1: Scoreboard (participant-facing)

# COMMAND ----------

page1 = [
    _make_text("title", "# DataOps Olympics -- Live Scoreboard", 0, 0),
    _make_bar("bar_cumulative", "ds_cumulative", "Team", "total_points", "Cumulative Leaderboard (Best Scores)", sort_x="y-reversed", label=True, y=1, h=4),
    _make_bar("bar_by_event", "ds_by_event", "Team", "Points", "Points by Event (Stacked)", color_field="Event", y=5, h=5),
    _make_table("tbl_standings", "ds_cumulative", "Overall Standings", y=10, h=3),
    _make_bar("bar_breakdown", "ds_breakdown", "Team", "Points", "Component Breakdown", color_field="Category", y=13, h=5),
    _make_table("tbl_detailed", "ds_breakdown", "Detailed Scores", y=18, h=5),
    _make_table("tbl_recent", "ds_history", "Recent Submissions", y=23, h=4),
]
_evt_y = 27
for ds in datasets:
    if ds["displayName"].startswith("Event") and ds["displayName"].endswith("Detail"):
        page1.append(_make_table(f"tbl_{ds['name']}", ds["name"], ds["displayName"].replace(" Detail", ""), y=_evt_y, h=3))
        _evt_y += 3

# COMMAND ----------

# MAGIC %md
# MAGIC ### Page 2: Organizer Command Center

# COMMAND ----------

page2 = [
    _make_text("org_title", "# Organizer Command Center", 0, 0),
    _make_counter("kpi_teams", "ds_kpi_teams", "total_teams", "Registered Teams", x=0, y=1, w=2, h=3),
    _make_counter("kpi_points", "ds_kpi_points", "total_points_awarded", "Points Awarded", x=2, y=1, w=2, h=3),
    _make_counter("kpi_events", "ds_kpi_events", "events_scored", "Events Scored (of 5)", x=4, y=1, w=2, h=3),
    _make_text("org_rank_hdr", "## Rankings & Standings", 0, 4),
    _make_table("tbl_rankings", "ds_rankings", "Full Rankings", y=5, h=5),
    _make_bar("bar_gap", "ds_gap_to_leader", "Team", "Gap", "Gap to Leader (points behind 1st place)", sort_x="y", label=True, y=10, h=4),
    _make_text("org_comp_hdr", "## Event Completion Tracking", 0, 14),
    _make_bar("bar_completion", "ds_completion_pct", "Event", "Completion_Pct", "Completion Rate by Event (%)", label=True, x=0, y=15, w=3, h=5),
    _make_table("tbl_completion", "ds_completion", "Team x Event Status", x=3, y=15, w=3, h=5),
    _make_text("org_insights_hdr", "## Event Insights", 0, 20),
    _make_pie("pie_e1_path", "ds_e1_paths", "team_count", "Path", "Event 1: SDP vs SQL Path", x=0, y=21),
    _make_bar("bar_e3_f1", "ds_e3_f1", "Team", "F1", "Event 3: ML Model F1 Scores", sort_x="y-reversed", label=True, x=3, y=21, w=3, h=5),
    _make_line("line_timeline", "ds_timeline", "scored_minute", "submissions", "Scoring Activity Timeline", y=26, h=5),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assemble Dashboard

# COMMAND ----------

serialized = json.dumps({
    "pages": [
        {"name": "scoreboard", "displayName": "Scoreboard", "pageType": "PAGE_TYPE_CANVAS", "layout": page1},
        {"name": "organizer", "displayName": "Organizer", "pageType": "PAGE_TYPE_CANVAS", "layout": page2},
    ],
    "datasets": [{"name": d["name"], "displayName": d["displayName"], "queryLines": [d["query"]]} for d in datasets],
    "uiSettings": {"theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"}, "applyModeEnabled": False},
})
print(f"Dashboard: {len(serialized)} chars, Page 1: {len(page1)} widgets, Page 2: {len(page2)} widgets, {len(datasets)} datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create / Update Dashboard via Lakeview API

# COMMAND ----------

import urllib.request as _req

_ws_url = spark.conf.get("spark.databricks.workspaceUrl", "")
_host = f"https://{_ws_url}"
_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

def _api(method, path, body=None):
    data = json.dumps(body).encode() if body else None
    r = _req.Request(f"{_host}{path}", data=data, method=method,
        headers={"Authorization": f"Bearer {_token}", "Content-Type": "application/json"})
    return json.loads(_req.urlopen(r).read())

existing_id = None
try:
    for d in _api("GET", "/api/2.0/lakeview/dashboards").get("dashboards", []):
        if d.get("display_name") == DASHBOARD_NAME:
            existing_id = d["dashboard_id"]
            break
except Exception as e:
    print(f"List note: {e}")

if existing_id:
    print(f"Updating: {existing_id}")
    _api("PATCH", f"/api/2.0/lakeview/dashboards/{existing_id}", {
        "display_name": DASHBOARD_NAME, "serialized_dashboard": serialized
    })
    dashboard_id = existing_id
else:
    user_email = spark.sql("SELECT current_user()").collect()[0][0]
    result = _api("POST", "/api/2.0/lakeview/dashboards", {
        "display_name": DASHBOARD_NAME, "serialized_dashboard": serialized,
        "parent_path": f"/Workspace/Users/{user_email}/gsk-dataops-olympics/scoring"
    })
    dashboard_id = result["dashboard_id"]
    print(f"Created: {dashboard_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Publish

# COMMAND ----------

try:
    warehouses = _api("GET", "/api/2.0/sql/warehouses").get("warehouses", [])
    wh_id = next((w["id"] for w in warehouses if w.get("state") == "RUNNING"), warehouses[0]["id"] if warehouses else "")
    _api("POST", f"/api/2.0/lakeview/dashboards/{dashboard_id}/published", {"embed_credentials": True, "warehouse_id": wh_id})
    print(f"Dashboard published with warehouse: {wh_id}")
except Exception as e:
    print(f"Publish note: {e}")

print(f"\nDashboard: https://{_ws_url}/dashboardsv3/{dashboard_id}")
print("The dashboard auto-refreshes from live data. No need to re-run this notebook.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Winner Announcement

# COMMAND ----------

from datetime import datetime

standings = spark.sql(f"""
    WITH best AS (
        SELECT team, event, category, MAX(points) AS points
        FROM {LB} GROUP BY team, event, category
    )
    SELECT team, ROUND(SUM(points), 1) AS total,
           COUNT(DISTINCT event) AS events
    FROM best GROUP BY team ORDER BY total DESC
""").toPandas()

all_teams = spark.sql(f"SELECT DISTINCT team FROM {RT} ORDER BY team").toPandas()["team"].tolist()

print("=" * 60)
print(f"  DATAOPS OLYMPICS STANDINGS -- {datetime.now().strftime('%H:%M:%S')}")
print("=" * 60)

scored_teams = set(standings["team"].tolist())
rank = 1
for _, row in standings.iterrows():
    medal = {1: "[GOLD]  ", 2: "[SILVER]", 3: "[BRONZE]"}.get(rank, "        ")
    print(f"  {rank}. {medal} {row['team']:12s} | {row['total']:6.1f} pts | {int(row['events'])} events")
    rank += 1

for t in all_teams:
    if t not in scored_teams:
        print(f"  {rank}. {'        '} {t:12s} |    0.0 pts | 0 events")
        rank += 1

print("=" * 60)
