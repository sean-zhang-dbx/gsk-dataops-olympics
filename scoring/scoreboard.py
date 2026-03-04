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

datasets = [
    {
        "name": f"ds_{_uid()}",
        "displayName": "Cumulative Leaderboard",
        "query": f"""
            WITH best AS (
                SELECT team, event, category, MAX(points) AS points
                FROM {LB} GROUP BY team, event, category
            ),
            totals AS (
                SELECT team, ROUND(SUM(points), 1) AS total_points
                FROM best GROUP BY team
            ),
            all_teams AS (SELECT DISTINCT team FROM {RT})
            SELECT t.team AS Team,
                   COALESCE(s.total_points, 0) AS total_points,
                   234 AS max_possible
            FROM all_teams t
            LEFT JOIN totals s ON t.team = s.team
            ORDER BY total_points DESC
        """
    },
    {
        "name": f"ds_{_uid()}",
        "displayName": "Points by Event",
        "query": f"""
            WITH best AS (
                SELECT team, event, category, MAX(points) AS points
                FROM {LB} GROUP BY team, event, category
            ),
            by_event AS (
                SELECT team, event, ROUND(SUM(points), 1) AS points
                FROM best GROUP BY team, event
            ),
            all_teams AS (SELECT DISTINCT team FROM {RT}),
            all_events AS (
                SELECT 'Event 1: Data Engineering' AS event UNION ALL
                SELECT 'Event 2: Data Analytics' UNION ALL
                SELECT 'Event 3: Data Science' UNION ALL
                SELECT 'Event 4: GenAI Agents' UNION ALL
                SELECT 'Event 5: Capstone'
            )
            SELECT t.team AS Team, e.event,
                   COALESCE(b.points, 0) AS points
            FROM all_teams t
            CROSS JOIN all_events e
            LEFT JOIN by_event b ON t.team = b.team AND b.event = e.event
            ORDER BY t.team, e.event
        """
    },
    {
        "name": f"ds_{_uid()}",
        "displayName": "Component Breakdown",
        "query": f"""
            WITH best AS (
                SELECT team, event, category,
                       MAX(points) AS points, MAX(max_points) AS max_points
                FROM {LB} GROUP BY team, event, category
            )
            SELECT team AS Team, event, category, points, max_points
            FROM best
            ORDER BY team, event, category
        """
    },
    {
        "name": f"ds_{_uid()}",
        "displayName": "Score History",
        "query": f"""
            SELECT team AS Team, event, category, points, max_points, scored_at
            FROM {LB}
            ORDER BY scored_at DESC
            LIMIT 200
        """
    },
]

event_tables = {
    "Event 1 Detail": "event1_scores",
    "Event 2 Detail": "event2_scores",
    "Event 3 Detail": "event3_scores",
    "Event 4 Detail": "event4_scores",
    "Event 5 Detail": "event5_scores",
}
for label, table in event_tables.items():
    try:
        cnt = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{table}").collect()[0][0]
        if cnt > 0:
            datasets.append({
                "name": f"ds_{_uid()}",
                "displayName": label,
                "query": f"SELECT * FROM {CATALOG}.{SCHEMA}.{table} ORDER BY Total DESC"
            })
    except Exception:
        pass

print(f"Dashboard datasets: {len(datasets)}")
for ds in datasets:
    print(f"  - {ds['displayName']}")

# COMMAND ----------

page_name = _uid()
widgets = []
_y = [0]

def _add_text(text, x=0, w=6, h=1):
    widgets.append({
        "widget": {"name": f"w_{_uid()}", "textbox_spec": text},
        "position": {"x": x, "y": _y[0], "width": w, "height": h}
    })
    _y[0] += h

def _add_bar(ds_name, x_field, y_field, title, color_field=None, w=6, h=4):
    fields = [
        {"name": x_field, "expression": f"`{x_field}`"},
        {"name": y_field, "expression": f"`{y_field}`"},
    ]
    enc = {
        "x": {"fieldName": x_field, "scale": {"type": "categorical"}, "displayName": x_field},
        "y": {"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y_field},
    }
    if color_field:
        fields.append({"name": color_field, "expression": f"`{color_field}`"})
        enc["color"] = {"fieldName": color_field, "scale": {"type": "categorical"}, "displayName": color_field}
    widgets.append({
        "widget": {
            "name": f"w_{_uid()}",
            "queries": [{"name": f"q_{_uid()}", "query": {"datasetName": ds_name, "fields": fields, "disaggregated": True}}],
            "spec": {"version": 3, "widgetType": "bar", "encodings": enc, "frame": {"showTitle": True, "title": title}}
        },
        "position": {"x": 0, "y": _y[0], "width": w, "height": h}
    })
    _y[0] += h

def _add_table(ds_name, title, w=6, h=4):
    widgets.append({
        "widget": {
            "name": f"w_{_uid()}",
            "queries": [{"name": f"q_{_uid()}", "query": {"datasetName": ds_name, "fields": [{"name": "*", "expression": "*"}], "disaggregated": True}}],
            "spec": {"version": 3, "widgetType": "table", "encodings": {}, "frame": {"showTitle": True, "title": title}}
        },
        "position": {"x": 0, "y": _y[0], "width": w, "height": h}
    })
    _y[0] += h

cumul = datasets[0]["name"]
by_evt = datasets[1]["name"]
breakdown = datasets[2]["name"]
history = datasets[3]["name"]

_add_text("# DataOps Olympics -- Live Scoreboard", w=6, h=1)
_add_bar(cumul, "Team", "total_points", "Cumulative Leaderboard (Best Scores)", w=6, h=4)
_add_bar(by_evt, "Team", "points", "Points by Event", color_field="event", w=6, h=5)
_add_table(cumul, "Overall Standings", w=6, h=3)
_add_bar(breakdown, "Team", "points", "Component Breakdown (All Events)", color_field="category", w=6, h=5)
_add_table(breakdown, "Detailed Component Scores", w=6, h=5)
_add_table(history, "Recent Score Submissions", w=6, h=4)

for ds in datasets[4:]:
    _add_table(ds["name"], ds["displayName"], w=6, h=3)

serialized = json.dumps({
    "pages": [{"name": page_name, "displayName": "Scoreboard", "layout": widgets}],
    "datasets": [{"name": d["name"], "displayName": d["displayName"], "query": d["query"]} for d in datasets]
})
print(f"Dashboard: {len(serialized)} chars, {len(widgets)} widgets, {len(datasets)} datasets")

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
    _api("POST", f"/api/2.0/lakeview/dashboards/{dashboard_id}/published", {"embed_credentials": True, "warehouse_id": ""})
    print("Dashboard published!")
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
