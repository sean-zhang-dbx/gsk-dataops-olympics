# Databricks notebook source
# MAGIC %md
# MAGIC # DataOps Olympics — Live Scoreboard
# MAGIC
# MAGIC **Run this notebook after scoring each event to create / refresh the unified AI/BI Dashboard.**
# MAGIC
# MAGIC The dashboard is created programmatically via the Lakeview API and published
# MAGIC so that anyone in the workspace can view it. Project it on a big screen!
# MAGIC
# MAGIC ### How It Works
# MAGIC
# MAGIC 1. Each event's `scoring.py` saves results to `dataops_olympics.default.eventN_scores`
# MAGIC 2. Each also writes a summary row to `dataops_olympics.default.olympics_leaderboard`
# MAGIC 3. This notebook reads those tables and creates a single AI/BI Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

DASHBOARD_NAME = "DataOps Olympics — Live Scoreboard"
CATALOG = "dataops_olympics"
SCHEMA = "default"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview: Current Standings

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS dataops_olympics.default.olympics_leaderboard (
        Team STRING, event STRING, points DOUBLE, max_points DOUBLE
    )
""")

leaderboard = spark.sql("""
    SELECT Team,
           ROUND(SUM(points), 1) AS total_points,
           ROUND(SUM(max_points), 1) AS max_possible,
           ROUND(SUM(points) * 100.0 / SUM(max_points), 1) AS pct_achieved,
           COUNT(DISTINCT event) AS events_completed
    FROM olympics_leaderboard
    GROUP BY Team
    ORDER BY total_points DESC
""")
display(leaderboard)

# COMMAND ----------

display(spark.sql("""
    SELECT Team, event, points, max_points,
           ROUND(points * 100.0 / max_points, 1) AS pct
    FROM olympics_leaderboard
    ORDER BY Team, event
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the AI/BI Dashboard

# COMMAND ----------

import json, uuid

def _uid():
    return uuid.uuid4().hex[:12]

datasets = [
    {
        "name": f"ds_{_uid()}",
        "displayName": "Cumulative Leaderboard",
        "query": f"""
            SELECT Team,
                   ROUND(SUM(points), 1) AS total_points,
                   ROUND(SUM(max_points), 1) AS max_possible,
                   ROUND(SUM(points) * 100.0 / SUM(max_points), 1) AS pct_achieved,
                   COUNT(DISTINCT event) AS events_completed
            FROM {CATALOG}.{SCHEMA}.olympics_leaderboard
            GROUP BY Team
            ORDER BY total_points DESC
        """
    },
    {
        "name": f"ds_{_uid()}",
        "displayName": "Points by Event",
        "query": f"""
            SELECT Team, event, points, max_points,
                   ROUND(points * 100.0 / max_points, 1) AS pct
            FROM {CATALOG}.{SCHEMA}.olympics_leaderboard
            ORDER BY Team, event
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
        spark.table(f"{CATALOG}.{SCHEMA}.{table}")
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
        "widget": {
            "name": f"w_{_uid()}",
            "textbox_spec": text,
        },
        "position": {"x": x, "y": _y[0], "width": w, "height": h}
    })
    _y[0] += h

def _add_bar(dataset_name, x_field, y_field, title, color_field=None, x=0, w=6, h=4):
    fields = [
        {"name": x_field, "expression": f"`{x_field}`"},
        {"name": y_field, "expression": f"`{y_field}`"},
    ]
    encodings = {
        "x": {"fieldName": x_field, "scale": {"type": "categorical"}, "displayName": x_field},
        "y": {"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y_field},
    }
    if color_field:
        fields.append({"name": color_field, "expression": f"`{color_field}`"})
        encodings["color"] = {"fieldName": color_field, "scale": {"type": "categorical"}, "displayName": color_field}

    widgets.append({
        "widget": {
            "name": f"w_{_uid()}",
            "queries": [{"name": f"q_{_uid()}", "query": {
                "datasetName": dataset_name,
                "fields": fields,
                "disaggregated": True
            }}],
            "spec": {
                "version": 3,
                "widgetType": "bar",
                "encodings": encodings,
                "frame": {"showTitle": True, "title": title}
            }
        },
        "position": {"x": x, "y": _y[0], "width": w, "height": h}
    })
    _y[0] += h

def _add_table(dataset_name, title, x=0, w=6, h=4):
    widgets.append({
        "widget": {
            "name": f"w_{_uid()}",
            "queries": [{"name": f"q_{_uid()}", "query": {
                "datasetName": dataset_name,
                "fields": [{"name": "*", "expression": "*"}],
                "disaggregated": True
            }}],
            "spec": {
                "version": 3,
                "widgetType": "table",
                "encodings": {},
                "frame": {"showTitle": True, "title": title}
            }
        },
        "position": {"x": x, "y": _y[0], "width": w, "height": h}
    })
    _y[0] += h


_add_text("# DataOps Olympics -- Live Scoreboard", w=6, h=1)

cumulative_ds = datasets[0]["name"]
by_event_ds = datasets[1]["name"]

_add_bar(cumulative_ds, "Team", "total_points", "Cumulative Leaderboard", w=6, h=4)

_add_bar(by_event_ds, "Team", "points", "Points by Event (Stacked)", color_field="event", w=6, h=4)

_add_table(cumulative_ds, "Standings Summary", w=6, h=3)

for ds in datasets[2:]:
    _add_table(ds["name"], ds["displayName"], w=6, h=3)

serialized = json.dumps({
    "pages": [{
        "name": page_name,
        "displayName": "Scoreboard",
        "layout": widgets
    }],
    "datasets": [{
        "name": ds["name"],
        "displayName": ds["displayName"],
        "query": ds["query"]
    } for ds in datasets]
})

print(f"Dashboard JSON: {len(serialized)} chars, {len(widgets)} widgets, {len(datasets)} datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create / Update Dashboard via Lakeview API

# COMMAND ----------

import urllib.request as _urllib_request

_ws_url = spark.conf.get("spark.databricks.workspaceUrl", "")
_host = f"https://{_ws_url}"
_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

def _api(method, path, body=None):
    data = json.dumps(body).encode() if body else None
    req = _urllib_request.Request(
        f"{_host}{path}", data=data, method=method,
        headers={"Authorization": f"Bearer {_token}", "Content-Type": "application/json"}
    )
    return json.loads(_urllib_request.urlopen(req).read())

existing_id = None
try:
    dashboards = _api("GET", "/api/2.0/lakeview/dashboards")
    for d in dashboards.get("dashboards", []):
        if d.get("display_name") == DASHBOARD_NAME:
            existing_id = d["dashboard_id"]
            break
except Exception as e:
    print(f"List dashboards note: {e}")

if existing_id:
    print(f"Updating existing dashboard: {existing_id}")
    _api("PATCH", f"/api/2.0/lakeview/dashboards/{existing_id}", {
        "display_name": DASHBOARD_NAME,
        "serialized_dashboard": serialized,
    })
    dashboard_id = existing_id
else:
    try:
        user_email = spark.sql("SELECT current_user()").collect()[0][0]
        parent = f"/Workspace/Users/{user_email}/gsk-dataops-olympics/scoring"
    except Exception:
        parent = None

    body = {
        "display_name": DASHBOARD_NAME,
        "serialized_dashboard": serialized,
    }
    if parent:
        body["parent_path"] = parent

    result = _api("POST", "/api/2.0/lakeview/dashboards", body)
    dashboard_id = result["dashboard_id"]
    print(f"Created new dashboard: {dashboard_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish Dashboard

# COMMAND ----------

try:
    _api("POST", f"/api/2.0/lakeview/dashboards/{dashboard_id}/published", {
        "embed_credentials": True,
        "warehouse_id": ""
    })
    print(f"Dashboard published!")
except Exception as e:
    print(f"Publish note: {e}")

print(f"\nDashboard URL: https://{_ws_url}/dashboardsv3/{dashboard_id}")
print(f"\nRe-run this notebook after each event to refresh the scoreboard.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Winner Announcement

# COMMAND ----------

from datetime import datetime

standings = spark.sql("""
    SELECT Team, ROUND(SUM(points), 1) AS total, COUNT(DISTINCT event) AS events
    FROM olympics_leaderboard GROUP BY Team ORDER BY total DESC
""").toPandas()

if len(standings) > 0:
    print("=" * 60)
    print(f"  DATAOPS OLYMPICS STANDINGS -- {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 60)
    for i, row in standings.iterrows():
        medal = {0: "[GOLD]  ", 1: "[SILVER]", 2: "[BRONZE]"}.get(i, "        ")
        print(f"  {i+1}. {medal} {row['Team']:12s} | {row['total']:6.1f} pts | {int(row['events'])} events")
    print("=" * 60)
else:
    print("No scores yet. Run event scoring notebooks first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Printable Certificates

# COMMAND ----------

if len(standings) >= 3:
    for i in range(min(3, len(standings))):
        row = standings.iloc[i]
        place = ["FIRST", "SECOND", "THIRD"][i]
        medal = ["GOLD", "SILVER", "BRONZE"][i]
        print("+" + "=" * 58 + "+")
        print(f"|{' '*10}DATAOPS OLYMPICS CERTIFICATE{' '*20}|")
        print(f"|{' '*5}{medal} - {place} PLACE{' '*(46-len(place)-len(medal))}|")
        print(f"|{' '*5}Awarded to: {row['Team']:<42}|")
        print(f"|{' '*5}Score: {row['total']:.1f} points{' '*(42-len(str(row['total'])))}|")
        print(f"|{' '*5}GSK India x Databricks{' '*31}|")
        print(f"|{' '*5}{datetime.now().strftime('%B %d, %Y'):<48}|")
        print("+" + "=" * 58 + "+")
        print()
