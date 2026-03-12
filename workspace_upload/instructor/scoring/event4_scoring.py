# Databricks notebook source
# MAGIC %md
# MAGIC # Event 4: Agent Bricks — Automated Scoring
# MAGIC
# MAGIC Scores teams on their Agent Bricks implementation:
# MAGIC - UC Function Tools (10 pts)
# MAGIC - Genie Space (8 pts)
# MAGIC - Knowledge Assistant (7 pts)
# MAGIC - Supervisor Agent (10 pts)
# MAGIC - Test Prompt Evaluation (5 pts)
# MAGIC - Bonus: MCP Server (+5 pts)
# MAGIC - Bonus: Routing Instructions (+3 pts)
# MAGIC - Bonus: DSPy Prompt Optimization (+3 pts)

# COMMAND ----------

import requests, json, time

host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def api_get(path):
    return requests.get(f"https://{host}{path}", headers=headers)

def api_post(path, body):
    return requests.post(f"https://{host}{path}", headers=headers, json=body)

_LB = "dataops_olympics.default.olympics_leaderboard"
_RT = "dataops_olympics.default.registered_teams"
_EVENT = "Event 4: Agent Bricks"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring Functions

# COMMAND ----------

def _score_uc_functions(catalog: str) -> tuple:
    """Check for UC function tools. Returns (points, details)."""
    try:
        funcs = spark.sql(f"""
            SELECT routine_name FROM system.information_schema.routines
            WHERE routine_catalog = '{catalog}' AND routine_schema = 'default'
        """).collect()
        func_names = [r.routine_name.lower() for r in funcs]
    except Exception as e:
        return 0, f"Error listing functions: {e}"

    risk_found = any("risk" in n or "assess" in n for n in func_names)
    cohort_found = any("cohort" in n or "summary" in n or "get_" in n for n in func_names)

    pts = 0
    details = []

    if risk_found:
        pts += 5
        details.append("patient_risk_assessment found")
    else:
        details.append("MISSING: risk assessment function")

    if cohort_found:
        pts += 5
        details.append("cohort summary function found")
    else:
        details.append("MISSING: cohort summary function")

    if pts > 0:
        try:
            test = spark.sql(f"SELECT {catalog}.default.patient_risk_assessment(55, 250, 145, 150) AS r").collect()
            if test and test[0]["r"]:
                details.append("risk function returns valid output")
            else:
                pts = max(pts - 2, 0)
                details.append("risk function returned empty result")
        except Exception as e:
            pts = max(pts - 2, 0)
            details.append(f"risk function execution error: {str(e)[:100]}")

    return min(pts, 10), " | ".join(details)


def _score_genie_space(catalog: str, team: str) -> tuple:
    """Check for a Genie Space. Returns (points, details, genie_space_id)."""
    genie_id = ""

    try:
        cfg = spark.sql(
            f"SELECT resource_id FROM {catalog}.default.agent_config "
            f"WHERE resource_type = 'genie_space' LIMIT 1"
        ).collect()
        if cfg:
            genie_id = cfg[0]["resource_id"]
    except Exception:
        pass

    if not genie_id:
        try:
            resp = api_get("/api/2.0/genie/spaces")
            if resp.status_code == 200:
                spaces = resp.json().get("spaces", resp.json().get("genie_spaces", []))
                for s in spaces:
                    title = s.get("title", s.get("name", "")).lower()
                    if team.lower() in title:
                        genie_id = s.get("space_id", s.get("id", ""))
                        break
        except Exception:
            pass

    if not genie_id:
        return 0, "No Genie Space found", ""

    pts = 5
    details = [f"Genie Space found: {genie_id}"]

    try:
        resp = api_get(f"/api/2.0/genie/spaces/{genie_id}")
        if resp.status_code == 200:
            data = resp.json()
            tables = data.get("table_identifiers", [])
            if len(tables) >= 2:
                pts += 2
                details.append(f"{len(tables)} tables configured")
            else:
                details.append(f"Only {len(tables)} table(s)")
            desc = data.get("description", "")
            if len(desc) > 20:
                pts += 1
                details.append("has description")
    except Exception:
        pass

    return min(pts, 8), " | ".join(details), genie_id


def _score_knowledge_assistant(catalog: str, team: str) -> tuple:
    """Check for a Knowledge Assistant. Returns (points, details, ka_tile_id)."""
    ka_id = ""

    try:
        cfg = spark.sql(
            f"SELECT resource_id FROM {catalog}.default.agent_config "
            f"WHERE resource_type = 'knowledge_assistant' LIMIT 1"
        ).collect()
        if cfg:
            ka_id = cfg[0]["resource_id"]
    except Exception:
        pass

    if not ka_id:
        try:
            resp = api_get("/api/2.0/serving-endpoints")
            if resp.status_code == 200:
                endpoints = resp.json().get("endpoints", [])
                for ep in endpoints:
                    name = ep.get("name", "")
                    if name.startswith("agents-") and team.lower() in name.lower():
                        candidate_id = name.replace("agents-", "")
                        if "doc" in name.lower() or "ka" in name.lower() or "knowledge" in name.lower():
                            ka_id = candidate_id
                            break
        except Exception:
            pass

    if not ka_id:
        return 0, "No Knowledge Assistant found", ""

    pts = 4
    details = [f"KA found: {ka_id}"]
    endpoint_name = f"agents-{ka_id}"

    try:
        resp = api_get(f"/api/2.0/serving-endpoints/{endpoint_name}")
        if resp.status_code == 200:
            state = resp.json().get("state", {})
            if state.get("ready") == "READY":
                pts += 3
                details.append("endpoint ONLINE")
            else:
                pts += 1
                details.append(f"endpoint state: {state.get('ready', 'UNKNOWN')}")
    except Exception as e:
        details.append(f"endpoint check failed: {str(e)[:80]}")

    return min(pts, 7), " | ".join(details), ka_id


def _score_supervisor(catalog: str, team: str) -> tuple:
    """Check for a Supervisor Agent. Returns (points, details, mas_tile_id)."""
    mas_id = ""

    try:
        cfg = spark.sql(
            f"SELECT resource_id FROM {catalog}.default.agent_config "
            f"WHERE resource_type = 'supervisor_agent' LIMIT 1"
        ).collect()
        if cfg:
            mas_id = cfg[0]["resource_id"]
    except Exception:
        pass

    if not mas_id:
        try:
            resp = api_get("/api/2.0/serving-endpoints")
            if resp.status_code == 200:
                endpoints = resp.json().get("endpoints", [])
                for ep in endpoints:
                    name = ep.get("name", "")
                    if name.startswith("agents-") and ("supervisor" in name.lower() or "clinical" in name.lower()):
                        if team.lower() in name.lower():
                            mas_id = name.replace("agents-", "")
                            break
        except Exception:
            pass

    if not mas_id:
        return 0, "No Supervisor Agent found", ""

    pts = 5
    details = [f"Supervisor found: {mas_id}"]
    endpoint_name = f"agents-{mas_id}"

    try:
        resp = api_get(f"/api/2.0/serving-endpoints/{endpoint_name}")
        if resp.status_code == 200:
            state = resp.json().get("state", {})
            if state.get("ready") == "READY":
                pts += 3
                details.append("endpoint ONLINE")

                tags = resp.json().get("tags", [])
                config = resp.json().get("config", {})
                served = config.get("served_entities", config.get("served_models", []))
                if len(served) >= 2 or len(tags) >= 2:
                    pts += 2
                    details.append(f"multi-agent wiring detected")
                else:
                    pts += 1
                    details.append("single-agent wiring")
            else:
                pts += 1
                details.append(f"endpoint: {state.get('ready', 'UNKNOWN')}")
    except Exception as e:
        details.append(f"endpoint check: {str(e)[:80]}")

    return min(pts, 10), " | ".join(details), mas_id


def _score_test_results(catalog: str) -> tuple:
    """Check for test prompt results. Returns (points, details)."""
    try:
        df = spark.table(f"{catalog}.default.supervisor_test_results")
        total = df.count()
        if total == 0:
            return 0, "supervisor_test_results table exists but empty"
        successes = df.filter("status = 'success'").count()
        pts = min(5, total)
        if successes >= 3:
            pts = 5
        elif successes >= 1:
            pts = 3
        else:
            pts = 2
        return pts, f"{successes}/{total} successful test prompts"
    except Exception:
        return 0, "No supervisor_test_results table found"


def _score_mcp_bonus(catalog: str, team: str) -> tuple:
    """Check for MCP server integration (bonus). Returns (points, details)."""
    try:
        conns = spark.sql(f"SHOW CONNECTIONS").collect()
        mcp_found = False
        for c in conns:
            name = c["name"].lower() if "name" in c.asDict() else str(c).lower()
            if team.lower() in name and "mcp" in name:
                mcp_found = True
                break
        if mcp_found:
            return 5, "MCP HTTP connection found"
    except Exception:
        pass

    try:
        cfg = spark.sql(
            f"SELECT resource_id FROM {catalog}.default.agent_config "
            f"WHERE resource_type = 'mcp_connection' LIMIT 1"
        ).collect()
        if cfg and cfg[0]["resource_id"]:
            return 3, f"MCP connection configured: {cfg[0]['resource_id']}"
    except Exception:
        pass

    return 0, "No MCP integration found"


def _score_routing_bonus(catalog: str, team: str, mas_id: str) -> tuple:
    """Check for advanced routing instructions (bonus). Returns (points, details)."""
    if not mas_id:
        return 0, "No Supervisor to check"

    endpoint_name = f"agents-{mas_id}"
    try:
        resp = api_get(f"/api/2.0/serving-endpoints/{endpoint_name}")
        if resp.status_code == 200:
            data = resp.json()
            config_str = json.dumps(data)
            instructions_markers = ["route", "chain", "delegate", "when to use"]
            found_markers = sum(1 for m in instructions_markers if m.lower() in config_str.lower())
            if found_markers >= 3 and len(config_str) > 500:
                return 3, f"Rich routing instructions detected ({found_markers} markers)"
            elif found_markers >= 1:
                return 1, f"Basic routing detected ({found_markers} markers)"
    except Exception:
        pass

    try:
        cfg = spark.sql(
            f"SELECT resource_id FROM {catalog}.default.agent_config "
            f"WHERE resource_type = 'routing_instructions' LIMIT 1"
        ).collect()
        if cfg and len(cfg[0]["resource_id"]) > 200:
            return 3, "Routing instructions in agent_config (200+ chars)"
    except Exception:
        pass

    return 0, "No advanced routing instructions found"


def _score_dspy_bonus(catalog: str) -> tuple:
    """Check for DSPy prompt optimization (bonus). Returns (points, details)."""
    try:
        df = spark.table(f"{catalog}.default.dspy_optimized_prompt")
        rows = df.collect()
        if rows:
            prompt_text = str(rows[0]["optimized_prompt"] or "")
            if len(prompt_text) > 50:
                return 3, f"DSPy optimized prompt found ({len(prompt_text)} chars)"
            else:
                return 1, "DSPy table exists but prompt is very short"
        return 0, "dspy_optimized_prompt table exists but empty"
    except Exception:
        return 0, "No dspy_optimized_prompt table found"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score All Teams

# COMMAND ----------

def score_event4(team: str) -> dict:
    """Score a single team for Event 4: Agent Bricks."""
    catalog = team
    print(f"\n{'='*60}")
    print(f"Scoring {team} for {_EVENT}")
    print(f"{'='*60}")

    uc_pts, uc_det = _score_uc_functions(catalog)
    print(f"  UC Functions:     {uc_pts}/10 — {uc_det}")

    genie_pts, genie_det, genie_id = _score_genie_space(catalog, team)
    print(f"  Genie Space:      {genie_pts}/8  — {genie_det}")

    ka_pts, ka_det, ka_id = _score_knowledge_assistant(catalog, team)
    print(f"  Knowledge Asst:   {ka_pts}/7  — {ka_det}")

    mas_pts, mas_det, mas_id = _score_supervisor(catalog, team)
    print(f"  Supervisor:       {mas_pts}/10 — {mas_det}")

    test_pts, test_det = _score_test_results(catalog)
    print(f"  Test Prompts:     {test_pts}/5  — {test_det}")

    mcp_pts, mcp_det = _score_mcp_bonus(catalog, team)
    print(f"  Bonus MCP:        {mcp_pts}/5  — {mcp_det}")

    route_pts, route_det = _score_routing_bonus(catalog, team, mas_id)
    print(f"  Bonus Routing:    {route_pts}/3  — {route_det}")

    dspy_pts, dspy_det = _score_dspy_bonus(catalog)
    print(f"  Bonus DSPy:       {dspy_pts}/3  — {dspy_det}")

    total = uc_pts + genie_pts + ka_pts + mas_pts + test_pts + mcp_pts + route_pts + dspy_pts
    max_total = 10 + 8 + 7 + 10 + 5 + 5 + 3 + 3
    print(f"\n  TOTAL: {total}/{max_total}")

    return {
        "team": team,
        "uc_functions": uc_pts,
        "genie_space": genie_pts,
        "knowledge_assistant": ka_pts,
        "supervisor": mas_pts,
        "test_prompts": test_pts,
        "bonus_mcp": mcp_pts,
        "bonus_routing": route_pts,
        "bonus_dspy": dspy_pts,
        "total": total,
    }

# COMMAND ----------

# Only score teams that have submitted for this event
_event_filter = "event4"
_submitted = spark.sql(f"""
    SELECT DISTINCT team_name FROM dataops_olympics.default.event_submissions
    WHERE event_name = '{_event_filter}'
""").collect()
teams = sorted([r.team_name for r in _submitted])
if not teams:
    print("WARNING: No teams have submitted for this event yet!")
else:
    print(f"Scoring {len(teams)} teams that submitted: {teams}")

results = [score_event4(t) for t in teams]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Leaderboard

# COMMAND ----------

from datetime import datetime as _dt

spark.sql(f"""CREATE TABLE IF NOT EXISTS {_LB}
    (team STRING, event STRING, category STRING, points DOUBLE, max_points DOUBLE, scored_at TIMESTAMP)""")

_now = _dt.now()

# Use MERGE to prevent duplicates (idempotent scoring)
for r in results:
    _t = r["team"]
    # Register team
    spark.sql(f"MERGE INTO {_RT} USING (SELECT '{_t}' AS team) src ON {_RT}.team = src.team WHEN NOT MATCHED THEN INSERT (team) VALUES (src.team)")

    # Merge each category score
    for cat, pts, mx in [
        ("UC Functions",        r["uc_functions"],        10),
        ("Genie Space",         r["genie_space"],          8),
        ("Knowledge Assistant", r["knowledge_assistant"],  7),
        ("Supervisor Agent",    r["supervisor"],          10),
        ("Test Prompts",        r["test_prompts"],         5),
        ("Bonus: MCP Server",   r["bonus_mcp"],            5),
        ("Bonus: Routing",      r["bonus_routing"],        3),
        ("Bonus: DSPy",         r["bonus_dspy"],           3),
    ]:
        spark.sql(f"""
            MERGE INTO {_LB} AS target
            USING (SELECT '{_t}' AS team, '{_EVENT}' AS event, '{cat}' AS category, {pts} AS points, {mx} AS max_points, current_timestamp() AS scored_at) AS source
            ON target.team = source.team AND target.event = source.event AND target.category = source.category
            WHEN MATCHED THEN UPDATE SET points = source.points, max_points = source.max_points, scored_at = source.scored_at
            WHEN NOT MATCHED THEN INSERT (team, event, category, points, max_points, scored_at) VALUES (source.team, source.event, source.category, source.points, source.max_points, source.scored_at)
        """)

    print(f"  {_t}: {r['total']} pts written to leaderboard")

print(f"Leaderboard updated: {len(results)} teams (MERGE - no duplicates)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leaderboard Summary

# COMMAND ----------

display(spark.sql(f"""
    SELECT team,
           SUM(points) AS total_points,
           SUM(max_points) AS max_possible,
           ROUND(SUM(points) / SUM(max_points) * 100, 1) AS pct
    FROM {_LB}
    WHERE event = '{_EVENT}'
    GROUP BY team
    ORDER BY total_points DESC
"""))
