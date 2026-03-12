# Databricks notebook source
# MAGIC %md
# MAGIC # Assign Users to Teams
# MAGIC
# MAGIC **FOR INSTRUCTOR ONLY**
# MAGIC
# MAGIC Run this notebook to assign workspace users to their team groups.
# MAGIC Each user is added to a workspace group (e.g., `team_01`) which controls
# MAGIC their catalog access permissions.
# MAGIC
# MAGIC ### Instructions
# MAGIC 1. Edit the `ASSIGNMENTS` dictionary below
# MAGIC 2. Run All
# MAGIC 3. Verify the output

# COMMAND ----------

# MAGIC %md
# MAGIC ## Team Assignments
# MAGIC
# MAGIC **Option 1 (Easiest):** Use CloudLabs Cloud DID numbers.
# MAGIC Go to CloudLabs portal → copy the `Cloud DID#` for each user → paste into `CLOUDLABS_TEAMS`.
# MAGIC The script auto-converts `2127029` → `odl_user_2127029@databrickslabs.com`.
# MAGIC
# MAGIC **Option 2:** Paste a spreadsheet extract (CSV/TSV) into `ROSTER`.
# MAGIC Columns: `email, name, team`
# MAGIC
# MAGIC **Option 3:** Edit the `ASSIGNMENTS` dict directly (email → team).

# COMMAND ----------

# ──── OPTION 1: CloudLabs Cloud DID mapping (EASIEST) ────
# From CloudLabs portal, just copy the Cloud DID# for each user.
# Format: {team_name: [cloud_did_1, cloud_did_2, ...]}
# The script auto-converts to odl_user_{DID}@databrickslabs.com
CLOUDLABS_TEAMS = {
    # "team_01": [2127029, 2119571],   # Sean Zhang, Bimal Sebastian
    # "team_02": [2119455, 2119450],   # Souvik, Sean Z
}
ODL_DOMAIN = "databrickslabs.com"  # CloudLabs email domain

# ──── OPTION 2: Paste spreadsheet data here ────
# Accepts CSV, TSV, or pipe-delimited. Header row optional.
# Required columns: email (or username) and team
ROSTER = """
email,name,team
# odl_user_2119571@databrickslabs.com,User One,team_01
# odl_user_2119450@databrickslabs.com,User Two,team_01
"""

# ──── OPTION 3: Direct dict (takes priority if non-empty) ────
ASSIGNMENTS = {
    # "odl_user_2119571@databrickslabs.com": "team_01",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse Roster

# COMMAND ----------

import csv, io

def parse_roster(text):
    """Parse CSV/TSV/pipe roster into {email: team} dict."""
    result = {}
    roster_info = []  # (email, name, username, team)
    lines = [l.strip() for l in text.strip().split("\n") if l.strip() and not l.strip().startswith("#")]
    if not lines:
        return result, roster_info

    # Auto-detect delimiter
    first_data = lines[1] if len(lines) > 1 else lines[0]
    if "\t" in first_data:
        delimiter = "\t"
    elif "|" in first_data:
        delimiter = "|"
    else:
        delimiter = ","

    reader = csv.reader(lines, delimiter=delimiter)
    rows = list(reader)
    if not rows:
        return result, roster_info

    # Detect header
    header = [h.strip().lower() for h in rows[0]]
    if "email" in header or "username" in header or "team" in header:
        data_rows = rows[1:]
    else:
        header = ["email", "name", "username", "team"][:len(rows[0])]
        data_rows = rows

    email_idx = header.index("email") if "email" in header else None
    name_idx = header.index("name") if "name" in header else None
    user_idx = header.index("username") if "username" in header else None
    team_idx = header.index("team") if "team" in header else None

    if team_idx is None:
        print("  [WARN] No 'team' column found in roster!")
        return result, roster_info

    for row in data_rows:
        row = [c.strip() for c in row]
        if len(row) <= team_idx:
            continue
        email = row[email_idx] if email_idx is not None else ""
        name = row[name_idx] if name_idx is not None else ""
        username = row[user_idx] if user_idx is not None else ""
        team = row[team_idx]

        if not email and username:
            email = username if "@" in username else f"{username}@databrickslabs.com"

        if email and team:
            result[email] = team
            roster_info.append((email, name, username, team))

    return result, roster_info

# Priority: ASSIGNMENTS dict > CloudLabs DIDs > Roster CSV
if not ASSIGNMENTS and CLOUDLABS_TEAMS:
    print("  Using CloudLabs Cloud DID mapping:")
    print(f"  {'ODL Email':45s} {'Team':10s}")
    print(f"  {'-'*45} {'-'*10}")
    for team, dids in CLOUDLABS_TEAMS.items():
        for did in dids:
            email = f"odl_user_{did}@{ODL_DOMAIN}"
            ASSIGNMENTS[email] = team
            print(f"  {email:45s} {team:10s}")
    print(f"\n  Total: {len(ASSIGNMENTS)} users across {len(CLOUDLABS_TEAMS)} teams")

if not ASSIGNMENTS:
    ASSIGNMENTS, roster_info = parse_roster(ROSTER)
    if roster_info:
        print(f"  Parsed {len(roster_info)} entries from roster:")
        print(f"  {'Email':45s} {'Name':20s} {'Team':10s}")
        print(f"  {'-'*45} {'-'*20} {'-'*10}")
        for email, name, username, team in roster_info:
            print(f"  {email:45s} {name:20s} {team:10s}")
    else:
        print("  No entries parsed from roster. Edit CLOUDLABS_TEAMS, ROSTER, or ASSIGNMENTS above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Assignments

# COMMAND ----------

import requests, json

host = spark.conf.get("spark.databricks.workspaceUrl", "")
if not host.startswith("http"):
    host = f"https://{host}"

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def get_user_id(email):
    """Look up user ID by email."""
    resp = requests.get(
        f"{host}/api/2.0/preview/scim/v2/Users",
        headers=headers,
        params={"filter": f'userName eq "{email}"'}
    )
    resp.raise_for_status()
    users = resp.json().get("Resources", [])
    return users[0]["id"] if users else None

def get_group_id(group_name):
    """Look up group ID by name."""
    resp = requests.get(
        f"{host}/api/2.0/preview/scim/v2/Groups",
        headers=headers,
        params={"filter": f'displayName eq "{group_name}"'}
    )
    resp.raise_for_status()
    groups = resp.json().get("Resources", [])
    return groups[0]["id"] if groups else None

def add_user_to_group(user_id, group_id, group_name):
    """Add a user to a group via SCIM PATCH."""
    resp = requests.patch(
        f"{host}/api/2.0/preview/scim/v2/Groups/{group_id}",
        headers=headers,
        json={
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{
                "op": "add",
                "value": {"members": [{"value": user_id}]}
            }]
        }
    )
    resp.raise_for_status()
    return resp.status_code

# COMMAND ----------

print("=" * 60)
print("  TEAM ASSIGNMENTS")
print("=" * 60)

SHARED_CATALOG = "dataops_olympics"
SHARED_SCHEMA = "default"
VOLUME_NAME = "raw_data"
ALL_TEAMS = [f"team_{i:02d}" for i in range(1, 16)]

if not ASSIGNMENTS:
    print("\n  No assignments configured! Edit the ASSIGNMENTS dict above.")
else:
    group_cache = {}
    for email, team in ASSIGNMENTS.items():
        try:
            # 1. Add to workspace group
            user_id = get_user_id(email)
            if not user_id:
                print(f"  [FAIL] {email} -> {team} (user not found)")
                continue

            if team not in group_cache:
                group_cache[team] = get_group_id(team)
            group_id = group_cache[team]

            if group_id:
                add_user_to_group(user_id, group_id, team)

            # 2. Grant Unity Catalog permissions (per-user, required on Azure)
            spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {team} TO `{email}`")
            spark.sql(f"GRANT USAGE ON CATALOG {SHARED_CATALOG} TO `{email}`")
            spark.sql(f"GRANT USAGE ON SCHEMA {SHARED_CATALOG}.{SHARED_SCHEMA} TO `{email}`")
            spark.sql(f"GRANT SELECT ON SCHEMA {SHARED_CATALOG}.{SHARED_SCHEMA} TO `{email}`")
            spark.sql(f"GRANT READ VOLUME ON VOLUME {SHARED_CATALOG}.{SHARED_SCHEMA}.{VOLUME_NAME} TO `{email}`")

            # 3. Revoke access to other team catalogs
            for other_team in ALL_TEAMS:
                if other_team != team:
                    try:
                        spark.sql(f"REVOKE ALL PRIVILEGES ON CATALOG {other_team} FROM `{email}`")
                    except Exception:
                        pass  # catalog may not exist yet

            print(f"  [OK]   {email} -> {team} (group + UC grants)")
        except Exception as e:
            print(f"  [FAIL] {email} -> {team}: {str(e)[:60]}")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Group Memberships

# COMMAND ----------

print("=" * 60)
print("  CURRENT GROUP MEMBERSHIPS")
print("=" * 60)

for i in range(1, 16):
    team = f"team_{i:02d}"
    group_id = get_group_id(team)
    if group_id:
        resp = requests.get(f"{host}/api/2.0/preview/scim/v2/Groups/{group_id}", headers=headers)
        members = resp.json().get("members", [])
        member_names = [m.get("display", m.get("value", "?")) for m in members]
        if member_names:
            print(f"  {team}: {', '.join(member_names)}")
        else:
            print(f"  {team}: (empty)")
    else:
        print(f"  {team}: (group not found)")

print("=" * 60)
