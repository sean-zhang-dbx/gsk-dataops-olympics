# Databricks notebook source
# MAGIC %md
# MAGIC # PANIC BUTTON: Emergency Access — Grant a User Full Access
# MAGIC
# MAGIC **Use when:** A team member lost access to their catalog, permissions broke,
# MAGIC or you need to give someone temporary admin-level access to fix things.
# MAGIC
# MAGIC This grants a specific user ALL PRIVILEGES on a specific catalog
# MAGIC plus read access to the shared catalog.

# COMMAND ----------

dbutils.widgets.text("USER_EMAIL", "", "User Email")
dbutils.widgets.text("TEAM_CATALOG", "team_XX", "Team Catalog to Grant Access To")

USER_EMAIL = dbutils.widgets.get("USER_EMAIL")
TEAM_CATALOG = dbutils.widgets.get("TEAM_CATALOG")

SHARED_CATALOG = "dataops_olympics"
SHARED_SCHEMA = "default"
VOLUME_NAME = "raw_data"

assert USER_EMAIL, "USER_EMAIL is required!"
assert TEAM_CATALOG != "team_XX", "Set the actual team catalog name!"

print(f"Granting emergency access: {USER_EMAIL} -> {TEAM_CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Grants

# COMMAND ----------

print("=" * 60)
print(f"  EMERGENCY ACCESS GRANT")
print("=" * 60)

# Full access to team catalog
try:
    spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {TEAM_CATALOG} TO `{USER_EMAIL}`")
    print(f"  [OK] ALL PRIVILEGES on {TEAM_CATALOG}")
except Exception as e:
    print(f"  [FAIL] {TEAM_CATALOG}: {str(e)[:80]}")

# Read access to shared catalog
for stmt, label in [
    (f"GRANT USAGE ON CATALOG {SHARED_CATALOG} TO `{USER_EMAIL}`", f"USAGE on {SHARED_CATALOG}"),
    (f"GRANT USAGE ON SCHEMA {SHARED_CATALOG}.{SHARED_SCHEMA} TO `{USER_EMAIL}`", f"USAGE on {SHARED_CATALOG}.{SHARED_SCHEMA}"),
    (f"GRANT SELECT ON SCHEMA {SHARED_CATALOG}.{SHARED_SCHEMA} TO `{USER_EMAIL}`", f"SELECT on {SHARED_CATALOG}.{SHARED_SCHEMA}"),
    (f"GRANT READ VOLUME ON VOLUME {SHARED_CATALOG}.{SHARED_SCHEMA}.{VOLUME_NAME} TO `{USER_EMAIL}`", f"READ VOLUME on raw_data"),
]:
    try:
        spark.sql(stmt)
        print(f"  [OK] {label}")
    except Exception as e:
        print(f"  [FAIL] {label}: {str(e)[:80]}")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Grants

# COMMAND ----------

print(f"\nGrants for {USER_EMAIL}:")
for cat in [TEAM_CATALOG, SHARED_CATALOG]:
    try:
        grants = spark.sql(f"SHOW GRANTS `{USER_EMAIL}` ON CATALOG {cat}").collect()
        for g in grants:
            print(f"  {cat}: {g[1]}")
    except Exception as e:
        print(f"  {cat}: could not check ({str(e)[:50]})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Test — Can This User Access Their Data?

# COMMAND ----------

try:
    tables = spark.sql(f"SHOW TABLES IN {TEAM_CATALOG}.default").collect()
    print(f"\nTables in {TEAM_CATALOG}.default:")
    for t in tables:
        print(f"  - {t.tableName}")
    if not tables:
        print("  (no tables yet)")
except Exception as e:
    print(f"\nCannot list tables: {str(e)[:80]}")
    print("The user may need to run their notebooks to create tables.")
