# Databricks notebook source
# MAGIC %md
# MAGIC # Submission Helper
# MAGIC
# MAGIC This notebook is loaded by starter notebooks via `%run ../_submit`.
# MAGIC It provides the `submit()` function for recording your team's submissions.

# COMMAND ----------

from datetime import datetime

def submit(event: str, assets: dict = None, submission_type: str = "notebook"):
    """
    Record a submission for your team.

    Args:
        event: Event name (e.g., "event1", "event2", "event3", "event4", "event5")
        assets: Optional dict of asset references (e.g., {"table": "heart_silver", "endpoint": "my_agent"})
        submission_type: Type of submission (default: "notebook")

    Returns:
        Submission timestamp
    """
    submitted_at = datetime.utcnow()
    assets_str = str(assets).replace("'", "\\'") if assets else ""
    user = spark.sql("SELECT current_user()").collect()[0][0]

    spark.sql(f"""
        INSERT INTO dataops_olympics.default.event_submissions
        VALUES (
            '{TEAM_NAME}',
            '{event}',
            '{submission_type}',
            '{assets_str}',
            '{submitted_at.strftime("%Y-%m-%d %H:%M:%S")}',
            '{user}'
        )
    """)

    print("=" * 60)
    print(f"  SUBMITTED!")
    print(f"  Team:      {TEAM_NAME}")
    print(f"  Event:     {event}")
    print(f"  Time:      {submitted_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"  User:      {user}")
    if assets:
        print(f"  Assets:    {assets}")
    print("=" * 60)
    return submitted_at


def check_submission(event: str = None):
    """Check your team's submissions."""
    filter_clause = f"AND event_name = '{event}'" if event else ""
    subs = spark.sql(f"""
        SELECT event_name, submission_type, asset_reference, submitted_at, submitted_by
        FROM dataops_olympics.default.event_submissions
        WHERE team_name = '{TEAM_NAME}' {filter_clause}
        ORDER BY submitted_at
    """)

    if subs.count() == 0:
        print(f"  No submissions found for {TEAM_NAME}" + (f" event {event}" if event else ""))
    else:
        print(f"  Submissions for {TEAM_NAME}:")
        subs.show(truncate=False)
    return subs
