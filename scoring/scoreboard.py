# Databricks notebook source
# MAGIC %md
# MAGIC # 🏆 DataOps Olympics — Live Scoreboard
# MAGIC
# MAGIC ## Organizer Control Panel
# MAGIC
# MAGIC Use this notebook to track scores across all events in real-time.
# MAGIC Project this on a big screen during the competition!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime

# Number of teams
NUM_TEAMS = 10
TEAM_NAMES = [f"Team {i+1}" for i in range(NUM_TEAMS)]

# Initialize scoreboard
scoreboard = pd.DataFrame({
    "team": TEAM_NAMES,
    "event1_speed_sprint": [0] * NUM_TEAMS,
    "event2_accuracy": [0] * NUM_TEAMS,
    "event3_innovation": [0] * NUM_TEAMS,
    "event4_relay": [0] * NUM_TEAMS,
    "event5_plot_twist": [0] * NUM_TEAMS,
})

print("Scoreboard initialized!")
print(f"Teams: {NUM_TEAMS}")
print(f"Events: 5")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enter Scores
# MAGIC
# MAGIC **Instructions:** Update the scores below after each event.

# COMMAND ----------

# =============================================================
# EVENT 1: SPEED SPRINT (max 10 pts)
# Enter finishing order: 1st=10, 2nd=8, 3rd=6, 4th=5, 5th=4, 6th+=2
# =============================================================

scoreboard.loc[0, "event1_speed_sprint"] = 0   # Team 1
scoreboard.loc[1, "event1_speed_sprint"] = 0   # Team 2
scoreboard.loc[2, "event1_speed_sprint"] = 0   # Team 3
scoreboard.loc[3, "event1_speed_sprint"] = 0   # Team 4
scoreboard.loc[4, "event1_speed_sprint"] = 0   # Team 5
scoreboard.loc[5, "event1_speed_sprint"] = 0   # Team 6
scoreboard.loc[6, "event1_speed_sprint"] = 0   # Team 7
scoreboard.loc[7, "event1_speed_sprint"] = 0   # Team 8
scoreboard.loc[8, "event1_speed_sprint"] = 0   # Team 9
scoreboard.loc[9, "event1_speed_sprint"] = 0   # Team 10

# COMMAND ----------

# =============================================================
# EVENT 2: ACCURACY CHALLENGE (max 20 pts = 15 F1 + 5 explainability)
# =============================================================

# F1 scores (enter each team's reported F1)
f1_scores = {
    "Team 1": 0.0,
    "Team 2": 0.0,
    "Team 3": 0.0,
    "Team 4": 0.0,
    "Team 5": 0.0,
    "Team 6": 0.0,
    "Team 7": 0.0,
    "Team 8": 0.0,
    "Team 9": 0.0,
    "Team 10": 0.0,
}

# Explainability bonus (judge-rated, max 5)
explainability_bonus = {
    "Team 1": 0,
    "Team 2": 0,
    "Team 3": 0,
    "Team 4": 0,
    "Team 5": 0,
    "Team 6": 0,
    "Team 7": 0,
    "Team 8": 0,
    "Team 9": 0,
    "Team 10": 0,
}

# Calculate F1-based points (proportional to best, max 15)
max_f1 = max(f1_scores.values()) if max(f1_scores.values()) > 0 else 1
for team in TEAM_NAMES:
    f1_points = round((f1_scores[team] / max_f1) * 15, 1) if max_f1 > 0 else 0
    total = f1_points + explainability_bonus[team]
    scoreboard.loc[scoreboard["team"] == team, "event2_accuracy"] = total

# COMMAND ----------

# =============================================================
# EVENT 3: INNOVATION SHOWCASE (max 30 pts)
# Judge-rated: Creativity(10) + Functionality(10) + Usefulness(5) + Demo(5)
# =============================================================

scoreboard.loc[0, "event3_innovation"] = 0   # Team 1
scoreboard.loc[1, "event3_innovation"] = 0   # Team 2
scoreboard.loc[2, "event3_innovation"] = 0   # Team 3
scoreboard.loc[3, "event3_innovation"] = 0   # Team 4
scoreboard.loc[4, "event3_innovation"] = 0   # Team 5
scoreboard.loc[5, "event3_innovation"] = 0   # Team 6
scoreboard.loc[6, "event3_innovation"] = 0   # Team 7
scoreboard.loc[7, "event3_innovation"] = 0   # Team 8
scoreboard.loc[8, "event3_innovation"] = 0   # Team 9
scoreboard.loc[9, "event3_innovation"] = 0   # Team 10

# COMMAND ----------

# =============================================================
# EVENT 4: RELAY CHALLENGE (max 25 pts)
# Time-based (15) + Quality gates (10)
# =============================================================

# Enter completion times in minutes (including penalties)
relay_times = {
    "Team 1": 30.0,
    "Team 2": 30.0,
    "Team 3": 30.0,
    "Team 4": 30.0,
    "Team 5": 30.0,
    "Team 6": 30.0,
    "Team 7": 30.0,
    "Team 8": 30.0,
    "Team 9": 30.0,
    "Team 10": 30.0,
}

# Quality gate scores (max 10, based on checkpoints passed)
quality_scores = {
    "Team 1": 0,
    "Team 2": 0,
    "Team 3": 0,
    "Team 4": 0,
    "Team 5": 0,
    "Team 6": 0,
    "Team 7": 0,
    "Team 8": 0,
    "Team 9": 0,
    "Team 10": 0,
}

# Calculate time-based points (fastest = 15, inversely proportional)
min_time = min(relay_times.values())
for team in TEAM_NAMES:
    time_points = round((min_time / relay_times[team]) * 15, 1) if relay_times[team] > 0 else 0
    total = time_points + quality_scores[team]
    scoreboard.loc[scoreboard["team"] == team, "event4_relay"] = total

# COMMAND ----------

# =============================================================
# EVENT 5: PLOT TWIST FINALS (max 25 pts)
# Only top 3 teams compete
# Judge-rated: Adaptation(10) + Quality(10) + Presentation(5)
# =============================================================

# Note: Only fill in for the top 3 teams
scoreboard.loc[0, "event5_plot_twist"] = 0   # Fill if team qualifies
scoreboard.loc[1, "event5_plot_twist"] = 0   # Fill if team qualifies
scoreboard.loc[2, "event5_plot_twist"] = 0   # Fill if team qualifies
# Remaining teams get 0 (they don't compete in this event)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Live Scoreboard

# COMMAND ----------

# Calculate totals
scoreboard["total"] = (
    scoreboard["event1_speed_sprint"] +
    scoreboard["event2_accuracy"] +
    scoreboard["event3_innovation"] +
    scoreboard["event4_relay"] +
    scoreboard["event5_plot_twist"]
)

# Sort by total
scoreboard_sorted = scoreboard.sort_values("total", ascending=False).reset_index(drop=True)
scoreboard_sorted.index = scoreboard_sorted.index + 1  # 1-based ranking
scoreboard_sorted.index.name = "Rank"

print("=" * 80)
print(f"  🏆 DATAOPS OLYMPICS SCOREBOARD — {datetime.now().strftime('%H:%M:%S')}")
print("=" * 80)
print()
print(scoreboard_sorted.to_string())
print()
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visual Scoreboard

# COMMAND ----------

# Bar chart - Total scores
fig_total = px.bar(
    scoreboard_sorted.reset_index(),
    x="team", y="total",
    title="🏆 DataOps Olympics — Total Scores",
    text="total",
    color="total",
    color_continuous_scale="Viridis"
)
fig_total.update_layout(
    template="plotly_white",
    font=dict(size=14),
    title_font_size=24,
    xaxis_title="",
    yaxis_title="Total Points",
    showlegend=False
)
fig_total.update_traces(textposition="outside")
fig_total.show()

# COMMAND ----------

# Stacked bar chart - Score breakdown by event
event_cols = ["event1_speed_sprint", "event2_accuracy", "event3_innovation", "event4_relay", "event5_plot_twist"]
event_labels = ["Speed Sprint", "Accuracy", "Innovation", "Relay", "Plot Twist"]

df_melted = scoreboard_sorted.reset_index().melt(
    id_vars=["team"],
    value_vars=event_cols,
    var_name="event",
    value_name="score"
)
df_melted["event"] = df_melted["event"].map(dict(zip(event_cols, event_labels)))

fig_stack = px.bar(
    df_melted,
    x="team", y="score", color="event",
    title="Score Breakdown by Event",
    barmode="stack",
    color_discrete_sequence=px.colors.qualitative.Set2
)
fig_stack.update_layout(
    template="plotly_white",
    font=dict(size=14),
    title_font_size=20,
    xaxis_title="",
    yaxis_title="Points",
    legend_title="Event"
)
fig_stack.show()

# COMMAND ----------

# Radar chart for top 3 teams
top3 = scoreboard_sorted.head(3)

fig_radar = go.Figure()

for _, team_row in top3.iterrows():
    fig_radar.add_trace(go.Scatterpolar(
        r=[team_row[col] for col in event_cols] + [team_row[event_cols[0]]],
        theta=event_labels + [event_labels[0]],
        fill="toself",
        name=team_row["team"]
    ))

fig_radar.update_layout(
    polar=dict(
        radialaxis=dict(visible=True, range=[0, 30])
    ),
    title="Top 3 Teams — Performance Radar",
    title_font_size=20,
    template="plotly_white"
)
fig_radar.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event-by-Event Results

# COMMAND ----------

# Event 1 Results
fig_e1 = px.bar(
    scoreboard_sorted.reset_index().sort_values("event1_speed_sprint", ascending=False),
    x="team", y="event1_speed_sprint",
    title="Event 1: Speed Sprint Results",
    text="event1_speed_sprint",
    color="event1_speed_sprint",
    color_continuous_scale="Blues"
)
fig_e1.update_layout(template="plotly_white", showlegend=False)
fig_e1.show()

# COMMAND ----------

# Event 2 Results
fig_e2 = px.bar(
    scoreboard_sorted.reset_index().sort_values("event2_accuracy", ascending=False),
    x="team", y="event2_accuracy",
    title="Event 2: Accuracy Challenge Results",
    text="event2_accuracy",
    color="event2_accuracy",
    color_continuous_scale="Greens"
)
fig_e2.update_layout(template="plotly_white", showlegend=False)
fig_e2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Winner Announcement

# COMMAND ----------

print()
print("=" * 60)
print("  🏆 DATAOPS OLYMPICS FINAL RESULTS 🏆")
print("=" * 60)

winner = scoreboard_sorted.iloc[0]
second = scoreboard_sorted.iloc[1]
third = scoreboard_sorted.iloc[2]

print(f"""

  🥇 FIRST PLACE:  {winner['team']}  ({winner['total']:.1f} pts)
  🥈 SECOND PLACE: {second['team']}  ({second['total']:.1f} pts)
  🥉 THIRD PLACE:  {third['team']}  ({third['total']:.1f} pts)

""")
print("=" * 60)
print("  Congratulations to all teams!")
print("  Thank you for participating in the DataOps Olympics!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Results

# COMMAND ----------

# Save final results as Delta table
df_results = spark.createDataFrame(scoreboard_sorted.reset_index())
df_results.write.format("delta").mode("overwrite").saveAsTable("dataops_olympics_results")
print("✅ Results saved to 'dataops_olympics_results' table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Printable Certificates (Template)

# COMMAND ----------

for i, (_, row) in enumerate(scoreboard_sorted.head(3).iterrows()):
    place = ["FIRST", "SECOND", "THIRD"][i]
    medal = ["🥇", "🥈", "🥉"][i]
    
    print("╔" + "═" * 58 + "╗")
    print(f"║{' ' * 58}║")
    print(f"║{' '*10}DATAOPS OLYMPICS CERTIFICATE{' '*20}║")
    print(f"║{' ' * 58}║")
    print(f"║{' '*5}{medal} {place} PLACE {medal}{' '*(45-len(place))}║")
    print(f"║{' ' * 58}║")
    print(f"║{' '*5}Awarded to: {row['team']:<42}║")
    print(f"║{' '*5}Score: {row['total']:.1f} points{' '*(42-len(f'{row[chr(39)+chr(39)}:.1f}'))}║")
    print(f"║{' ' * 58}║")
    print(f"║{' '*5}GSK India x Databricks{' '*31}║")
    print(f"║{' '*5}{datetime.now().strftime('%B %d, %Y'):<48}║")
    print(f"║{' ' * 58}║")
    print("╚" + "═" * 58 + "╝")
    print()
