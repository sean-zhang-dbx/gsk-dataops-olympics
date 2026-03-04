# Databricks notebook source
# MAGIC %md
# MAGIC # DataOps Olympics — Live Scoring Dashboard
# MAGIC
# MAGIC **Event 1: Data Engineering Challenge**
# MAGIC
# MAGIC Run the **scoring.py** notebook first, then run this dashboard to visualize results.

# COMMAND ----------

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

scores_df = spark.table("dataops_olympics.default.event1_scores").toPandas()
scores_df = scores_df.sort_values("Total", ascending=False).reset_index(drop=True)
scores_df["Rank"] = range(1, len(scores_df) + 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overall Leaderboard

# COMMAND ----------

MEDAL_COLORS = {1: "#FFD700", 2: "#C0C0C0", 3: "#CD7F32"}
PATH_COLORS = {"SDP": "#00D4AA", "SQL": "#6C5CE7"}

fig = go.Figure()

for _, row in scores_df.iterrows():
    rank = int(row["Rank"])
    bar_color = MEDAL_COLORS.get(rank, "#636e72")
    path_badge = row["Path"]
    
    fig.add_trace(go.Bar(
        y=[f"#{rank} {row['Team']}"],
        x=[row["Total"]],
        orientation="h",
        marker=dict(
            color=bar_color,
            line=dict(color="white", width=2),
            pattern=dict(shape="/" if path_badge == "SQL" else "")
        ),
        text=f"  {int(row['Total'])} pts  ({path_badge})",
        textposition="outside",
        textfont=dict(size=18, color="white"),
        showlegend=False,
        hovertemplate=(
            f"<b>{row['Team']}</b> ({path_badge})<br>"
            f"Bronze: {int(row['Bronze'])}/10<br>"
            f"Silver: {int(row['Silver'])}/15<br>"
            f"Gold: {int(row['Gold'])}/15<br>"
            f"DQ: {int(row['DQ'])}/5<br>"
            f"Gov: {int(row['Governance'])}/5<br>"
            f"Bonus: {int(row['Bonus'])}/5<br>"
            f"<b>Total: {int(row['Total'])}/55</b>"
            "<extra></extra>"
        ),
    ))

fig.update_layout(
    title=dict(
        text="DataOps Olympics — Event 1 Leaderboard",
        font=dict(size=28, color="white"),
        x=0.5,
    ),
    plot_bgcolor="#1a1a2e",
    paper_bgcolor="#16213e",
    font=dict(color="white", size=14),
    xaxis=dict(
        title="Total Points",
        range=[0, 60],
        gridcolor="#2d3436",
        showgrid=True,
    ),
    yaxis=dict(
        autorange="reversed",
        tickfont=dict(size=16, color="white"),
    ),
    height=max(350, len(scores_df) * 80 + 100),
    margin=dict(l=160, r=80, t=80, b=60),
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Category Breakdown — Stacked View

# COMMAND ----------

categories = ["Bronze", "Silver", "Gold", "DQ", "Governance", "Bonus"]
cat_colors = {
    "Bronze": "#E17055",
    "Silver": "#74B9FF",
    "Gold": "#FDCB6E",
    "DQ": "#00CEC9",
    "Governance": "#A29BFE",
    "Bonus": "#FF6B6B",
}
cat_max = {"Bronze": 10, "Silver": 15, "Gold": 15, "DQ": 5, "Governance": 5, "Bonus": 5}

ordered = scores_df.sort_values("Total", ascending=True)

fig2 = go.Figure()
for cat in categories:
    fig2.add_trace(go.Bar(
        y=ordered["Team"],
        x=ordered[cat],
        name=f"{cat} (/{cat_max[cat]})",
        orientation="h",
        marker=dict(color=cat_colors[cat], line=dict(color="#1a1a2e", width=1)),
        text=ordered[cat].astype(int),
        textposition="inside",
        textfont=dict(size=12, color="white"),
    ))

fig2.update_layout(
    barmode="stack",
    title=dict(
        text="Score Breakdown by Category",
        font=dict(size=24, color="white"),
        x=0.5,
    ),
    plot_bgcolor="#1a1a2e",
    paper_bgcolor="#16213e",
    font=dict(color="white", size=13),
    xaxis=dict(title="Points", gridcolor="#2d3436", range=[0, 60]),
    yaxis=dict(tickfont=dict(size=14)),
    legend=dict(
        orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5,
        font=dict(size=12),
    ),
    height=max(350, len(scores_df) * 80 + 120),
    margin=dict(l=120, r=40, t=100, b=60),
)
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Radar Chart — Per-Team Capability Profile

# COMMAND ----------

fig3 = go.Figure()

radar_cats = ["Bronze", "Silver", "Gold", "DQ", "Governance", "Bonus"]
radar_max  = [10, 15, 15, 5, 5, 5]

for _, row in scores_df.iterrows():
    normalized = [row[c] / m * 100 for c, m in zip(radar_cats, radar_max)]
    normalized.append(normalized[0])
    labels = [f"{c}\n({int(row[c])}/{m})" for c, m in zip(radar_cats, radar_max)]
    labels.append(labels[0])
    
    path_color = PATH_COLORS.get(row["Path"], "#636e72")
    
    fig3.add_trace(go.Scatterpolar(
        r=normalized,
        theta=labels,
        fill="toself",
        name=f"{row['Team']} ({row['Path']})",
        line=dict(width=3),
        opacity=0.7,
    ))

fig3.update_layout(
    title=dict(
        text="Team Capability Radar",
        font=dict(size=24, color="white"),
        x=0.5,
    ),
    polar=dict(
        bgcolor="#1a1a2e",
        radialaxis=dict(
            visible=True, range=[0, 100], showticklabels=True,
            gridcolor="#2d3436", tickfont=dict(color="#b2bec3"),
        ),
        angularaxis=dict(
            gridcolor="#2d3436", tickfont=dict(size=11, color="white"),
        ),
    ),
    paper_bgcolor="#16213e",
    font=dict(color="white"),
    legend=dict(font=dict(size=12)),
    height=550,
)
fig3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task Completion Heatmap

# COMMAND ----------

heatmap_data = []
for _, row in scores_df.iterrows():
    for cat, mx in cat_max.items():
        pct = row[cat] / mx * 100 if mx > 0 else 0
        heatmap_data.append({
            "Team": f"{row['Team']} ({row['Path']})",
            "Category": cat,
            "Score": int(row[cat]),
            "Max": mx,
            "Pct": round(pct, 0),
        })

hm_df = pd.DataFrame(heatmap_data)
pivot = hm_df.pivot(index="Team", columns="Category", values="Pct")
pivot = pivot[categories]

labels = hm_df.pivot(index="Team", columns="Category", values="Score")
labels = labels[categories]
maxes = {c: cat_max[c] for c in categories}
text_matrix = [[f"{int(labels.iloc[i][c])}/{maxes[c]}" for c in categories] for i in range(len(labels))]

fig4 = go.Figure(data=go.Heatmap(
    z=pivot.values,
    x=categories,
    y=pivot.index.tolist(),
    text=text_matrix,
    texttemplate="%{text}",
    textfont=dict(size=14, color="white"),
    colorscale=[
        [0, "#2d3436"],
        [0.3, "#e17055"],
        [0.6, "#fdcb6e"],
        [0.85, "#00b894"],
        [1.0, "#00cec9"],
    ],
    showscale=True,
    colorbar=dict(title="% of Max", ticksuffix="%"),
    hovertemplate="<b>%{y}</b><br>%{x}: %{text} (%{z:.0f}%)<extra></extra>",
))

fig4.update_layout(
    title=dict(
        text="Task Completion Heatmap",
        font=dict(size=24, color="white"),
        x=0.5,
    ),
    plot_bgcolor="#1a1a2e",
    paper_bgcolor="#16213e",
    font=dict(color="white", size=13),
    xaxis=dict(side="top", tickfont=dict(size=14)),
    yaxis=dict(tickfont=dict(size=13), autorange="reversed"),
    height=max(300, len(scores_df) * 70 + 100),
    margin=dict(l=180, r=40, t=100, b=40),
)
fig4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SDP vs SQL Path Comparison

# COMMAND ----------

path_summary = scores_df.groupby("Path")[categories + ["Total"]].mean().reset_index()

fig5 = go.Figure()
for cat in categories:
    fig5.add_trace(go.Bar(
        x=path_summary["Path"],
        y=path_summary[cat],
        name=cat,
        marker=dict(color=cat_colors[cat]),
        text=path_summary[cat].round(1),
        textposition="inside",
        textfont=dict(color="white", size=13),
    ))

fig5.update_layout(
    barmode="stack",
    title=dict(
        text="Average Score: SDP vs SQL Path",
        font=dict(size=24, color="white"),
        x=0.5,
    ),
    plot_bgcolor="#1a1a2e",
    paper_bgcolor="#16213e",
    font=dict(color="white"),
    xaxis=dict(tickfont=dict(size=16)),
    yaxis=dict(title="Avg Points", gridcolor="#2d3436"),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
    height=450,
)
fig5.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Summary

# COMMAND ----------

print("=" * 72)
print("  DATAOPS OLYMPICS — EVENT 1: DATA ENGINEERING")
print("  FINAL LEADERBOARD")
print("=" * 72)
for _, row in scores_df.iterrows():
    rank = int(row["Rank"])
    medal = {1: "GOLD  ", 2: "SILVER", 3: "BRONZE"}.get(rank, "      ")
    bar = int(row["Total"])
    bar_vis = "#" * bar + "." * (55 - bar)
    print(f"  {rank}. [{medal}] {row['Team']:12s} | {row['Path']:3s} | {bar_vis} {bar}/55 pts")
print("=" * 72)
print()

total_teams = len(scores_df)
sdp_teams = len(scores_df[scores_df["Path"] == "SDP"])
sql_teams = len(scores_df[scores_df["Path"] == "SQL"])
avg_total = scores_df["Total"].mean()
max_total = scores_df["Total"].max()

print(f"  Teams: {total_teams}  |  SDP: {sdp_teams}  |  SQL: {sql_teams}")
print(f"  Avg Score: {avg_total:.1f}/55  |  Top Score: {int(max_total)}/55")
print(f"  Avg Bronze: {scores_df['Bronze'].mean():.1f}/10  |  Avg Gold: {scores_df['Gold'].mean():.1f}/15")
print(f"  Bonus achieved: {(scores_df['Bonus'] > 0).sum()}/{total_teams} teams")
