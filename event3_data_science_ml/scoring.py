# Databricks notebook source
# MAGIC %md
# MAGIC # Event 3: ML Challenge — Automated Scoring
# MAGIC
# MAGIC **FOR ORGANIZERS ONLY**
# MAGIC
# MAGIC Scores each team's ML submission based on:
# MAGIC - Data loading + EDA
# MAGIC - Feature engineering
# MAGIC - MLflow usage
# MAGIC - Model performance (F1 score)
# MAGIC - Model registration
# MAGIC - Bonus challenges
# MAGIC
# MAGIC ### Scoring Breakdown (40 pts + 8 bonus)
# MAGIC
# MAGIC | Category | Points |
# MAGIC |----------|--------|
# MAGIC | Data Loading + EDA | 5 |
# MAGIC | Feature Engineering | 5 |
# MAGIC | Model Training + MLflow | 10 |
# MAGIC | Model Performance (F1) | 15 |
# MAGIC | Model Registration | 5 |
# MAGIC | **Bonus: SHAP** | +3 |
# MAGIC | **Bonus: Ensemble** | +3 |
# MAGIC | **Bonus: Cross-Val** | +2 |

# COMMAND ----------

TEAMS = ["team_01", "team_02", "team_03", "team_04"]
SCHEMA = "default"

# COMMAND ----------

import pandas as pd
import mlflow
from mlflow.tracking import MlflowClient

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()


def _fqn(catalog, table):
    return f"{catalog}.{SCHEMA}.{table}"


def _table_exists(catalog, table):
    try:
        spark.table(_fqn(catalog, table))
        return True
    except Exception:
        return False


def _get_best_f1(team_name):
    """Find the best F1 score from team's MLflow experiment."""
    try:
        user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    except Exception:
        user_email = ""
    possible_paths = [
        f"/Users/{user_email}/{team_name}_heart_ml",
        f"/Users/{user_email}/{team_name}_ml_challenge",
    ]

    best_f1 = 0.0
    best_run = None
    model_type = "unknown"
    n_runs = 0

    for exp_path in possible_paths:
        try:
            exp = client.get_experiment_by_name(exp_path)
            if exp is None:
                continue
            runs = client.search_runs(
                experiment_ids=[exp.experiment_id],
                order_by=["metrics.f1_score DESC"],
                max_results=20,
            )
            n_runs += len(runs)
            for run in runs:
                f1 = run.data.metrics.get("f1_score", 0)
                if f1 > best_f1:
                    best_f1 = f1
                    best_run = run
                    model_type = run.data.params.get("model_type", "unknown")
        except Exception:
            continue

    return best_f1, model_type, n_runs, best_run


def _has_registered_model(catalog):
    """Check if a model is registered in the catalog."""
    try:
        models = spark.sql(f"SHOW MODELS IN {catalog}.{SCHEMA}").collect()
        return len(models) > 0
    except Exception:
        return False


def _get_user_email():
    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    except Exception:
        return ""


def score_team(team_name: str) -> dict:
    catalog = team_name
    scores = {
        "team": team_name, "eda": 0, "features": 0, "mlflow": 0,
        "performance": 0, "registration": 0, "bonus": 0, "total": 0,
        "f1_score": 0.0, "details": [],
    }

    def log(msg):
        scores["details"].append(msg)

    # ─── EDA (5 pts) ───
    if _table_exists(catalog, "heart_silver"):
        scores["eda"] = 5
        log("EDA: heart_silver exists and accessible [+5]")
    else:
        log("EDA: heart_silver not found [+0]")

    # ─── Feature Engineering (5 pts) ───
    best_f1, model_type, n_runs, best_run = _get_best_f1(team_name)

    if best_run:
        n_features = int(best_run.data.params.get("n_features", 13))
        if n_features > 15:
            scores["features"] = 5
            log(f"Features: {n_features} features (good engineering) [+5]")
        elif n_features > 13:
            scores["features"] = 3
            log(f"Features: {n_features} features (some engineering) [+3]")
        else:
            scores["features"] = 1
            log(f"Features: {n_features} features (base only) [+1]")
    else:
        log("Features: no MLflow runs found [+0]")

    # ─── MLflow (10 pts) ───
    if n_runs >= 3:
        scores["mlflow"] = 10
        log(f"MLflow: {n_runs} runs logged (multiple models tried) [+10]")
    elif n_runs >= 2:
        scores["mlflow"] = 7
        log(f"MLflow: {n_runs} runs logged [+7]")
    elif n_runs >= 1:
        scores["mlflow"] = 5
        log(f"MLflow: {n_runs} run logged [+5]")
    else:
        log("MLflow: no runs found [+0]")

    # ─── Performance (15 pts, scaled by F1) ───
    scores["f1_score"] = best_f1
    if best_f1 >= 0.90:
        scores["performance"] = 15
        log(f"Performance: F1={best_f1:.4f} (excellent) [+15]")
    elif best_f1 >= 0.85:
        scores["performance"] = 12
        log(f"Performance: F1={best_f1:.4f} (very good) [+12]")
    elif best_f1 >= 0.80:
        scores["performance"] = 10
        log(f"Performance: F1={best_f1:.4f} (good) [+10]")
    elif best_f1 >= 0.70:
        scores["performance"] = 7
        log(f"Performance: F1={best_f1:.4f} (fair) [+7]")
    elif best_f1 > 0:
        scores["performance"] = 4
        log(f"Performance: F1={best_f1:.4f} (needs improvement) [+4]")
    else:
        log("Performance: no F1 score recorded [+0]")

    if model_type != "unknown":
        log(f"  Best model type: {model_type}")

    # ─── Registration (5 pts) ───
    if _has_registered_model(catalog):
        scores["registration"] = 5
        log("Registration: model registered in UC [+5]")
    elif n_runs > 0:
        scores["registration"] = 2
        log("Registration: model logged but not registered [+2]")
    else:
        log("Registration: no model found [+0]")

    # ─── Bonus (8 pts max) ───
    # SHAP (+3): check for heart_shap_importance table
    if _table_exists(catalog, "heart_shap_importance"):
        scores["bonus"] += 3
        log("Bonus: SHAP importance table found [+3]")

    # Ensemble (+3): check for VotingClassifier in MLflow runs
    if best_run:
        try:
            user_email = _get_user_email()
            exp = client.get_experiment_by_name(
                f"/Users/{user_email}/{team_name}_heart_ml"
            )
            if exp:
                runs = client.search_runs(experiment_ids=[exp.experiment_id])
                has_ensemble = any(
                    r.data.params.get("model_type", "").lower() in ("votingclassifier", "ensemble")
                    for r in runs
                )
                if has_ensemble:
                    scores["bonus"] += 3
                    log("Bonus: Ensemble model found in MLflow [+3]")
        except Exception:
            pass

    # Cross-Val (+2): check for heart_cv_results table
    if _table_exists(catalog, "heart_cv_results"):
        scores["bonus"] += 2
        log("Bonus: CV results table found [+2]")

    scores["total"] = (
        scores["eda"] + scores["features"] + scores["mlflow"]
        + scores["performance"] + scores["registration"] + scores["bonus"]
    )
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
    print(f"  EDA:{r['eda']}/5  Feat:{r['features']}/5  MLflow:{r['mlflow']}/10  Perf:{r['performance']}/15  Reg:{r['registration']}/5  Bonus:{r['bonus']}/8")
    print(f"  F1: {r['f1_score']:.4f}  |  TOTAL: {r['total']}/48")
    for d in r["details"]:
        print(f"    {d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leaderboard

# COMMAND ----------

import plotly.graph_objects as go

df_scores = pd.DataFrame([{
    "Team": r["team"],
    "EDA": r["eda"],
    "Features": r["features"],
    "MLflow": r["mlflow"],
    "Performance": r["performance"],
    "Registration": r["registration"],
    "Bonus": r["bonus"],
    "Total": r["total"],
    "F1": r["f1_score"],
} for r in results]).sort_values("Total", ascending=False)

MEDAL_COLORS = {0: "#FFD700", 1: "#C0C0C0", 2: "#CD7F32"}
categories = ["EDA", "Features", "MLflow", "Performance", "Registration", "Bonus"]
cat_colors = {
    "EDA": "#3498db", "Features": "#e67e22", "MLflow": "#9b59b6",
    "Performance": "#2ecc71", "Registration": "#1abc9c", "Bonus": "#e74c3c",
}

ordered = df_scores.sort_values("Total", ascending=True)
fig = go.Figure()
for cat in categories:
    fig.add_trace(go.Bar(
        y=ordered["Team"],
        x=ordered[cat],
        name=cat,
        orientation="h",
        marker=dict(color=cat_colors[cat], line=dict(color="#1a1a2e", width=1)),
        text=ordered[cat].astype(int),
        textposition="inside",
        textfont=dict(size=12, color="white"),
    ))

fig.update_layout(
    barmode="stack",
    title=dict(text="Event 3: ML Challenge — Scores", font=dict(size=24, color="white"), x=0.5),
    plot_bgcolor="#1a1a2e", paper_bgcolor="#16213e",
    font=dict(color="white", size=13),
    xaxis=dict(title="Points", gridcolor="#2d3436", range=[0, 50]),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
    height=max(350, len(df_scores) * 80 + 120),
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### F1 Score Comparison

# COMMAND ----------

fig2 = go.Figure()
fig2.add_trace(go.Bar(
    x=df_scores["Team"],
    y=df_scores["F1"],
    marker=dict(
        color=df_scores["F1"],
        colorscale=[[0, "#e74c3c"], [0.7, "#f39c12"], [0.85, "#2ecc71"], [1, "#00cec9"]],
        showscale=True,
        colorbar=dict(title="F1"),
    ),
    text=df_scores["F1"].round(4),
    textposition="outside",
    textfont=dict(size=14, color="white"),
))
fig2.update_layout(
    title=dict(text="F1 Score Leaderboard", font=dict(size=22, color="white"), x=0.5),
    plot_bgcolor="#1a1a2e", paper_bgcolor="#16213e",
    font=dict(color="white"),
    yaxis=dict(title="F1 Score", range=[0, 1], gridcolor="#2d3436"),
    height=400,
)
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

spark.createDataFrame(df_scores).write.format("delta").mode("overwrite").saveAsTable(
    "dataops_olympics.default.event3_scores"
)
print("Saved to dataops_olympics.default.event3_scores")
