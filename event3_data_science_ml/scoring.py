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
# MAGIC ### Scoring Breakdown (45 pts + 8 bonus)
# MAGIC
# MAGIC | Category | Points |
# MAGIC |----------|--------|
# MAGIC | Data Loading + EDA | 5 |
# MAGIC | Feature Engineering (regular table) | 5 |
# MAGIC | Feature Engineering (Feature Store) | **8** |
# MAGIC | Model Training + MLflow | 10 |
# MAGIC | Model Performance (F1) | 15 |
# MAGIC | Model Registration | 5 |
# MAGIC | Results Saved to Catalog | 2 |
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


def _is_feature_store_table(catalog, table):
    """Check if a table was created via Feature Engineering (has feature store metadata)."""
    fqn = f"{catalog}.{SCHEMA}.{table}"
    try:
        from databricks.feature_engineering import FeatureEngineeringClient
        fe = FeatureEngineeringClient()
        fe.get_table(name=fqn)
        return True
    except Exception:
        pass
    try:
        props = spark.sql(f"DESCRIBE EXTENDED {fqn}").filter("col_name = 'Table Properties'").collect()
        if props:
            prop_str = str(props[0]["data_type"]).lower()
            if "feature" in prop_str or "fe_table" in prop_str:
                return True
    except Exception:
        pass
    return False


def _get_team_results(catalog):
    """Read team's self-reported results from event3_results table."""
    try:
        row = spark.table(f"{catalog}.{SCHEMA}.event3_results").orderBy(
            "submitted_at", ascending=False
        ).limit(1).collect()
        if row:
            return {
                "f1_score": float(row[0]["f1_score"] or 0),
                "roc_auc": float(row[0]["roc_auc"] or 0),
                "accuracy": float(row[0]["accuracy"] or 0),
                "model_type": str(row[0]["model_type"] or "unknown"),
                "n_features": int(row[0]["n_features"] or 0),
                "mlflow_run_id": str(row[0]["mlflow_run_id"] or ""),
            }
    except Exception:
        pass
    return None


def score_team(team_name: str) -> dict:
    catalog = team_name
    scores = {
        "team": team_name, "eda": 0, "features": 0, "mlflow": 0,
        "performance": 0, "registration": 0, "results_saved": 0,
        "bonus": 0, "total": 0,
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

    # ─── Results saved to catalog (2 pts) ───
    team_results = _get_team_results(catalog)
    if team_results:
        scores["results_saved"] = 2
        log(f"Results: event3_results table found (F1={team_results['f1_score']:.4f}) [+2]")
    else:
        log("Results: event3_results table not found [+0]")

    # ─── Feature Engineering (5 pts regular, 8 pts Feature Store) ───
    best_f1, model_type, n_runs, best_run = _get_best_f1(team_name)

    has_features_table = _table_exists(catalog, "heart_features")
    is_fs = _is_feature_store_table(catalog, "heart_features") if has_features_table else False

    if is_fs:
        scores["features"] = 8
        log("Features: heart_features is a Feature Store table [+8]")
    elif has_features_table:
        scores["features"] = 5
        log("Features: heart_features exists as regular Delta table [+5]")
    elif best_run:
        n_features = int(best_run.data.params.get("n_features", 13))
        if n_features > 15:
            scores["features"] = 5
            log(f"Features: {n_features} features in MLflow (good engineering, no table saved) [+5]")
        elif n_features > 13:
            scores["features"] = 3
            log(f"Features: {n_features} features in MLflow (some engineering) [+3]")
        else:
            scores["features"] = 1
            log(f"Features: {n_features} features in MLflow (base only) [+1]")
    else:
        log("Features: no feature table or MLflow runs found [+0]")

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

    # ─── Performance (15 pts, scaled by F1) — check MLflow and event3_results ───
    if team_results and team_results["f1_score"] > best_f1:
        best_f1 = team_results["f1_score"]
        model_type = team_results["model_type"]
        log(f"  Using F1 from event3_results table ({best_f1:.4f}) — higher than MLflow")
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
        + scores["performance"] + scores["registration"]
        + scores["results_saved"] + scores["bonus"]
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
    print(f"  EDA:{r['eda']}/5  Feat:{r['features']}/8  MLflow:{r['mlflow']}/10  Perf:{r['performance']}/15  Reg:{r['registration']}/5  Results:{r['results_saved']}/2  Bonus:{r['bonus']}/8")
    print(f"  F1: {r['f1_score']:.4f}  |  TOTAL: {r['total']}/53")
    for d in r["details"]:
        print(f"    {d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leaderboard

# COMMAND ----------

df_scores = pd.DataFrame([{
    "Team": r["team"],
    "EDA": r["eda"],
    "Features": r["features"],
    "MLflow": r["mlflow"],
    "Performance": r["performance"],
    "Registration": r["registration"],
    "Results_Saved": r["results_saved"],
    "Bonus": r["bonus"],
    "Total": r["total"],
    "F1": r["f1_score"],
} for r in results]).sort_values("Total", ascending=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

spark.createDataFrame(df_scores).write.format("delta").mode("overwrite").saveAsTable(
    "dataops_olympics.default.event3_scores"
)
print("Saved to dataops_olympics.default.event3_scores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Unified Leaderboard

# COMMAND ----------

_LB = "dataops_olympics.default.olympics_leaderboard"
_RT = "dataops_olympics.default.registered_teams"
spark.sql(f"CREATE TABLE IF NOT EXISTS {_LB} (team STRING, event STRING, category STRING, points DOUBLE, max_points DOUBLE, scored_at TIMESTAMP)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {_RT} (team STRING)")

from datetime import datetime as _dt
_now = _dt.now()
_event = "Event 3: Data Science"

for r in results:
    _t = r["team"]
    if spark.sql(f"SELECT 1 FROM {_RT} WHERE team = '{_t}'").count() == 0:
        spark.sql(f"INSERT INTO {_RT} VALUES ('{_t}')")
    for cat, pts, mx in [
        ("EDA", r["eda"], 5), ("Features", r["features"], 8),
        ("MLflow", r["mlflow"], 10), ("Performance", r["performance"], 15),
        ("Registration", r["registration"], 5), ("Results_Saved", r["results_saved"], 2),
        ("Bonus", r["bonus"], 8),
    ]:
        spark.sql(f"INSERT INTO {_LB} VALUES ('{_t}', '{_event}', '{cat}', {pts}, {mx}, '{_now}')")

print(f"Leaderboard updated: {len(results)} teams × 7 categories")
