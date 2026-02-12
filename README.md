# DataOps Olympics — GSK India x Databricks

Welcome to the **DataOps Olympics** — a hands-on, competitive event designed to showcase the power of the Databricks Lakehouse Platform through real-world data engineering and machine learning challenges.

## Prerequisites

- **Databricks Free Edition** workspace (sign up at [databricks.com/try](https://www.databricks.com/try-databricks))
- A running compute cluster (single-node is sufficient)
- Basic familiarity with Python, SQL, and Spark

> **All data is bundled in the `data/` folder. No internet downloads or API keys needed at runtime.**

---

## Event Overview

| Event | Name | Duration | Focus Area |
|-------|------|----------|------------|
| 1 | **Speed Sprint** | 15 min | End-to-end pipeline (ingest → govern → query → visualize) |
| 1B | **Declarative Pipelines** | 20 min | Bronze → Silver → Gold with DLT + Expectations |
| 2 | **Accuracy Challenge** | 20 min | Predictive modeling (patient readmission risk) |
| 3 | **Innovation Showcase** | 30 min | AI-powered clinical decision support |
| 4 | **The Relay Challenge** | 30 min | Multi-stage team relay pipeline |
| 5 | **Plot Twist Finals** | 20 min | Adapt to surprise constraints |
| ⚡ | **Bonus Challenges** | Anytime | Liquid Clustering, Change Data Feed, UC Volumes, AI Functions |

---

## Team Structure

- **8–10 teams** of 4–5 people each
- Cross-functional: mix of data scientists, analysts, engineers
- Each team gets a workstation with a pre-configured Databricks workspace

---

## Repository Structure

```
gsk-dataops-olympics/
├── README.md                          ← You are here
│
├── data/                              ← Bundled datasets (no downloads needed)
│   ├── heart_disease.csv              ← 500 records, UCI-style
│   ├── heart_disease_batch_1-3.csv    ← Incremental batches for DLT (with quality issues)
│   ├── diabetes_readmission.csv       ← 768 records, readmission risk
│   ├── life_expectancy.csv            ← 480 records, WHO indicators
│   ├── life_expectancy_sample.json    ← 100 records (JSON format)
│   ├── drug_reviews.csv               ← 1,000 drug review records
│   └── clinical_notes.json            ← 20 clinical notes (NLP)
│
├── setup/
│   └── 00_setup_and_data.py           ← Run FIRST: loads data, creates tables
│
├── event1_speed_sprint/
│   ├── starter_notebook.py            ← Participant notebook (with TODOs)
│   └── solution_notebook.py           ← Reference solution (organizers only)
│
├── event1b_declarative_pipelines/     ← NEW: Modern data engineering
│   ├── starter_notebook.py            ← DLT pipeline with Expectations
│   └── solution_notebook.py           ← Full medallion architecture solution
│
├── event2_accuracy_challenge/
│   ├── starter_notebook.py
│   └── solution_notebook.py
│
├── event3_innovation_showcase/
│   ├── starter_notebook.py
│   └── solution_notebook.py
│
├── event4_relay_challenge/
│   ├── leg1_ingestion.py              ← Person A: Data ingestion (7 min)
│   ├── leg2_feature_engineering.py    ← Person B: Feature engineering (8 min)
│   ├── leg3_model_training.py         ← Person C: Model training + MLflow (10 min)
│   └── leg4_deployment.py             ← Person D: Batch predictions + dashboard (5 min)
│
├── event5_plot_twist/
│   ├── base_notebook.py               ← Working solution to adapt
│   └── twist_cards.py                 ← 8 random challenge cards
│
├── bonus_challenges/                  ← NEW: Modern Databricks features
│   └── advanced_features.py           ← Liquid Clustering, CDF, Volumes, AI Functions
│
└── scoring/
    └── scoreboard.py                  ← Live scoreboard with charts
```

---

## Quick Start

### Step 1: Import into Databricks
1. In your Databricks workspace, go to **Workspace → Repos → Add Repo**
2. Paste this repository's Git URL
3. Or use **Workspace → Import** to upload individual `.py` notebooks

### Step 2: Run Setup
1. Open `setup/00_setup_and_data.py`
2. Attach to your compute cluster
3. Run all cells — this loads bundled data and creates Delta tables

### Step 3: Distribute Starter Notebooks
- Give each team the **starter notebooks** for each event
- Keep **solution notebooks** for organizers/judges only

---

## Datasets (All Bundled)

| Dataset | File | Records | Used In |
|---------|------|---------|---------|
| Heart Disease | `data/heart_disease.csv` | 500 | Events 1, 1B, 4, 5 |
| Heart Batches | `data/heart_disease_batch_*.csv` | 50 each | Event 1B (DLT) |
| Diabetes/Readmission | `data/diabetes_readmission.csv` | 768 | Event 2 |
| WHO Life Expectancy | `data/life_expectancy.csv` | 480 | Events 1, 4 |
| Life Expectancy (JSON) | `data/life_expectancy_sample.json` | 100 | Event 1 |
| Drug Reviews | `data/drug_reviews.csv` | 1,000 | Event 3 |
| Clinical Notes | `data/clinical_notes.json` | 20 | Event 3 |

---

## Modern Databricks Features Covered

### Core Events
| Feature | Event | What Participants Learn |
|---------|-------|----------------------|
| **Delta Lake** | All | Table format, ACID transactions, time travel |
| **Unity Catalog** | 1, 1B | Catalog/schema governance, table comments |
| **MLflow** | 2, 3, 4 | Experiment tracking, model logging, runs |
| **Spark SQL** | All | Distributed queries, aggregations |
| **Plotly Visualizations** | 1, 2, 4 | Interactive charts in notebooks |

### Event 1B — Declarative Pipelines (DLT)
| Feature | What Participants Learn |
|---------|----------------------|
| **`@dlt.table` decorator** | Declarative table definitions |
| **`@dlt.expect` / `@dlt.expect_or_drop`** | Data quality expectations |
| **Medallion Architecture** | Bronze → Silver → Gold pattern |
| **Auto-managed pipelines** | Orchestration handled by Databricks |

### Bonus Challenges
| Feature | What Participants Learn |
|---------|----------------------|
| **Liquid Clustering** | Modern alternative to partitioning + Z-ordering |
| **Change Data Feed** | Row-level change tracking (INSERT/UPDATE/DELETE) |
| **Unity Catalog Volumes** | Managed file storage with governance |
| **Predictive Optimization** | Auto-OPTIMIZE and VACUUM |
| **AI Functions (SQL)** | `ai_classify()`, `ai_query()`, `ai_extract()` |
| **Delta Time Travel** | Query any historical version of a table |

---

## Scoring Summary

### Event 1: Speed Sprint (15 min) — max 10 pts
First place = 10, Second = 8, Third = 6, ... 6th+ = 2 pts

### Event 1B: Declarative Pipelines (20 min) — max 15 pts
Bronze layer (3) + Silver with expectations (4) + Gold aggregates (3) + Pipeline runs (3) + CDF bonus (2)

### Event 2: Accuracy Challenge (20 min) — max 20 pts
F1 Score (max 15) + Explainability bonus (max 5)

### Event 3: Innovation Showcase (30 min) — max 30 pts
Creativity (10) + Functionality (10) + Usefulness (5) + Demo quality (5)

### Event 4: Relay Challenge (30 min) — max 25 pts
Completion time (15) + Quality gates (10) | Penalty: +2 min per failed checkpoint

### Event 5: Plot Twist Finals (20 min) — max 25 pts
Top 3 teams only: Adaptation (10) + Quality (10) + Presentation (5)

### Bonus Challenges — max 12 pts
2 pts per completed section (6 sections)

### Grand Total: max 137 pts

---

## Databricks Free Edition Compatibility

| Feature | Free Edition | Notebook Approach |
|---------|-------------|-------------------|
| Delta Lake | ✅ Available | Used throughout |
| Unity Catalog | ✅ Available | Catalog/schema governance |
| Declarative Pipelines (DLT) | ✅ Available | Event 1B |
| MLflow | ✅ Available | Experiment tracking |
| Liquid Clustering | ✅ Available | Bonus challenge |
| Change Data Feed | ✅ Available | Bonus challenge |
| Spark SQL | ✅ Available | Primary query engine |
| pandas / sklearn | ✅ Available | ML modeling |
| AI Functions | ⚠️ May be limited | Fallback included |
| Vector Search | ⚠️ May be limited | ChromaDB fallback |
| Model Serving | ⚠️ May be limited | Batch inference fallback |

> All notebooks include conditional logic to detect features and fall back gracefully.

---

## For Organizers

### Pre-Event Checklist
- [ ] Create Databricks Free Edition workspaces (1 per team)
- [ ] Import this repo into each workspace via **Repos**
- [ ] Run `setup/00_setup_and_data.py` on each workspace
- [ ] Verify all 5 tables are created
- [ ] Import starter notebooks into each workspace
- [ ] Set up `scoring/scoreboard.py` on the organizer workspace
- [ ] Print twist cards from `event5_plot_twist/twist_cards.py`
- [ ] Assign judges for Events 3 and 5
- [ ] Prepare timers (15 / 20 / 30 min)

### Event Day Schedule (Suggested)
| Time | Activity | Duration |
|------|----------|----------|
| 0:00 | Welcome & Team Formation | 15 min |
| 0:15 | Setup & Environment Check | 10 min |
| 0:25 | **Event 1: Speed Sprint** | 15 min |
| 0:40 | **Event 1B: Declarative Pipelines** | 20 min |
| 1:00 | Break | 10 min |
| 1:10 | **Event 2: Accuracy Challenge** | 20 min |
| 1:30 | **Event 3: Innovation Showcase** | 30 min |
| 2:00 | Break | 10 min |
| 2:10 | **Event 4: Relay Challenge** | 30 min |
| 2:40 | Scores Tabulation | 10 min |
| 2:50 | **Event 5: Plot Twist Finals** (Top 3) | 20 min |
| 3:10 | Awards & Closing | 15 min |
| **Total** | | **~3.5 hours** |

### Tips
- Have a "help desk" for environment issues
- Project the scoreboard on a big screen
- Bonus challenges can be tackled during downtime or breaks
- Award extra points for team spirit and collaboration

---

## License

This project uses synthetic and open-source datasets for educational and demonstration purposes only. All datasets retain their original licenses.

---

**Happy competing! May the best data team win!**
