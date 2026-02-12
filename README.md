# DataOps Olympics - GSK India x Databricks

Welcome to the **DataOps Olympics** — a hands-on, competitive event designed to showcase the power of the Databricks Lakehouse Platform through real-world data engineering and machine learning challenges.

## Prerequisites

- **Databricks Free Edition** workspace (sign up at [databricks.com/try](https://www.databricks.com/try-databricks))
- A running compute cluster (single-node is sufficient)
- Basic familiarity with Python, SQL, and Spark

> **All data used in this event is open-source and publicly available. No proprietary or sensitive data is required.**

---

## Event Overview

| Event | Name | Duration | Focus Area |
|-------|------|----------|------------|
| 1 | **Speed Sprint** | 15 min | End-to-end pipeline (ingest → govern → query → visualize) |
| 2 | **Accuracy Challenge** | 20 min | Predictive modeling (patient readmission risk) |
| 3 | **Innovation Showcase** | 30 min | AI Agent / LLM-powered application |
| 4 | **The Relay Challenge** | 30 min | Multi-stage team relay pipeline |
| 5 | **Plot Twist Finals** | 20 min | Adapt to surprise constraints |

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
├── setup/
│   └── 00_setup_and_data.py           ← Run FIRST: downloads data, verifies environment
│
├── event1_speed_sprint/
│   ├── starter_notebook.py            ← Participant notebook (with TODOs)
│   └── solution_notebook.py           ← Reference solution (organizers only)
│
├── event2_accuracy_challenge/
│   ├── starter_notebook.py            ← Participant notebook (with TODOs)
│   └── solution_notebook.py           ← Reference solution (organizers only)
│
├── event3_innovation_showcase/
│   ├── starter_notebook.py            ← Participant notebook (with TODOs)
│   └── solution_notebook.py           ← Reference solution (organizers only)
│
├── event4_relay_challenge/
│   ├── leg1_ingestion.py              ← Person A: Data ingestion & cleansing
│   ├── leg2_feature_engineering.py    ← Person B: Feature engineering
│   ├── leg3_model_training.py         ← Person C: Model training & MLflow
│   └── leg4_deployment.py             ← Person D: Deployment & dashboarding
│
├── event5_plot_twist/
│   ├── base_notebook.py               ← Working solution to be adapted
│   └── twist_cards.py                 ← Random challenge card generator
│
└── scoring/
    └── scoreboard.py                  ← Live scoreboard notebook
```

---

## Quick Start

### Step 1: Import into Databricks
1. In your Databricks workspace, go to **Workspace → Import**
2. Import this entire repository (via Git URL or upload as `.dbc`)
3. Or import individual `.py` files as notebooks

### Step 2: Run Setup
1. Open `setup/00_setup_and_data.py`
2. Attach to your compute cluster
3. Run all cells — this downloads all open-source datasets and creates the database

### Step 3: Distribute Starter Notebooks
- Give each team the **starter notebooks** for each event
- Keep **solution notebooks** for organizers/judges only

---

## Datasets Used (All Open Source)

| Dataset | Source | Used In |
|---------|--------|---------|
| Heart Disease UCI | [UCI ML Repository](https://archive.ics.uci.edu/dataset/45/heart+disease) | Events 1, 4, 5 |
| Diabetes (Pima Indians) | [Kaggle / UCI](https://www.kaggle.com/datasets/uciml/pima-indians-diabetes-database) | Event 2 |
| Drug Review Dataset | [UCI ML Repository](https://archive.ics.uci.edu/dataset/462/drug+review+dataset+drugs+com) | Event 3 |
| WHO Life Expectancy | [Kaggle](https://www.kaggle.com/datasets/kumarajarshi/life-expectancy-who) | Event 4 |
| Synthetic Clinical Notes | Generated in setup | Event 3 |

> All datasets are downloaded programmatically during setup. No manual download needed.

---

## Scoring Summary

### Event 1: Speed Sprint (15 min)
| Place | Points |
|-------|--------|
| 1st | 10 |
| 2nd | 8 |
| 3rd | 6 |
| 4th | 5 |
| 5th | 4 |
| 6th+ | 2 |

### Event 2: Accuracy Challenge (20 min)
- **F1 Score** (max 15 pts): Proportional to best score
- **Explainability Bonus** (max 5 pts): Judge-rated
- **Total**: max 20 pts

### Event 3: Innovation Showcase (30 min)
- **Creativity** (max 10 pts)
- **Functionality** (max 10 pts)
- **Usefulness** (max 5 pts)
- **Live Demo Quality** (max 5 pts)
- **Total**: max 30 pts

### Event 4: The Relay Challenge (30 min)
- **Completion Time** (max 15 pts): Inversely proportional
- **Quality Gates** (max 10 pts): Automated checks
- **Penalty**: 2 minutes added per failed checkpoint
- **Total**: max 25 pts

### Event 5: Plot Twist Finals (20 min)
- **Top 3 teams only** (from cumulative scores)
- **Adaptation Speed** (max 10 pts)
- **Solution Quality** (max 10 pts)
- **Presentation** (max 5 pts)
- **Total**: max 25 pts

### Grand Total: max 130 pts

---

## Databricks Free Edition Compatibility

All notebooks are designed to run on **Databricks Free Edition** with these considerations:

| Feature | Free Edition | Notebook Approach |
|---------|-------------|-------------------|
| Delta Lake | ✅ Available | Used throughout |
| Unity Catalog | ✅ Available | Used for governance |
| MLflow | ✅ Available | Used for experiment tracking |
| Spark SQL | ✅ Available | Primary query engine |
| pandas / sklearn | ✅ Available | Used for ML |
| Databricks AI Functions | ⚠️ Limited | Fallback to open-source LLMs |
| Vector Search | ⚠️ Limited | ChromaDB fallback included |
| Model Serving | ⚠️ Limited | Local inference fallback |
| Genie Dashboard | ⚠️ May not be available | Matplotlib/Plotly fallback |

> Notebooks include conditional logic to detect available features and gracefully fall back to alternatives.

---

## For Organizers

### Pre-Event Checklist
- [ ] Create Databricks Free Edition workspaces (1 per team)
- [ ] Run `setup/00_setup_and_data.py` on each workspace
- [ ] Verify all datasets downloaded successfully
- [ ] Import starter notebooks into each workspace
- [ ] Set up `scoring/scoreboard.py` on organizer workspace
- [ ] Print twist cards for Event 5 (in `event5_plot_twist/twist_cards.py`)
- [ ] Assign judges for Events 3 and 5
- [ ] Prepare timers (15 min, 20 min, 30 min)

### Tips
- Have a "help desk" for teams that get stuck on environment issues
- Project the scoreboard on a big screen
- Take photos/videos for internal comms
- Award bonus points for team spirit and collaboration

---

## License

This project uses open-source datasets and is intended for educational and demonstration purposes only. All datasets retain their original licenses. See individual dataset sources for details.

---

**Happy competing! May the best data team win! 🏆**
