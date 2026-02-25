# DataOps Olympics — GSK India x Databricks

Welcome to the **DataOps Olympics** — a hands-on, competitive event where GSK India data teams race through real-world data engineering, analytics, ML, and GenAI challenges on the Databricks Lakehouse Platform.

**Part of the Databricks team visit to GSK India GCC, Bengaluru — March 12-17, 2026**

---

## Event Format

Each event follows a **Lightning Talk + Build** format:

1. **Lightning Talk (15 min)** — Organizer runs a live demo showing the complete solution
2. **Build Session** — Teams replicate and extend the demo on their own workspaces
3. **Scoring** — Fastest / most accurate / most creative wins

> **Vibe Coding Encouraged!** Teams are encouraged to use the **Databricks Assistant** to help them build. Difficulty is calibrated accordingly.

> **Safety Net:** If a team doesn't complete an event, the organizer clones the demo data/tables to their workspace so they can continue to the next event with a clean starting point.

---

## The 4 Events + Plot Twist

| # | Event | Focus | How You Win |
|---|-------|-------|-------------|
| **1** | **Data Engineering** | Ingest, Delta tables, governance, Lakeflow Designer | Fastest to complete the pipeline |
| **2** | **Data Analytics** | AI/BI Dashboards, Genie space, SQL queries | Most accurate answers to benchmark questions |
| **3** | **Data Science / ML** | Classic ML, model training, MLflow registration | Highest model accuracy (F1 score) |
| **4** | **GenAI / Agents** | AgentBricks, Genie tools, prompt engineering, evaluation | Best agent evaluated against test prompts |
| **5** | **Plot Twist Challenge** | Adapt under constraints | Speed + quality of adaptation |

**All tasks are related** — data flows from one event to the next as a cohesive story. The heart disease dataset ingested in Event 1 becomes the data behind the dashboard in Event 2, the training data for the model in Event 3, and the knowledge base for the agent in Event 4.

**Total Duration: ~2 hours** (excluding breaks and awards)

---

## Team Structure

- **8-10 teams** of 4-5 people each
- Cross-functional: mix of data scientists, analysts, engineers
- Each team gets a workstation with a pre-configured Databricks workspace

---

## Event Details

### Event 1: Data Engineering — Speed Sprint

**Duration:** ~20 min build | **Scoring:** First to finish wins

Teams race to:
- Ingest CSV and JSON healthcare data into their workspace
- Create governed Delta tables with Unity Catalog
- Add table/column comments for governance
- Build a basic medallion flow using Lakeflow Designer (DLT)

**Dataset:** Heart Disease (500 rows CSV) + Life Expectancy (JSON)

---

### Event 2: Data Analytics — Dashboard & Genie Challenge

**Duration:** ~20 min build | **Scoring:** Accuracy against benchmark questions

Teams build:
- An **AI/BI (Lakeview) Dashboard** with key visualizations from the heart disease data
- A **Genie space** connected to their data, enabling natural language queries
- Answer a pool of benchmark questions — the team with the most correct answers wins

**Dataset:** Heart Disease Delta tables (from Event 1)

---

### Event 3: Data Science / ML — Model Accuracy Challenge

**Duration:** ~20 min build | **Scoring:** Highest F1 score on test set

Teams build:
- A classification model predicting patient readmission risk
- Feature engineering to improve accuracy
- Full experiment tracking with **MLflow** (log params, metrics, model)
- Register the best model in the MLflow Model Registry

**Dataset:** Diabetes Readmission (768 rows)

---

### Event 4: GenAI / Agents — Agent Building Challenge

**Duration:** ~25 min build | **Scoring:** Agent evaluation against test prompts

Teams build:
- An AI agent using **AgentBricks** that can answer clinical questions
- Connect the agent to a **Genie space** as a tool for structured data queries
- Write system prompts and configure the agent's behavior
- Agent is evaluated against a set of test prompts using an evaluation script

**Dataset:** Clinical Notes (JSON) + Drug Reviews + Heart Disease data

---

### Event 5: Plot Twist Challenge

**Duration:** ~30 min total (10 min per twist) | **Scoring:** Adaptation speed + quality

Three specific twists, applied one at a time to the team's existing work:

| Twist | What Happens | What Teams Must Do |
|-------|-------------|-------------------|
| **Schema Change** | Source data schema changes mid-pipeline | Heal the data engineering pipeline — handle new/changed columns without breaking |
| **New Genie Data Source** | Users start asking questions the current Genie space can't answer | Add a new data source to the Genie room so it can answer the new questions |
| **Multi-Agent** | Requirement to add another agent to the system | Extend the single agent into a multi-agent system with specialized roles |

---

## Repository Structure

```
gsk-dataops-olympics/
├── README.md                          <- You are here
│
├── data/                              <- Bundled datasets (no downloads needed)
│   ├── heart_disease.csv              <- 500 records, UCI-style
│   ├── heart_disease_batch_1-3.csv    <- Incremental batches (with quality issues)
│   ├── diabetes_readmission.csv       <- 768 records, readmission risk
│   ├── life_expectancy.csv            <- 480 records, WHO indicators
│   ├── life_expectancy_sample.json    <- 100 records (JSON format)
│   ├── drug_reviews.csv               <- 1,000 drug review records
│   └── clinical_notes.json            <- 20 clinical notes (NLP)
│
├── setup/
│   └── 00_setup_and_data.py           <- Run FIRST: loads data, creates tables
│
├── demos/                             <- Lightning talk demos (organizer only)
│   ├── demo1_data_engineering.py      <- 15-min demo: ingestion + governance
│   ├── demo2_data_analytics.py        <- 15-min demo: dashboard + Genie
│   ├── demo3_data_science_ml.py       <- 15-min demo: ML + MLflow
│   └── demo4_genai_agents.py          <- 15-min demo: AgentBricks + evaluation
│
├── event1_data_engineering/
│   ├── starter_notebook.py            <- Participant notebook (with TODOs)
│   └── solution_notebook.py           <- Reference solution (organizers only)
│
├── event2_data_analytics/
│   ├── starter_notebook.py
│   └── solution_notebook.py
│
├── event3_data_science_ml/
│   ├── starter_notebook.py
│   └── solution_notebook.py
│
├── event4_genai_agents/
│   ├── starter_notebook.py
│   └── solution_notebook.py
│
├── event5_plot_twist/
│   ├── twist1_schema_change.py        <- Schema change data + instructions
│   ├── twist2_new_genie_source.py     <- New data source for Genie
│   └── twist3_multi_agent.py          <- Multi-agent extension challenge
│
└── scoring/
    └── scoreboard.py                  <- Live scoreboard with charts
```

---

## Quick Start

### Step 1: Import into Databricks
1. In your Databricks workspace, go to **Workspace > Repos > Add Repo**
2. Paste this repository's Git URL
3. Or use **Workspace > Import** to upload individual `.py` notebooks

### Step 2: Run Setup
1. Open `setup/00_setup_and_data.py`
2. Attach to your compute cluster
3. Run all cells — this loads bundled data and creates Delta tables

### Step 3: Distribute Starter Notebooks
- Give each team the **starter notebooks** for each event
- Keep **solution notebooks** and **demos/** for organizers/judges only

---

## Datasets (All Bundled)

| Dataset | File | Records | Used In |
|---------|------|---------|---------|
| Heart Disease | `data/heart_disease.csv` | 500 | Events 1, 2, 3, 5 |
| Heart Batches | `data/heart_disease_batch_*.csv` | 50 each | Event 1 (incremental) |
| Diabetes/Readmission | `data/diabetes_readmission.csv` | 768 | Event 3 |
| WHO Life Expectancy | `data/life_expectancy.csv` | 480 | Event 1 |
| Life Expectancy (JSON) | `data/life_expectancy_sample.json` | 100 | Event 1 |
| Drug Reviews | `data/drug_reviews.csv` | 1,000 | Event 4 |
| Clinical Notes | `data/clinical_notes.json` | 20 | Event 4 |

> **All data is bundled in the `data/` folder. No internet downloads or API keys needed at runtime.**

---

## Scoring

| Event | Max Points | How Scored |
|-------|-----------|------------|
| **1. Data Engineering** | Gold | Fastest complete pipeline |
| **2. Data Analytics** | Gold | Most accurate benchmark answers |
| **3. Data Science / ML** | Gold | Highest F1 score |
| **4. GenAI / Agents** | Gold | Best agent evaluation score |
| **5. Plot Twist** | Judges' Pick | Adaptation speed + quality + presentation |

**4 Gold Awards** — one per competitive event, plus a judges' pick for the plot twist.

---

## Event Day Schedule

| Time | Activity | Duration |
|------|----------|----------|
| 0:00 | Welcome, Team Formation & Setup | 15 min |
| 0:15 | **Lightning Talk 1:** Data Engineering Demo | 15 min |
| 0:30 | **Event 1:** Data Engineering Build | 20 min |
| 0:50 | **Lightning Talk 2:** Data Analytics Demo | 15 min |
| 1:05 | **Event 2:** Data Analytics Build | 20 min |
| 1:25 | Break | 10 min |
| 1:35 | **Lightning Talk 3:** Data Science / ML Demo | 15 min |
| 1:50 | **Event 3:** Data Science / ML Build | 20 min |
| 2:10 | **Lightning Talk 4:** GenAI / Agents Demo | 15 min |
| 2:25 | **Event 4:** GenAI / Agents Build | 25 min |
| 2:50 | Break + Score Tabulation | 10 min |
| 3:00 | **Event 5:** Plot Twist Challenge | 30 min |
| 3:30 | Awards & Closing | 15 min |
| **Total** | | **~3.75 hours** |

---

## Databricks Features Covered

| Feature | Event | What Teams Learn |
|---------|-------|-----------------|
| **Delta Lake** | 1, 2, 3 | ACID transactions, table format, time travel |
| **Unity Catalog** | 1 | Catalog/schema governance, table comments |
| **Lakeflow Designer (DLT)** | 1 | Visual pipeline builder, medallion architecture |
| **AI/BI Dashboards (Lakeview)** | 2 | Interactive dashboards, visualizations |
| **Genie** | 2, 4 | Natural language data querying |
| **MLflow** | 3 | Experiment tracking, model logging, registry |
| **AgentBricks** | 4 | AI agent building, tool use, evaluation |
| **Databricks Assistant** | All | Vibe coding — AI-assisted development |

---

## For Organizers

### Pre-Event Checklist
- [ ] Provision Databricks workspaces (1 per team, ~10 workspaces)
- [ ] Import this repo into each workspace via **Repos**
- [ ] Run `setup/00_setup_and_data.py` on each workspace
- [ ] Verify all tables are created
- [ ] Prepare lightning talk demos (organizer workspace)
- [ ] Set up `scoring/scoreboard.py` on the organizer workspace
- [ ] Prepare Genie benchmark question pool for Event 2
- [ ] Prepare agent evaluation prompts for Event 4
- [ ] Prepare twist data for Event 5
- [ ] Assign judges for Events 4 and 5
- [ ] Prepare timers and awards (4 gold trophies)

### Tips
- Have a "help desk" for environment issues
- Project the scoreboard on a big screen
- **Safety net**: After each event, clone demo solution data to teams that didn't complete, so everyone starts the next event on equal footing
- The lightning talks are crucial — they show teams exactly what they need to build

---

## License

This project uses synthetic and open-source datasets for educational and demonstration purposes only. All datasets retain their original licenses.

---

**Happy competing! May the best data team win!**
