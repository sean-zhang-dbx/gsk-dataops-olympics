# DataOps Olympics — GSK India x Databricks

Welcome to the **DataOps Olympics** — a hands-on, competitive event where GSK India data teams race through real-world data engineering, analytics, ML, and GenAI challenges on the Databricks Lakehouse Platform.

**Part of the Databricks team visit to GSK India GCC, Bengaluru — March 12-17, 2026**

---

## Vibe Coding Philosophy

This competition is calibrated for teams using **AI-assisted development**. Every event includes a **Databricks Assistant Prompt Gallery** with ready-to-use prompts, and a set of **Stretch Goals** designed to be tackled with AI help.

**What is Vibe Coding?**
Instead of writing every line by hand, you describe what you want in natural language and let the Databricks Assistant generate the code. You iterate on prompts, not syntax. Teams that master this workflow will finish events faster and tackle harder challenges.

- **Standard Mode:** Code generation, explanation, debugging, SQL from English
- **Agent Mode (with skills):** Multi-step tasks — create dashboards, Genie spaces, pipelines from a description

> The **Warm-Up Sprint** teaches this workflow before points are on the line.

---

## Event Format

Each event follows a **Lightning Talk + Build** format:

1. **Lightning Talk (15 min)** — Organizer runs a live demo showing the complete solution
2. **Build Session** — Teams replicate and extend the demo on their own workspaces
3. **Scoring** — Fastest / most accurate / most creative wins

> **Safety Net:** If a team doesn't complete an event, the organizer clones the demo data/tables to their workspace so they can continue to the next event with a clean starting point.

---

## The Schedule

| # | Event | Focus | Duration | How You Win |
|---|-------|-------|----------|-------------|
| **W** | **Warm-Up: Vibe Coding Sprint** | Databricks Assistant workflow | 10 min | First to finish (bragging rights) |
| **1** | **Data Engineering** | Ingest, Delta tables, governance, medallion | 20 min build | Fastest to complete the pipeline |
| **2** | **Data Analytics** | AI/BI Dashboards, Genie space, SQL queries | 20 min build | Most accurate benchmark answers |
| **3** | **Data Science / ML** | Classic ML, model training, MLflow | 20 min build | Highest model accuracy (F1 score) |
| **4** | **GenAI / Agents** | Agent building, Genie tools, prompt engineering | 25 min build | Best agent evaluation score |
| **5** | **Capstone: End-to-End Build** | Pipeline + Dashboard + ML/Agent/App | 30 min build | Judges rate completeness + presentation |

**All tasks are related** — data flows from one event to the next. The heart disease dataset ingested in Event 1 becomes the data behind the dashboard in Event 2, the training data for the model in Event 3, and the knowledge base for the agent in Event 4. Event 5 (Capstone) uses new data to build everything from scratch.

**Total Duration: ~3.5 hours** (including breaks and awards)

---

## Event Day Schedule

| Time | Activity | Duration |
|------|----------|----------|
| 0:00 | Welcome, Team Formation & Setup | 15 min |
| 0:15 | **Warm-Up:** Vibe Coding Sprint | 10 min |
| 0:25 | **Lightning Talk 1:** Data Engineering Demo | 15 min |
| 0:40 | **Event 1:** Data Engineering Build | 20 min |
| 1:00 | **Lightning Talk 2:** Data Analytics Demo | 15 min |
| 1:15 | **Event 2:** Data Analytics Build | 20 min |
| 1:35 | Break | 10 min |
| 1:45 | **Lightning Talk 3:** Data Science / ML Demo | 15 min |
| 2:00 | **Event 3:** Data Science / ML Build | 20 min |
| 2:20 | **Lightning Talk 4:** GenAI / Agents Demo | 15 min |
| 2:35 | **Event 4:** GenAI / Agents Build | 25 min |
| 3:00 | Break + Score Tabulation | 10 min |
| 3:10 | **Event 5:** Capstone — End-to-End Build | 30 min |
| 3:40 | Presentations (3 min per team) + Awards | 20 min |
| **Total** | | **~4 hours** |

---

## Team Structure

- **8-10 teams** of 4-5 people each
- Cross-functional: mix of data scientists, analysts, engineers
- Each team gets a workstation with a pre-configured Databricks workspace

---

## Repository Structure

```
gsk-dataops-olympics/
├── README.md                              <- You are here
│
├── data/                                  <- Bundled datasets (no downloads needed)
│   ├── heart_disease.csv                  <- 500 records, UCI-style
│   ├── heart_disease_batch_1-3.csv        <- Incremental batches (with quality issues)
│   ├── diabetes_readmission.csv           <- 768 records, readmission risk
│   ├── life_expectancy.csv                <- 480 records, WHO indicators
│   ├── life_expectancy_sample.json        <- 100 records (JSON format)
│   ├── drug_reviews.csv                   <- 1,000 drug review records
│   └── clinical_notes.json               <- 20 clinical notes (NLP)
│
├── setup/
│   └── 00_setup_and_data.py               <- Run FIRST: loads data, creates tables
│
├── warmup_vibe_coding/                    <- NEW: AI-assisted coding warm-up
│   ├── vibe_coding_sprint.py              <- 10-min warm-up challenge
│   └── agent_skills_guide.py              <- Assistant tutorial + ai-dev-kit install
│
├── demos/                                 <- Lightning talk demos (organizer only)
│   ├── demo1_data_engineering.py
│   ├── demo2_data_analytics.py
│   ├── demo3_data_science_ml.py
│   └── demo4_genai_agents.py
│
├── event1_data_engineering/
│   ├── starter_notebook.py                <- With prompt gallery + stretch goals
│   └── solution_notebook.py
│
├── event1b_declarative_pipelines/         <- Optional SDP challenge
│   ├── starter_notebook.py
│   └── solution_notebook.py
│
├── event2_data_analytics/
│   ├── starter_notebook.py                <- With prompt gallery + stretch goals
│   └── solution_notebook.py
│
├── event3_data_science_ml/
│   ├── starter_notebook.py                <- With prompt gallery + stretch goals
│   └── solution_notebook.py
│
├── event4_genai_agents/
│   ├── starter_notebook.py                <- With prompt gallery + stretch goals
│   └── solution_notebook.py
│
├── event5_capstone/                       <- NEW: End-to-end build challenge
│   └── capstone_challenge.py              <- Pipeline + Dashboard + Intelligence
│
├── bonus/                                 <- Optional extras
│   ├── twist1_schema_change.py
│   ├── twist2_new_genie_source.py
│   └── twist3_multi_agent.py
│
├── bonus_challenges/
│   └── advanced_features.py               <- Liquid Clustering, CDF, AI Functions, etc.
│
└── scoring/
    └── scoreboard.py                      <- Live scoreboard with charts
```

---

## Datasets (All Bundled)

| Dataset | File | Records | Used In |
|---------|------|---------|---------|
| Heart Disease | `data/heart_disease.csv` | 500 | Events 1, 2, Warm-Up |
| Heart Batches | `data/heart_disease_batch_*.csv` | 50 each | Event 1b (SDP) |
| Diabetes/Readmission | `data/diabetes_readmission.csv` | 768 | Event 3 |
| WHO Life Expectancy | `data/life_expectancy.csv` | 480 | Event 1 |
| Life Expectancy (JSON) | `data/life_expectancy_sample.json` | 100 | Event 1 |
| Drug Reviews | `data/drug_reviews.csv` | 1,000 | Events 4, 5 |
| Clinical Notes | `data/clinical_notes.json` | 20 | Events 4, 5 |

> **All data is bundled in the `data/` folder. No internet downloads or API keys needed at runtime.**

---

## Scoring & Awards

| Event | Max Points | How Scored |
|-------|-----------|------------|
| **W. Warm-Up** | Not scored | First to finish gets applause |
| **1. Data Engineering** | Gold | Fastest complete pipeline |
| **2. Data Analytics** | Gold | Most accurate benchmark answers |
| **3. Data Science / ML** | Gold | Highest F1 score |
| **4. GenAI / Agents** | Gold | Best agent evaluation score |
| **5. Capstone** | 25 pts (judged) | Completeness + quality + presentation |

**Awards:**
- **4 Gold Awards** — one per competitive event (Events 1-4)
- **Capstone Champion** — best end-to-end build (Event 5)
- **Best Vibe Coder** — team that most effectively used AI assistance throughout the day (judges' pick across all events)

---

## Databricks Features Covered

| Feature | Event | What Teams Learn |
|---------|-------|-----------------|
| **Delta Lake** | 1, 2, 3, 5 | ACID transactions, table format, time travel |
| **Unity Catalog** | 1 | Catalog/schema governance, table comments |
| **Spark Declarative Pipelines (SDP)** | 1b | Medallion architecture, data quality expectations |
| **AI/BI Dashboards (Lakeview)** | 2, 5 | Interactive dashboards, visualizations |
| **Genie** | 2, 4, 5 | Natural language data querying |
| **MLflow** | 3, 5 | Experiment tracking, model logging, registry |
| **AgentBricks** | 4 | AI agent building, tool use, evaluation |
| **Databricks Assistant** | All | Vibe coding — AI-assisted development |
| **Agent Skills (ai-dev-kit)** | All | Agent mode for dashboards, pipelines, Genie |
| **Liquid Clustering** | Bonus | Adaptive Delta table optimization |
| **Change Data Feed** | Bonus | Row-level change tracking |
| **AI Functions** | Bonus | ai_classify(), ai_query() in SQL |

---

## For Organizers

### Pre-Event Checklist
- [ ] Provision Databricks workspaces (1 per team, ~10 workspaces)
- [ ] Import this repo into each workspace via **Repos**
- [ ] Run `setup/00_setup_and_data.py` on each workspace
- [ ] Verify all tables are created (`heart_disease`, `diabetes_readmission`, `drug_reviews`, `clinical_notes`, `life_expectancy`)
- [ ] **Install Agent Skills** — run the ai-dev-kit install script on each workspace (see `warmup_vibe_coding/agent_skills_guide.py`, Option B)
- [ ] Prepare lightning talk demos (organizer workspace)
- [ ] Set up `scoring/scoreboard.py` on the organizer workspace
- [ ] Prepare Genie benchmark question pool for Event 2
- [ ] Prepare agent evaluation prompts for Event 4
- [ ] Assign judges for Events 4 and 5 (and Best Vibe Coder)
- [ ] Prepare timers and awards (4 Gold trophies + Capstone Champion + Best Vibe Coder)

### Tips
- Have a "help desk" for environment issues
- Project the scoreboard on a big screen
- **Safety net**: After each event, clone demo solution data to teams that didn't complete, so everyone starts the next event on equal footing
- The lightning talks are crucial — they show teams exactly what they need to build
- Watch for teams that are using the Assistant creatively for the **Best Vibe Coder** award

---

## Compatibility

**Designed for Databricks Free Edition.** All events work without paid features. Where paid features would be ideal (Vector Search, Model Serving), we provide open-source alternatives (ChromaDB, batch inference).

---

## License

This project uses synthetic and open-source datasets for educational and demonstration purposes only. All datasets retain their original licenses.

---

**Happy competing! May the best data team win!**
