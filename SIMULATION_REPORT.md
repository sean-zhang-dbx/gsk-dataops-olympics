# GSK DataOps Olympics — End-to-End Simulation Report

**Date:** 2026-03-12
**Teams simulated:** 5 (team_01 through team_05)
**team_05 = max score team** (designed to achieve highest possible in every event)

## Final Leaderboard (Events 1-4, Event 5 scoring failed)

| Rank | Team | E1 | E2 | E3 | E4 | E5 | TOTAL |
|------|------|-----|-----|-----|-----|-----|-------|
| 1 GOLD | team_05 | 33 | 54 | 35 | 18 | 0* | **140** |
| 2 SILVER | team_01 | 31 | 38 | 24 | 6 | 0* | **99** |
| 3 BRONZE | team_02 | 29 | 34 | 22 | 6 | 0* | **91** |
| 4 | team_03 | 25 | 22 | 5 | 6 | 0* | **58** |
| 5 | team_04 | 12 | 14 | 11 | 6 | 0* | **43** |

*Event 5 scoring notebook has a bug (see below)

## What Worked

1. **Notebook execution via Jobs API** — all team notebooks ran successfully on the instructor cluster
2. **`%run ../_config` and `%run ../_submit`** — worked perfectly across all events
3. **Submission mechanism** — `submit()` function correctly inserted into `event_submissions` table with timestamps and user
4. **Step 0 data preparation** — `heart_silver_correct` and `heart_gold_correct` created correctly from shared data
5. **Event 1 scoring** — correctly detected Bronze/Silver/Gold tables, row counts, dedup, comments, SDP properties, bonuses
6. **Event 2 scoring** — LLM judge (`ai_query`) worked for answer checking, fuzzy matching caught numeric answers, bonus tables detected
7. **Event 3 scoring** — MLflow experiments, runs, metrics, model registration all detected correctly
8. **Event 4 scoring** — Genie spaces detected via API, `agent_config` table parsed correctly, DSPy bonus detected
9. **Multiple submissions** — team_02 submitted Event 1 twice (mistake then fix), scoring correctly took latest answer
10. **Parallel execution** — 5 team notebooks ran concurrently on same cluster without conflicts

## Bugs Found (MUST FIX)

### BUG 1: Event 5 scoring — ValueError unpacking
- **File:** `event5_scoring.py`
- **Error:** `ValueError: not enough values to unpack (expected 3, got 2)`
- **Impact:** Event 5 scores are 0 for all teams
- **Action:** Need to debug the scoring function — likely a tuple return mismatch

### BUG 2: Event 2/5 scoring — SDK auth failure (FIXED)
- **File:** `event2_scoring.py`, `event5_scoring.py`
- **Error:** `WorkspaceClient()` throws `ValueError` not `ImportError`
- **Fix applied:** Changed `except ImportError` to `except Exception`
- **Status:** Fixed in event2, fixed in event5 but scoring still fails due to BUG 1

### BUG 3: Event 4 — UC Functions not detected
- **File:** `event4_scoring.py`
- **Issue:** `SHOW USER FUNCTIONS IN {catalog}.default` returns 0 results even though functions exist
- **Impact:** All teams scored 0 for UC Functions (should be 5-10 pts each)
- **Likely cause:** The `SHOW USER FUNCTIONS` syntax may need `SHOW FUNCTIONS` or the function names don't match the pattern checks
- **Action:** Debug on the actual cluster — run `SHOW FUNCTIONS IN team_01.default` manually

### BUG 4: `_submit.py` was mismatched with table schema (FIXED)
- **Original:** Inserted 4 values into 6-column table, wrong column names in `check_submission`
- **Fix applied:** Updated to insert all 6 columns, fixed column references
- **Status:** Fixed and deployed

## Scoring Threshold Issues (FIXED)

### Silver row count threshold
- **Original:** 480-495 (expected silver)
- **Actual data:** 491 silver rows (510 raw - 14 dirty - 5 dupes)
- **Fix applied:** Changed to 485-500

### Bronze row count threshold
- **Original:** >=495 for full marks
- **Actual data:** 510 total raw rows
- **Fix applied:** Changed to >=505

## Data Issues Found

### Duplicate rows in NDJSON
- **Original:** 5 duplicate rows were copies of dirty rows, so after filtering they were removed — dedup test was meaningless
- **Fix applied:** Added 5 duplicate rows that are copies of CLEAN rows
- **Now:** team_03 (no dedup) has 496 silver vs team_01 (deduped) has 491 — scoring correctly distinguishes

## Suggestions for Improvement

### High Priority
1. **Fix Event 5 scoring bug** before the event
2. **Fix UC function detection** in Event 4 scoring
3. **Only score teams that submitted** — scoring currently runs on all 15 teams and shows 0 for non-participants, cluttering the leaderboard

### Medium Priority
4. **Genie space table binding** — can't be set via API, only UI. Document this for teams clearly
5. **Model registration** — team_05 tried to register but got 2/5 instead of 5/5. May need UC model registry permissions granted in setup
6. **SDK auth on single-user clusters** — `WorkspaceClient()` fails with ValueError. Need to pass credentials explicitly or use `spark.conf` for auth
7. **Event 2 speed bonus** — currently manual (`SPEED_WINNERS` dict). Consider auto-detecting based on `event2_submissions` timestamps

### Low Priority
8. **Event 3 Feature Engineering scoring** — most teams got 1/8 for "base only" features. Consider making the threshold more generous (13 features = 3pts, >15 = 5pts)
9. **Knowledge Assistant / Supervisor scoring** — scored from `agent_config` table entries without verifying the resources actually exist. Could add an API health check
10. **Presentation scoring** — currently fully manual (PRESENTATION_SCORES dict). Fine for a live event

## Files Changed During Simulation

| File | Change |
|------|--------|
| `_submit.py` | Fixed column mismatch with event_submissions table |
| `event1_scoring.py` | Fixed silver/bronze row count thresholds |
| `event2_scoring.py` | Fixed `except ImportError` -> `except Exception` |
| `event5_scoring.py` | Fixed `except ImportError` -> `except Exception` |
| `data/heart_events/intake_batch_003.json` | Added 3 clean-row duplicates |
| `data/heart_events/intake_batch_005.json` | Added 2 clean-row duplicates |

## Workspace State After Simulation

- **Clusters:** 2 running (instructor + test user)
- **Catalogs:** dataops_olympics + team_01 through team_05
- **Tables per team:** heart_bronze, heart_silver, heart_gold, heart_silver_correct, heart_gold_correct, event2_submissions, event3_results, agent_config, plus bonus tables
- **Genie spaces:** 5 created (no table bindings)
- **MLflow experiments:** 5 (one per team, under instructor's user path)
- **Submissions:** 13 rows in event_submissions
- **Leaderboard:** populated for Events 1-4
