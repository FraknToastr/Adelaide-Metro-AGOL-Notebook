# Fair A/B Testing Tips for ChatGPT Prompt Evaluation

Use this checklist to compare multiple prompts **fairly** when asking ChatGPT to produce an ArcGIS Online Python notebook cell.

---

## 1) Fresh, Isolated Runs
- Start a **new chat** for each prompt (no prior context).
- Paste **only the prompt**—no extra commentary.
- Ask for **one notebook cell** targeting **ArcGIS Online** explicitly.

## 2) Fixed Success Criteria (pass/fail)
Evaluate each run against the same criteria:
1. Logs in with `GIS("home")` (no interactive prompts).
2. Fetches GTFS-RT from `https://gtfs.adelaidemetro.com.au/v1/realtime/vehicle_positions`.
3. Filters to Adelaide bounds (lon `[137.5, 140.5]`, lat `[-36.5, -33.5]`).
4. Deduplicates by `VehicleID`, keeping **latest PositionTimestamp**.
5. Sets `VehicleType = "Train"` if `RouteID` is alphabetic, else `"Bus"`.
6. Creates/updates **Adelaide_Metro_Vehicles** (WKID 4326, required fields) and **replaces all features** on update.
7. Sets `LastUpdated` to **UTC now**.
8. Prints counts: fetched, filtered, deduped, written.
9. Uses only `arcgis`, `gtfs-realtime-bindings`, `urllib3`, `certifi`.
10. Runs in a **single cell** with clear, non-interactive errors (raise on failure).

> Tip: Mark each criterion as ✅/❌ and add notes.

## 3) Scoring Rubric (0–3 each)
- **Correctness:** Meets the criteria above.
- **Robustness:** Minimal but sensible retry, input checks, and clear failures.
- **Clarity:** Readable code, obvious steps, concise prints.
- **Idempotence:** Safe to re-run (update if exists, create if not).
- **Latency:** Produces the result without unnecessary steps/imports.

_Total score per prompt: 0–15._

## 4) Data Capture Template
Use this table per run to keep evidence comparable:

| Prompt Variant | Pass/Fail (1–10) | Score (0–15) | Layer Action (Created/Updated) | Feature Count | Runtime Notes / Errors |
|---|---:|---:|---|---:|---|
| Contract (full) |  |  |  |  |  |
| One-shot |  |  |  |  |  |
| Ultra-minimal |  |  |  |  |  |
| Minified (1‑paragraph) |  |  |  |  |  |

## 5) Repro Steps (keep identical)
- Same AGOL account and environment.
- Same network conditions (if possible).
- Same order of testing (or **randomize** order to avoid bias).
- No manual edits between runs; re-run exactly as generated.

## 6) What to Bring Back
- The **exact cell** ChatGPT produced.
- Console output (success or traceback).
- Whether the layer was **created** or **updated**.
- Final feature **counts** and any mismatched schema messages.

## 7) Tie-Breakers
- Prefer the prompt that yields **shorter code** with equal correctness.
- Prefer **clearer prints** and obvious failure points over silent errors.
- Prefer code that **does not mutate schema** to force a fix when mismatched.

---

### Quick Verdict Labels
- **Production-ready:** Meets all criteria, clean output, safe re-runs.
- **Usable with tweaks:** Minor issues, but clear how to fix.
- **Missed the mark:** Fails key criteria or requires multiple edits.

Good luck—this will make your feedback crisp and actionable.
