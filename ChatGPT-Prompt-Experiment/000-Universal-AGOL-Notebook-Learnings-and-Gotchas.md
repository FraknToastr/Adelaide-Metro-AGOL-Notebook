# Universal AGOL Notebook Learnings (From Real-World Scripts)

These notes capture robust patterns that make **ArcGIS Online (AGOL) Python notebooks** reliable, idempotent, and production-friendly—applicable to most data-ingest/update tasks. They’re distilled from working scripts (e.g., GTFS-RT → Feature Service) and hardened by field use.

---

## 1) Login, Identity, and Safety
- **Prefer `GIS("home")`** in AGOL notebooks; fail fast if it returns `None` or throws.
- If you must support explicit login, **never** hardcode credentials; read env vars or use a secure secret store.
- Log only **which org** you’re connected to—not tokens, usernames, or URLs unless needed for diagnostics.
- Validate that your **active user owns** or has **edit rights** on the target item before attempting edits.

## 2) Idempotent “Update-or-Create” Pattern
- Search **owned content** for a **title-exact match** for your service.
- If it exists, **update features in place** (or replace); if not, **create** the service with the correct schema.
- **Never mutate schema** dynamically during updates; if a mismatch is detected, **raise a clear error** that instructs to recreate.
- When you must version outputs, use a suffix like `_YYYYMMDD_HHMMSS` and keep only the **N newest** (policy-driven cleanup).

## 3) Schema Discipline
- Specify **field names, types, and lengths** explicitly—no surprises.
- Use **WGS84 (WKID 4326)** for global point feeds unless you have a strong reason not to.
- Set an **initial extent** to the plausible area of interest; it improves map UX and some server-side behaviors.
- For dates, write **epoch milliseconds** or `datetime` objects; set `timezone=UTC` in your logic and **normalize** all times.

## 4) Feature Editing & Batching
- Prefer **replace-all** for small/medium layers (simple and reliable). For very large layers, do **diff-upserts** by stable keys.
- Batch `add_features`/`edit_features` to avoid payload limits (e.g., **200–500 features per batch**).
- Validate geometry: **drop null/NaN** coords early; coerce to floats; ensure SR = 4326.
- Keep attribute values simple (strings, numbers, dates). Avoid oversized blobs; respect field lengths.

## 5) Data Hygiene & Rules
- Apply **bounds filters** to eliminate junk records.
- **Deduplicate** on a stable key (e.g., `VehicleID`) and keep the **newest timestamp**.
- Enforce lightweight business rules (e.g., derive `VehicleType`) **before** writing to AGOL.
- Fail early with **actionable messages** if the input feed is empty or malformed.

## 6) Networking & Retries
- Use a **certifi-backed SSL context** and a **User-Agent** string.
- Implement **exponential backoff** for transient HTTP/IO errors; cap retries (3–5) with jitter.
- Treat **HTTP 4xx** (except 429) as non-retriable unless the API is known to be flaky; **retry 429** with server-provided hints when possible.
- Log the **final URL** (not tokens), HTTP code, and try-count on failure.

## 7) Logging That Matters
- Print concise milestones: *fetch → parse → filter → dedupe → write (counts)*.
- On exceptions: emit a **one-paragraph summary** + **single traceback** block.
- Include the **item id** and **layer id** you wrote to—vital for post-run verification.

## 8) Sharing, Ownership, and Governance
- After creation, set **proper sharing** (private, group, org, public) **explicitly**.
- Record **itemId**, **service URL**, and **layer URL** in a small audit log (even a markdown cell) for later runs.
- Avoid altering **capabilities** and **indexes** on every run; set them once during creation.

## 9) Reproducibility
- Pin key package versions (e.g., `arcgis` major/minor) when stability is critical.
- Keep a small **README/Usage** note with: schema, target item, and what “success” prints look like.
- Capture **UTC timestamps** for each phase to aid incident review.

## 10) Notebook Ergonomics
- Prefer a **single cell** or a **small set** of top-level functions + `main()`—easy to re-run.
- Avoid `sys.exit` in notebooks; **raise** exceptions instead.
- Where helpful, provide a **dry-run** flag that parses + validates but **skips writes**.

---

## Universal Code Skeleton (Pseudocode)

```python
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from datetime import datetime, timezone

def main():
    gis = GIS("home")
    assert gis.users.me is not None, "Auth failed"

    # 1) Fetch
    raw = fetch_feed_with_retry(url)

    # 2) Parse + Clean
    recs = parse(raw)
    recs = bounds_filter(recs, bbox)
    recs = dedupe_latest(recs, key="VehicleID", ts="PositionTimestamp")
    recs = derive_fields(recs)

    # 3) Discover target
    item = find_or_create_service(gis, title, schema, extent, wkid=4326)

    # 4) Write
    replace_all_features(item.layers[0], recs)

    # 5) Report
    print_counts(...)
```

---

## Operational Limits You Should Remember
- **Feature Service add/edit size**: keep batches modest; large payloads time out.
- **Max record count** affects query pagination; use `return_all_records` or page results.
- **Rate limits** may apply—stagger multi-run workflows and honor backoff.
- **Field length** overruns silently truncate in some paths; validate lengths in code.

---

## Troubleshooting Cues
- If your writes “succeed” but you see no features: check **sharing**, **layer id (0)**, **spatial reference**, and **NaN geometries**.
- If updates throw schema errors: verify **field names/types/lengths** exactly; AGOL is strict.
- If search returns the wrong item: filter by **owner** and **type** (Feature Service vs Feature Layer).

---

# ChatGPT’s Known Gotchas (ArcGIS Pro 3.5 + AGOL)

These are issues that frequently trip up otherwise-solid scripts and tools around Pro 3.5 and AGOL.

### ArcGIS Pro 3.5 / ArcPy
- **Parameter insert positions**: values like `"BOTTOM"` are invalid for some APIs—supported values are commonly **`"AFTER"`/`"BEFORE"`** for insert operations. Check the specific GP tool docs.
- **Parameter `DataType` mismatches**: `arcpy.Parameter(datatype=...)` must use **valid, documented** datatypes; free-form strings cause `ValueError: Invalid input value for DataType`.
- **Toolbox discovery**: a `.pyt` can load but show **no tools** if `getParameterInfo()` raises or class names don’t match the tool label; ensure `Tool.dialogID`, `label`, and `description` are defined and classes are imported at module scope.
- **HTTP 499 / image services**: server-side disconnects may appear as 499s—common with ImageServer or long requests. Use shorter extents/tiles, verify credentials, and avoid blocked hosts. Consider testing with a small clip first.
- **Environment versions**: Pro 3.5 typically runs **Python 3.9**; pin libraries accordingly. Avoid installing packages that conflict with Esri’s stack (e.g., `shapely`, `geopandas`) without a plan.
- **Feature locks**: running tools against layers **open in the map** can lock data; prefer writing to new outputs or close map references before write.
- **Projection pitfalls (GDA2020)**: ensure correct EPSG/WKID and axis order; when mixing Web Mercator and GDA2020, **project** explicitly rather than assuming automatic reprojection.
- **Overwrite behavior**: set `arcpy.env.overwriteOutput = True` consciously; understand that some tools still refuse to overwrite in-place datasets.
- **Symbology automation**: programmatic renderer changes can fail silently if class breaks or fields are missing—always verify field existence before applying renderers.

### AGOL / ArcGIS API for Python
- **Feature Layer vs Feature Service**: searching may return a **layer item**; you often need the **parent service** to manage definitions or recreate schema.
- **Layer index assumptions**: most single-layer services expose **layer 0**; multi-layer items need explicit index selection.
- **Date fields**: ensure you send **UTC** in epoch ms or Python `datetime`; avoid mixing local timezones.
- **NaN/None geometries**: any NaN in lon/lat will **drop** features; pre-validate coordinates.
- **Edit payload size**: large `add_features` calls can fail mid-batch. **Chunk** and check return statuses.
- **Silent truncation**: strings that exceed length may be truncated; enforce max lengths before writing.
- **Schema mutation**: adding/removing fields via code is risky on shared prod layers—prefer recreate + swap if schema has to change.
- **Sharing drift**: new services default to **private**; set sharing intentionally every time.

---

## Minimal Preflight Checklist (Apply to Any AGOL Notebook)
- ✅ `GIS("home")` works and `users.me` is not `None`.
- ✅ Target item found or created with **exact schema** and **WKID 4326**.
- ✅ Input filtered & deduped; **no NaN** geometries; lengths within limits.
- ✅ Batched writes with success counts checked.
- ✅ Final printout: **fetched**, **filtered**, **deduped**, **written**, **itemId/layerId**.
- ✅ Exceptions produce a **clear summary + traceback**.

---

*Use this as a drop-in preface or a pinned README in your AGOL notebooks so future runs stay reliable and predictable.*
