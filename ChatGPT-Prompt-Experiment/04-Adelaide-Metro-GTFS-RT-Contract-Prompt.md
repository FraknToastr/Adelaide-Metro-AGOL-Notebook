# Contract Prompt — Adelaide Metro GTFS-RT → ArcGIS Online Vehicle Positions

## Role
You are an expert Python + ArcGIS API engineer. Build a single, Jupyter-safe script (runnable in a notebook cell or as `python script.py`) that fetches **Adelaide Metro GTFS-Realtime vehicle positions** and publishes them to **ArcGIS Online** (AGOL) as a point Feature Service/Feature Layer, updating if it exists, or creating it with the exact schema if not.

## Invariants (do not change)
- **Feed URL:** `https://gtfs.adelaidemetro.com.au/v1/realtime/vehicle_positions`
- **Default service base name:** `Adelaide_Metro_Vehicles`
- **Geometry:** `esriGeometryPoint`, WKID **4326** (WGS84)
- **ObjectID field:** `OBJECTID`
- **Initial layer extent (WGS84):** `xmin=137.5, ymin=-36.5, xmax=140.5, ymax=-33.5`
- **Fields (names, types, lengths):**  
  - `VehicleID` *(String, 50)*  
  - `VehicleLabel` *(String, 100)*  
  - `LicensePlate` *(String, 50)*  
  - `TripID` *(String, 50)*  
  - `RouteID` *(String, 50)*  
  - `DirectionID` *(Integer)*  
  - `StartTime` *(String, 20)*  
  - `StartDate` *(String, 20)*  
  - `Bearing` *(Double)*  
  - `Speed` *(Double)*  
  - `CurrentStopID` *(String, 50)*  
  - `PositionTimestamp` *(Date)*  
  - `LastUpdated` *(Date)*  
  - `VehicleType` *(String, 20)*
- **Vehicle type rule:** if `RouteID` is **alphabetic only** → `"Train"`, else `"Bus"`.
- **Adelaide bounds filter (keep only points plausibly in/near Adelaide):** latitude ∈ \[-36.5, −33.5], longitude ∈ \[137.5, 140.5].
- **Dedup rule:** keep the **latest** position per `VehicleID` by position timestamp.
- **Auth:** use `GIS("home")` first; allow fallback to `GIS(url, username, password)` when provided.
- **Notebook-safe:** never hard-exit a Jupyter kernel; raise exceptions or return cleanly.
- **Batch adds/updates:** chunk features to avoid payload limits.
- **Idempotent update:** if a matching owned Feature Service/Layer titled `Adelaide_Metro_Vehicles` exists, **update it**; else **create** a new **timestamped** service `Adelaide_Metro_Vehicles_YYYYMMDD_HHMMSS` and then apply **cleanup** rules (below).
- **Cleanup policy:** keep at most **N** newest timestamped services with this base name; delete older ones. Default **N=3**.
- **README generation:** after a successful run, (re)write an item/page README that documents requirements, CLI usage, schema, troubleshooting.

## Requirements
- Python 3.9+  
- Packages: `arcgis`, `gtfs-realtime-bindings`, `urllib3`, `certifi`
- Robust network layer: set a modern User-Agent, use **certifi** SSL context, implement **retry with exponential backoff** for the GTFS request.

## CLI / Parameters
Provide argparse flags that also work as function parameters when called in a notebook:
- `--name` *(default: Adelaide_Metro_Vehicles)* — base service name.
- `--keep-n` *(default: 3)* — how many timestamped services to retain.
- `--no-cleanup` — skip deletion of old timestamped services.
- `--portal`, `--username`, `--password` — optional explicit AGOL login.
- `--dry-run` — fetch/parse/validate and print counts, but do not write to AGOL.

## Exact Behaviors to Implement

### 1) Fetch & Parse GTFS-RT
- Request `vehicle_positions` with header `User-Agent: ArcGIS Online Notebook GTFS-RT Client`.
- Use certifi-backed SSL context.
- Retries: e.g., 5 attempts, backoff (1s, 2s, 4s, 8s, 16s), only on transient errors.
- Parse with `gtfs_realtime_pb2.FeedMessage`. Extract for each vehicle entity:
  - `VehicleID` (vehicle.id or similar), `VehicleLabel`, `LicensePlate`
  - `TripID`, `RouteID`, `DirectionID`, `StartTime`, `StartDate`
  - `Bearing`, `Speed` (if present), `CurrentStopID`
  - `PositionTimestamp` (prefer entity/position timestamp; else feed header)
  - `VehicleType` (per invariant rule)
  - `geometry` (lon/lat; ensure in WGS84)
- Record `feed_header_ts` if present and use as fallback timestamp.

### 2) Validate & Filter
- Drop records missing lat/lon or outside Adelaide bounds.
- Coerce/clean types.
- Deduplicate by `VehicleID`, keeping the newest `PositionTimestamp`.

### 3) AGOL Discovery
- Search **owned** content for exact title `--name` across types `Feature Service` and `Feature Layer`.
- Prefer a **Feature Service** item if both exist; otherwise, take the Feature Layer’s parent service.

### 4) Create Service (when missing)
- Create a **Feature Service** named `"{name}_{YYYYMMDD_HHMMSS}"` with one **editable** point layer:
  - Geometry, objectId, spatial reference, extent per invariants.
  - Fields **exactly** as listed (names/types/lengths).
- Return both the service item and the first layer object.

### 5) Update Existing Layer
- Obtain an **editable** layer reference (if item is a FL, resolve parent service).
- Use chunked `add_features` (or `edit_features`) to upsert vehicle points:
  - Geometry from lon/lat.
  - Attributes populated for all invariant fields.
  - Set `LastUpdated` to now (UTC).
- If schema mismatch is detected, **do not mutate schema**; raise a clear error instructing the user to delete/recreate.

### 6) Cleanup Old Timestamped Services
- Identify items with title prefix `"{name}_"` that match pattern `_{YYYYMMDD_HHMMSS}`.
- Keep newest **N** (default 3). Delete older ones.  
- Respect `--no-cleanup` to skip this step.

### 7) README / Item Description
- Write or update a README/description on the updated/created item with:
  - Purpose and data flow
  - Requirements & installation
  - CLI examples:
    - `python script.py`
    - `python script.py --keep-n 5`
    - `python script.py --no-cleanup`
  - Output & schema listing
  - Troubleshooting (auth, empty feed, bounds filter, schema mismatch)

## Error Handling & Logging
- Clear, human-readable `print()` / logger messages for each phase:
  - Fetching, parsing counts, filtered counts, dedup counts
  - Found/created service details
  - Batch sizes and success/failure counts
  - Cleanup decisions
- On exceptions, print a concise summary plus a single traceback block.

## Testing Hooks
- Provide a top-level `main()` and a pure function for each step:
  - `fetch_and_parse_gtfs_data(url) -> (records, feed_header_ts)`
  - `validate_and_filter_positions(records) -> records`
  - `search_owned_exact(gis, title, types)`
  - `prefer_feature_service(items) -> item`
  - `create_feature_layer_with_schema(gis, base_name) -> (service_item, layer)`
  - `update_existing_layer(item_or_service, records) -> bool`
  - `cleanup_old_services(gis, base_name, keep_item_id, keep_n) -> None`
  - `write_readme(item, keep_n) -> None`
- Ensure `if __name__ == "__main__": main()` guard.
- In notebooks, allow calling `main(args_obj)` without exiting kernel.

## Deliverables
- One **self-contained Python file** (or one notebook cell) implementing everything above.
- Clean, readable code; no TODOs; no placeholders.
- No changes to field names, types, lengths, geometry, or bounds.
