# One‑Shot Contract Prompt — Adelaide Metro GTFS‑RT → ArcGIS Online

Create a single **ArcGIS Online Python notebook cell** that:

- Logs in with `GIS("home")`; raise if auth fails.
- Fetches **GTFS‑Realtime vehicle positions** from `https://gtfs.adelaidemetro.com.au/v1/realtime/vehicle_positions` using `gtfs-realtime-bindings`, `urllib3` with certifi-backed SSL, and a simple retry.
- Filters records to **Adelaide bounds**: lon ∈ [137.5, 140.5], lat ∈ [-36.5, -33.5].
- **Deduplicates** by `VehicleID`, keeping the newest **PositionTimestamp**.
- Sets `VehicleType = "Train"` if `RouteID` is alphabetic only, else `"Bus"`.
- **Creates or updates** a Feature Service titled **`Adelaide_Metro_Vehicles`** (one point layer, WKID 4326, initial extent xmin=137.5, ymin=-36.5, xmax=140.5, ymax=-33.5). If a service with that title exists, **replace all features**; otherwise **create** it with the exact schema below.
- Uses exactly these fields (name:type[length]):  
  `VehicleID:str[50], VehicleLabel:str[100], LicensePlate:str[50], TripID:str[50], RouteID:str[50], DirectionID:int, StartTime:str[20], StartDate:str[20], Bearing:float, Speed:float, CurrentStopID:str[50], PositionTimestamp:date, LastUpdated:date, VehicleType:str[20]`.
- Writes geometry (lon/lat) and attributes for each record; sets `LastUpdated` to **UTC now**.
- Prints counts: fetched, filtered, deduped, written; **raise on failure**.

**Packages required in the notebook environment:** `arcgis`, `gtfs-realtime-bindings`, `urllib3`, `certifi`.  
**Deliverable:** one runnable cell (no argparse/CLI, no cleanup/README extras).
