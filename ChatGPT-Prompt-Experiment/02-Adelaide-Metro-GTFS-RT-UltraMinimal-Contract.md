# Ultra‑Minimal Contract Prompt — Adelaide Metro GTFS‑RT → ArcGIS Online

One AGOL notebook cell that:
- `GIS("home")`; raise on auth error.
- Fetch GTFS‑RT vehicle positions from `https://gtfs.adelaidemetro.com.au/v1/realtime/vehicle_positions`; simple retry (≤3); certifi SSL.
- Keep points within lon [137.5,140.5], lat [-36.5,-33.5]; dedupe latest per `VehicleID`.
- `VehicleType="Train"` if `RouteID` is alphabetic, else `"Bus"`.
- Create or update a point Feature Service titled **Adelaide_Metro_Vehicles** (WKID 4326; extent 137.5,-36.5,140.5,-33.5) with fields:

`VehicleID(str50), VehicleLabel(str100), LicensePlate(str50), TripID(str50), RouteID(str50), DirectionID(int), StartTime(str20), StartDate(str20), Bearing(float), Speed(float), CurrentStopID(str50), PositionTimestamp(date), LastUpdated(date), VehicleType(str20)`

- On update: **replace all features**; set `LastUpdated=UTC now`; print final counts.
- Use only `arcgis`, `gtfs-realtime-bindings`, `urllib3`, `certifi`.
