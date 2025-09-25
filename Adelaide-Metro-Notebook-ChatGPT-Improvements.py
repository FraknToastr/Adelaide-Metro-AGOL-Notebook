#!/usr/bin/env python3
"""
Adelaide Metro GTFS-RT Vehicle Position Fetcher
Version 6.6 - Fixed schema, robust retries, configurable cleanup

Author: Converted for ArcGIS Online Integration (refined)

------------------------------------------------------------
INSTRUCTIONS
------------------------------------------------------------

1. REQUIREMENTS:
   - ArcGIS API for Python (comes with ArcGIS Pro, or install with pip):
       pip install arcgis
   - GTFS bindings:
       pip install gtfs-realtime-bindings

2. AUTHENTICATION:
   - Run this script inside ArcGIS Online Notebooks OR
   - On your machine, log in once with:
       from arcgis.gis import GIS
       GIS("home")    # will prompt browser login

3. CONFIGURATION:
   - FEATURE_LAYER_NAME: base name of your service
   - KEEP_N: number of timestamped layers to retain (default 3)
   - GTFS_URL: Adelaide Metro real-time vehicle positions feed

4. RUN:
   - Default (keep last 3 layers):
       python adl_metro_gtfs.py
   - Keep 5 timestamped layers:
       python adl_metro_gtfs.py --keep-n 5
   - Skip cleanup entirely:
       python adl_metro_gtfs.py --no-cleanup

5. OUTPUT:
   - Updates existing service if found
   - Otherwise creates new timestamped service
   - Cleanup removes older timestamped services beyond KEEP_N
   - Base layer (no timestamp) and protected services are preserved
------------------------------------------------------------
"""

import urllib.request
import certifi
import ssl
import sys
import datetime as dt
import time
import traceback
import argparse
from typing import List, Dict, Optional

from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Dependencies ---
try:
    from google.transit import gtfs_realtime_pb2
    print("✓ Using gtfs-realtime-bindings for parsing")
except ImportError:
    print("Installing gtfs-realtime-bindings...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "gtfs-realtime-bindings"])
    from google.transit import gtfs_realtime_pb2
    print("✓ gtfs-realtime-bindings installed and imported")

# --- Configuration ---
FEATURE_LAYER_NAME = "Adelaide_Metro_Vehicles"
GTFS_URL = "https://gtfs.adelaidemetro.com.au/v1/realtime/vehicle_positions"
MAX_ADD_PER_REQUEST = 950
KEEP_N = 3   # default number of timestamped services to keep


# === Helpers ===
def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def to_epoch_ms(d: Optional[dt.datetime]) -> Optional[int]:
    if d is None:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return int(d.timestamp() * 1000)

def chunk(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


# === GTFS Parsing ===
def parse_with_bindings(feed_data: bytes) -> List[Dict]:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(feed_data)
    vehicles = []
    feed_header_ts = None
    if feed.header.HasField("timestamp"):
        feed_header_ts = dt.datetime.fromtimestamp(feed.header.timestamp, tz=dt.timezone.utc)

    for entity in feed.entity:
        if entity.HasField("vehicle"):
            v = {}
            vp = entity.vehicle

            if vp.HasField("vehicle"):
                v["vehicle_id"] = vp.vehicle.id or None
                v["vehicle_label"] = vp.vehicle.label or None
                v["license_plate"] = vp.vehicle.license_plate or None

            if vp.HasField("trip"):
                td = vp.trip
                v["trip_id"] = td.trip_id or None
                v["route_id"] = td.route_id or None
                try:
                    v["direction_id"] = td.direction_id if td.HasField("direction_id") else None
                except Exception:
                    v["direction_id"] = getattr(td, "direction_id", None)
                v["start_time"] = td.start_time or None
                v["start_date"] = td.start_date or None

            if vp.HasField("position"):
                pos = vp.position
                v["latitude"] = getattr(pos, "latitude", None)
                v["longitude"] = getattr(pos, "longitude", None)
                v["bearing"] = getattr(pos, "bearing", None)
                v["speed"] = getattr(pos, "speed", None)

            v["current_stop_id"] = vp.stop_id or None
            v["position_timestamp"] = dt.datetime.fromtimestamp(vp.timestamp, tz=dt.timezone.utc) if vp.HasField("timestamp") else feed_header_ts
            v["last_updated"] = utc_now()
            vehicles.append(v)
    return vehicles


# === Fetch with retries ===
def fetch_and_parse_gtfs_data(gtfs_url: str, max_retries: int = 4) -> List[Dict]:
    print(f"Fetching data from: {gtfs_url}")
    context = ssl.create_default_context(cafile=certifi.where())
    request = urllib.request.Request(gtfs_url, headers={"User-Agent": "ArcGIS Online Notebook GTFS-RT Client"})
    attempt = 0
    while True:
        try:
            with urllib.request.urlopen(request, context=context, timeout=30) as response:
                data = response.read()
            print("Data fetched successfully.")
            return parse_with_bindings(data)
        except Exception as e:
            attempt += 1
            print(f"Fetch error ({attempt}): {e}")
            if attempt >= max_retries:
                print(traceback.format_exc())
                return []
            time.sleep(1.5 * (2 ** (attempt - 1)))


# === Validation ===
def validate_and_filter_positions(vehicles: List[Dict]) -> List[Dict]:
    valid = []
    for v in vehicles:
        lat, lon = v.get("latitude"), v.get("longitude")
        if lat and lon and lat != 0 and lon != 0:
            if -36.5 <= lat <= -33.5 and 137.5 <= lon <= 140.5:
                valid.append(v)
    print(f"Valid vehicles in bounds: {len(valid)}")
    return valid

def classify_vehicle_type(route_id: Optional[str]) -> str:
    if not route_id:
        return "Unknown"
    rid = route_id.upper().strip()
    if rid in {"GLNELG", "BTANIC"}:
        return "Tram"
    if rid.isalpha():
        return "Train"
    return "Bus"


# === AGOL Helpers ===
def search_owned_exact(gis: GIS, title: str, types: Optional[List[str]] = None):
    owner = gis.users.me.username
    type_clause = "" if not types else " AND (" + " OR ".join([f'type:\"{t}\"' for t in types]) + ")"
    q = f'title:\"{title}\" AND owner:\"{owner}\"{type_clause}'
    return gis.content.search(q, max_items=50)

def prefer_feature_service(items):
    fs = [i for i in items if i.type == "Feature Service"]
    if fs: return fs[0]
    fl = [i for i in items if i.type == "Feature Layer"]
    if fl: return fl[0]
    return items[0] if items else None

def get_editable_layer(item):
    try: return item.layers[0]
    except Exception: return None


def update_existing_layer(layer_item, vehicles: List[Dict]) -> bool:
    try:
        fl = get_editable_layer(layer_item)
        if not fl: return False

        fl_fields = {f["name"] for f in fl.properties.fields}
        features = []
        for v in vehicles:
            if v.get("latitude") and v.get("longitude"):
                attrs = {
                    "VehicleID": v.get("vehicle_id"),
                    "VehicleLabel": v.get("vehicle_label"),
                    "LicensePlate": v.get("license_plate"),
                    "TripID": v.get("trip_id"),
                    "RouteID": v.get("route_id"),
                    "DirectionID": v.get("direction_id"),
                    "StartTime": v.get("start_time"),
                    "StartDate": v.get("start_date"),
                    "Bearing": v.get("bearing"),
                    "Speed": v.get("speed"),
                    "CurrentStopID": v.get("current_stop_id"),
                    "PositionTimestamp": to_epoch_ms(v.get("position_timestamp")),
                    "LastUpdated": to_epoch_ms(v.get("last_updated")),
                    "VehicleType": classify_vehicle_type(v.get("route_id")),
                }
                attrs = {k: val for k, val in attrs.items() if k in fl_fields}
                features.append({"geometry": {"x": v["longitude"], "y": v["latitude"], "spatialReference": {"wkid": 4326}}, "attributes": attrs})

        try: fl.manager.truncate()
        except Exception: fl.delete_features(where="1=1")

        for batch in chunk(features, MAX_ADD_PER_REQUEST):
            fl.edit_features(adds=batch)
        print(f"✓ Updated {len(features)} features.")
        return True
    except Exception as e:
        print(f"Update error: {e}")
        print(traceback.format_exc())
        return False


def create_feature_layer_with_schema(gis: GIS, base_name: str):
    ts = utc_now().strftime("%Y%m%d_%H%M%S")
    unique_name = f"{base_name}_{ts}"
    fields = [
        {"name": "VehicleID", "type": "esriFieldTypeString", "alias": "Vehicle ID", "length": 50},
        {"name": "VehicleLabel", "type": "esriFieldTypeString", "alias": "Vehicle Label", "length": 50},
        {"name": "LicensePlate", "type": "esriFieldTypeString", "alias": "License Plate", "length": 50},
        {"name": "TripID", "type": "esriFieldTypeString", "alias": "Trip ID", "length": 50},
        {"name": "RouteID", "type": "esriFieldTypeString", "alias": "Route ID", "length": 50},
        {"name": "DirectionID", "type": "esriFieldTypeInteger", "alias": "Direction ID"},
        {"name": "StartTime", "type": "esriFieldTypeString", "alias": "Start Time", "length": 20},
        {"name": "StartDate", "type": "esriFieldTypeString", "alias": "Start Date", "length": 20},
        {"name": "Bearing", "type": "esriFieldTypeDouble", "alias": "Bearing"},
        {"name": "Speed", "type": "esriFieldTypeDouble", "alias": "Speed"},
        {"name": "CurrentStopID", "type": "esriFieldTypeString", "alias": "Current Stop ID", "length": 50},
        {"name": "PositionTimestamp", "type": "esriFieldTypeDate", "alias": "Position Timestamp"},
        {"name": "LastUpdated", "type": "esriFieldTypeDate", "alias": "Last Updated"},
        {"name": "VehicleType", "type": "esriFieldTypeString", "alias": "Vehicle Type", "length": 20},
    ]
    layer_def = {"layers": [{"name": unique_name, "type": "Feature Layer", "geometryType": "esriGeometryPoint", "fields": fields, "objectIdField": "OBJECTID", "extent": {"xmin": 137.5, "ymin": -36.5, "xmax": 140.5, "ymax": -33.5, "spatialReference": {"wkid": 4326}}, "spatialReference": {"wkid": 4326}}]}
    props = {"title": unique_name, "type": "Feature Service", "description": "Adelaide Metro vehicles (schema defined)", "tags": "Adelaide Metro, GTFS-RT, Vehicles, Real-time"}
    service = gis.content.create_service(unique_name, props, service_type="featureService", create_params=layer_def)
    return service, FeatureLayerCollection.fromitem(service).layers[0]


def cleanup_old_services(gis: GIS, base_name: str, keep_item_id: str, keep_n: int = 3):
    items = search_owned_exact(gis, base_name, ["Feature Service", "Feature Layer"])
    base_items = [it for it in items if it.title == base_name]
    timestamped = [it for it in items if it.title.startswith(base_name + "_")]

    timestamped.sort(key=lambda it: it.modified, reverse=True)
    keep_ids = {it.id for it in timestamped[:keep_n]} | {keep_item_id}

    deleted = 0
    for it in timestamped[keep_n:]:
        if getattr(it, "protected", False) or it.id in keep_ids:
            continue
        try:
            it.delete()
            deleted += 1
            print(f"Deleted old timestamped service: {it.title}")
        except Exception as e:
            print(f"Could not delete {it.title}: {e}")

    if deleted:
        print(f"✓ Cleanup complete, deleted {deleted} old service(s).")
    else:
        print("No old timestamped services deleted.")

    if base_items:
        print(f"Base service(s) preserved: {[it.title for it in base_items]}")


# === Main ===
def main():
    parser = argparse.ArgumentParser(description="Adelaide Metro GTFS-RT fetcher")
    parser.add_argument("--no-cleanup", action="store_true", help="Skip deleting old timestamped services")
    parser.add_argument("--keep-n", type=int, default=KEEP_N, help=f"Number of timestamped services to keep (default {KEEP_N})")
    args = parser.parse_args()

    gis = GIS("home")
    print(f"Connected as: {gis.users.me.username}")

    vehicles = fetch_and_parse_gtfs_data(GTFS_URL)
    valid = validate_and_filter_positions(vehicles)
    if not valid:
        return

    owned = search_owned_exact(gis, FEATURE_LAYER_NAME, ["Feature Service", "Feature Layer"])
    target = prefer_feature_service(owned)

    if target:
        if update_existing_layer(target, valid):
            print(f"Updated existing: {target.title}")
        return

    service, fl = create_feature_layer_with_schema(gis, FEATURE_LAYER_NAME)
    if update_existing_layer(service, valid):
        print(f"Created + populated: {service.title}")

    if args.no_cleanup:
        print("⚠ Cleanup skipped (--no-cleanup flag used).")
    else:
        cleanup_old_services(gis, FEATURE_LAYER_NAME, service.id, keep_n=args.keep_n)


if __name__ == "__main__":
    main()
