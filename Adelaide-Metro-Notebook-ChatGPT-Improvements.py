#!/usr/bin/env python3
"""
Adelaide Metro GTFS-RT Vehicle Position Fetcher
Version 6.8 - Fixed schema, robust retries, configurable cleanup,
              Jupyter-safe CLI, auto-README generator (fixed)
"""

import urllib.request, certifi, ssl, sys, datetime as dt, time, traceback, argparse, os
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
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "gtfs-realtime-bindings"])
    from google.transit import gtfs_realtime_pb2
    print("✓ Installed gtfs-realtime-bindings")

# --- Configuration ---
FEATURE_LAYER_NAME = "Adelaide_Metro_Vehicles"
GTFS_URL = "https://gtfs.adelaidemetro.com.au/v1/realtime/vehicle_positions"
MAX_ADD_PER_REQUEST = 950
KEEP_N = 3   # number of timestamped services to keep by default


# === Helpers ===
def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def to_epoch_ms(d: Optional[dt.datetime]) -> Optional[int]:
    if d is None: return None
    if d.tzinfo is None: d = d.replace(tzinfo=dt.timezone.utc)
    return int(d.timestamp() * 1000)

def chunk(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


# === GTFS Parsing ===
def parse_with_bindings(feed_data: bytes) -> List[Dict]:
    feed = gtfs_realtime_pb2.FeedMessage(); feed.ParseFromString(feed_data)
    vehicles, feed_header_ts = [], None
    if feed.header.HasField("timestamp"):
        feed_header_ts = dt.datetime.fromtimestamp(feed.header.timestamp, tz=dt.timezone.utc)
    for entity in feed.entity:
        if entity.HasField("vehicle"):
            v, vp = {}, entity.vehicle
            if vp.HasField("vehicle"):
                v["vehicle_id"], v["vehicle_label"], v["license_plate"] = (
                    vp.vehicle.id or None, vp.vehicle.label or None, vp.vehicle.license_plate or None
                )
            if vp.HasField("trip"):
                td = vp.trip
                v["trip_id"], v["route_id"] = td.trip_id or None, td.route_id or None
                try:
                    v["direction_id"] = td.direction_id if td.HasField("direction_id") else None
                except Exception:
                    v["direction_id"] = getattr(td, "direction_id", None)
                v["start_time"], v["start_date"] = td.start_time or None, td.start_date or None
            if vp.HasField("position"):
                pos = vp.position
                v["latitude"], v["longitude"], v["bearing"], v["speed"] = (
                    getattr(pos, "latitude", None),
                    getattr(pos, "longitude", None),
                    getattr(pos, "bearing", None),
                    getattr(pos, "speed", None),
                )
            v["current_stop_id"] = vp.stop_id or None
            v["position_timestamp"] = (
                dt.datetime.fromtimestamp(vp.timestamp, tz=dt.timezone.utc)
                if vp.HasField("timestamp") else feed_header_ts
            )
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
                print(traceback.format_exc()); return []
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
    if not route_id: return "Unknown"
    rid = route_id.upper().strip()
    if rid in {"GLNELG","BTANIC"}: return "Tram"
    if rid.isalpha(): return "Train"
    return "Bus"


# === AGOL Helpers ===
def search_owned_exact(gis: GIS, title: str, types: Optional[List[str]]=None):
    owner = gis.users.me.username
    type_clause = "" if not types else " AND (" + " OR ".join([f'type:\"{t}\"' for t in types]) + ")"
    return gis.content.search(f'title:\"{title}\" AND owner:\"{owner}\"{type_clause}', max_items=50)

def prefer_feature_service(items):
    fs=[i for i in items if i.type=="Feature Service"]
    fl=[i for i in items if i.type=="Feature Layer"]
    return fs[0] if fs else fl[0] if fl else (items[0] if items else None)

def get_editable_layer(item):
    try: return item.layers[0]
    except Exception: return None


def update_existing_layer(layer_item, vehicles: List[Dict]) -> bool:
    try:
        fl = get_editable_layer(layer_item)
        if not fl: return False
        fl_fields = {f["name"] for f in fl.properties.fields}
        features=[]
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
                attrs={k:val for k,val in attrs.items() if k in fl_fields}
                features.append({
                    "geometry":{"x":v["longitude"],"y":v["latitude"],"spatialReference":{"wkid":4326}},
                    "attributes":attrs
                })
        try: fl.manager.truncate()
        except Exception: fl.delete_features(where="1=1")
        for batch in chunk(features, MAX_ADD_PER_REQUEST):
            fl.edit_features(adds=batch)
        print(f"✓ Updated {len(features)} features."); return True
    except Exception as e:
        print(f"Update error: {e}"); print(traceback.format_exc()); return False


def create_feature_layer_with_schema(gis: GIS, base_name: str):
    ts=utc_now().strftime("%Y%m%d_%H%M%S"); unique=f"{base_name}_{ts}"
    fields=[{"name":"VehicleID","type":"esriFieldTypeString","alias":"Vehicle ID","length":50},
            {"name":"VehicleLabel","type":"esriFieldTypeString","alias":"Vehicle Label","length":50},
            {"name":"LicensePlate","type":"esriFieldTypeString","alias":"License Plate","length":50},
            {"name":"TripID","type":"esriFieldTypeString","alias":"Trip ID","length":50},
            {"name":"RouteID","type":"esriFieldTypeString","alias":"Route ID","length":50},
            {"name":"DirectionID","type":"esriFieldTypeInteger","alias":"Direction ID"},
            {"name":"StartTime","type":"esriFieldTypeString","alias":"Start Time","length":20},
            {"name":"StartDate","type":"esriFieldTypeString","alias":"Start Date","length":20},
            {"name":"Bearing","type":"esriFieldTypeDouble","alias":"Bearing"},
            {"name":"Speed","type":"esriFieldTypeDouble","alias":"Speed"},
            {"name":"CurrentStopID","type":"esriFieldTypeString","alias":"Current Stop ID","length":50},
            {"name":"PositionTimestamp","type":"esriFieldTypeDate","alias":"Position Timestamp"},
            {"name":"LastUpdated","type":"esriFieldTypeDate","alias":"Last Updated"},
            {"name":"VehicleType","type":"esriFieldTypeString","alias":"Vehicle Type","length":20}]
    layer_def={"layers":[{"name":unique,"type":"Feature Layer","geometryType":"esriGeometryPoint","fields":fields,
                          "objectIdField":"OBJECTID",
                          "extent":{"xmin":137.5,"ymin":-36.5,"xmax":140.5,"ymax":-33.5,"spatialReference":{"wkid":4326}},
                          "spatialReference":{"wkid":4326}}]}
    props={"title":unique,"type":"Feature Service","description":"Adelaide Metro vehicles (schema defined)","tags":"Adelaide Metro, GTFS-RT, Vehicles, Real-time"}
    service=gis.content.create_service(unique,props,service_type="featureService",create_params=layer_def)
    return service, FeatureLayerCollection.fromitem(service).layers[0]


def cleanup_old_services(gis: GIS, base_name: str, keep_item_id: str, keep_n: int=3):
    items=search_owned_exact(gis, base_name, ["Feature Service","Feature Layer"])
    base_items=[it for it in items if it.title==base_name]
    timestamped=[it for it in items if it.title.startswith(base_name+"_")]
    timestamped.sort(key=lambda it: it.modified, reverse=True)
    keep_ids={it.id for it in timestamped[:keep_n]} | {keep_item_id}
    deleted=0
    for it in timestamped[keep_n:]:
        if getattr(it,"protected",False) or it.id in keep_ids: continue
        try: it.delete(); deleted+=1; print(f"Deleted old service: {it.title}")
        except Exception as e: print(f"Could not delete {it.title}: {e}")
    print(f"✓ Cleanup complete, deleted {deleted} old service(s).") if deleted else print("No old services deleted.")
    if base_items: print(f"Base service(s) preserved: {[it.title for it in base_items]}")


# === README Auto-Writer ===
def write_readme():
    readme_path=os.path.join(os.path.dirname(__file__),"README.md")
    if os.path.exists(readme_path): return
    content = """# 🚍 Adelaide Metro GTFS-RT → ArcGIS Online
### Vehicle Position Fetcher (v6.8)

Fetches **real-time Adelaide Metro vehicle positions** from the GTFS-RT feed and publishes them to ArcGIS Online.

## 🔧 Requirements
- ArcGIS API for Python (`pip install arcgis`)
- GTFS Realtime Bindings (`pip install gtfs-realtime-bindings`)

## ⚙️ Configuration
At top of script:
FEATURE_LAYER_NAME = "Adelaide_Metro_Vehicles"
KEEP_N = 3   # timestamped services to keep

## 🚀 Usage
python adl_metro_gtfs.py               # keep last 3
python adl_metro_gtfs.py --keep-n 5    # keep last 5
python adl_metro_gtfs.py --no-cleanup  # skip cleanup

## 📂 Output
- Updates existing `Adelaide_Metro_Vehicles` if found.
- Otherwise creates new timestamped service:
  Adelaide_Metro_Vehicles_YYYYMMDD_HHMMSS
- Deletes older timestamped services beyond KEEP_N.

## 🔍 Schema
VehicleID, VehicleLabel, LicensePlate, TripID, RouteID, DirectionID,
StartTime, StartDate, Bearing, Speed, CurrentStopID,
PositionTimestamp (Date), LastUpdated (Date), VehicleType

## 🛠️ Troubleshooting
- Field mismatch → delete service, let script recreate schema.
- Empty results → check Adelaide bounds filter.
- ArcGIS login prompt → ensure GIS("home") is valid.
"""
    with open(readme_path,"w",encoding="utf-8") as f: f.write(content)
    print(f"✓ README.md created at {readme_path}")


# === Argument Handling (Jupyter-safe) ===
def parse_args():
    parser=argparse.ArgumentParser(description="Adelaide Metro GTFS-RT fetcher")
    parser.add_argument("--no-cleanup",action="store_true",help="Skip deleting old timestamped services")
    parser.add_argument("--keep-n",type=int,default=KEEP_N,help=f"Number of timestamped services to keep (default {KEEP_N})")
    args,unknown=parser.parse_known_args()
    if unknown: print(f"⚠ Ignoring unrecognized args: {unknown}")
    return args


# === Main ===
def main():
    args=parse_args()
    gis=GIS("home"); print(f"Connected as: {gis.users.me.username}")
    vehicles=fetch_and_parse_gtfs_data(GTFS_URL); valid=validate_and_filter_positions(vehicles)
    if not valid: return
    owned=search_owned_exact(gis,FEATURE_LAYER_NAME,["Feature Service","Feature Layer"]); target=prefer_feature_service(owned)
    if target:
        if update_existing_layer(target,valid): print(f"Updated existing: {target.title}")
        return
    service,fl=create_feature_layer_with_schema(gis,FEATURE_LAYER_NAME)
    if update_existing_layer(service,valid): print(f"Created + populated: {service.title}")
    if args.no_cleanup: print("⚠ Cleanup skipped (--no-cleanup flag used).")
    else: cleanup_old_services(gis,FEATURE_LAYER_NAME,service.id,keep_n=args.keep_n)
    write_readme()


if __name__=="__main__": main()

