#!/usr/bin/env python3
"""
Adelaide Metro GTFS-RT Vehicle Position Fetcher
Version 6.0 - Robust retries, batching, UTC timestamps, safer AGOL search

Author: Converted for ArcGIS Online Integration (refined)
"""

import urllib.request
import certifi
import ssl
import sys
import datetime as dt
import time
import traceback
import json
import pandas as pd
import tempfile
import os
import uuid
from typing import List, Dict, Tuple, Optional

from arcgis.gis import GIS
import urllib3

# Suppress InsecureRequestWarning for cleaner output in Notebooks
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Ensure gtfs bindings
try:
    from google.transit import gtfs_realtime_pb2
    print("✓ Using gtfs-realtime-bindings for parsing")
except ImportError:
    print("Installing gtfs-realtime-bindings...")
    import subprocess
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'gtfs-realtime-bindings'])
    from google.transit import gtfs_realtime_pb2
    print("✓ gtfs-realtime-bindings installed and imported")

# --- Configuration ---
FEATURE_LAYER_NAME = "Adelaide_Metro_Vehicles"
GTFS_URL = "https://gtfs.adelaidemetro.com.au/v1/realtime/vehicle_positions"
MAX_ADD_PER_REQUEST = 950  # stay below 1000 to be safe

# === Helpers ===
def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def to_epoch_ms(d: Optional[dt.datetime]) -> Optional[int]:
    if d is None:
        return None
    if d.tzinfo is None:
        # assume UTC if naïve
        d = d.replace(tzinfo=dt.timezone.utc)
    return int(d.timestamp() * 1000)

def chunk(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

# === GTFS Parsing ===
def parse_with_bindings(feed_data: bytes) -> List[Dict]:
    """Parse GTFS-RT data using official bindings."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(feed_data)
    vehicles = []
    feed_header_ts = None
    try:
        if feed.header.HasField('timestamp'):
            feed_header_ts = dt.datetime.fromtimestamp(feed.header.timestamp, tz=dt.timezone.utc)
    except Exception:
        feed_header_ts = None

    for entity in feed.entity:
        if entity.HasField('vehicle'):
            vehicle_data = {}
            vehicle_pos = entity.vehicle

            # Vehicle descriptor
            if vehicle_pos.HasField('vehicle'):
                vd = vehicle_pos.vehicle
                vehicle_data['vehicle_id'] = vd.id or None
                vehicle_data['vehicle_label'] = vd.label or None
                vehicle_data['license_plate'] = vd.license_plate or None

            # Trip descriptor
            if vehicle_pos.HasField('trip'):
                td = vehicle_pos.trip
                vehicle_data['trip_id'] = td.trip_id or None
                vehicle_data['route_id'] = td.route_id or None
                # Some GTFS builds use proto2 (HasField OK), guard it:
                try:
                    vehicle_data['direction_id'] = td.direction_id if td.HasField('direction_id') else None
                except Exception:
                    # proto3: fall back to attribute presence
                    vehicle_data['direction_id'] = getattr(td, "direction_id", None)
                vehicle_data['start_time'] = td.start_time or None
                vehicle_data['start_date'] = td.start_date or None

            # Position
            if vehicle_pos.HasField('position'):
                pos = vehicle_pos.position
                vehicle_data['latitude'] = getattr(pos, 'latitude', None)
                vehicle_data['longitude'] = getattr(pos, 'longitude', None)
                try:
                    vehicle_data['bearing'] = pos.bearing if pos.HasField('bearing') else None
                except Exception:
                    vehicle_data['bearing'] = getattr(pos, 'bearing', None)
                try:
                    vehicle_data['speed'] = pos.speed if pos.HasField('speed') else None
                except Exception:
                    vehicle_data['speed'] = getattr(pos, 'speed', None)

            vehicle_data['current_stop_id'] = vehicle_pos.stop_id or None

            if vehicle_pos.HasField('timestamp'):
                vehicle_data['position_timestamp'] = dt.datetime.fromtimestamp(vehicle_pos.timestamp, tz=dt.timezone.utc)
            else:
                vehicle_data['position_timestamp'] = feed_header_ts

            vehicle_data['last_updated'] = utc_now()
            vehicles.append(vehicle_data)

    return vehicles

# === Fetch with retries ===
def fetch_and_parse_gtfs_data(gtfs_url: str, max_retries: int = 4, base_delay: float = 1.5) -> List[Dict]:
    print(f"Fetching data from: {gtfs_url}")
    context = ssl.create_default_context(cafile=certifi.where())
    request = urllib.request.Request(
        gtfs_url,
        headers={'User-Agent': 'ArcGIS Online Notebook GTFS-RT Client'}
    )
    attempt = 0
    while True:
        try:
            with urllib.request.urlopen(request, context=context, timeout=30) as response:
                feed_data = response.read()
            print("Data fetched successfully. Parsing...")
            vehicles = parse_with_bindings(feed_data)
            print(f"Parsed {len(vehicles)} vehicles from feed")
            return vehicles
        except Exception as e:
            attempt += 1
            print(f"Fetch error (attempt {attempt}/{max_retries}): {e}")
            if attempt >= max_retries:
                print(f"Full traceback: {traceback.format_exc()}")
                return []
            sleep_s = base_delay * (2 ** (attempt - 1))
            time.sleep(sleep_s)

# === Validation / classification ===
def validate_and_filter_positions(vehicles: List[Dict]) -> List[Dict]:
    """Validate coordinates for Adelaide region."""
    valid = []
    for v in vehicles:
        lat = v.get('latitude'); lon = v.get('longitude')
        if lat is None or lon is None:
            continue
        # Exclude zero/garbage coords
        if lat == 0 or lon == 0:
            continue
        # Adelaide-ish bounds
        if -36.5 <= lat <= -33.5 and 137.5 <= lon <= 140.5:
            valid.append(v)
    print(f"Found {len(valid)} valid vehicles within Adelaide bounds.")
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

# === AGOL helpers ===
def search_owned_exact(gis: GIS, title: str, types: Optional[List[str]] = None):
    owner = gis.users.me.username
    type_clause = "" if not types else " AND (" + " OR ".join([f'type:"{t}"' for t in types]) + ")"
    q = f'title:"{title}" AND owner:{owner}{type_clause}'
    return gis.content.search(q, max_items=50)

def prefer_feature_service(items):
    # Prefer "Feature Service" items; fall back to "Feature Layer"
    fs = [i for i in items if i.type == "Feature Service"]
    if fs:
        return fs[0]
    fl = [i for i in items if i.type == "Feature Layer"]
    if fl:
        return fl[0]
    return items[0] if items else None

def get_editable_layer(item):
    # Return the first layer that supports edits
    try:
        lyr = item.layers[0]
        # If it's a view or not editable, we may fail on truncate later—handle at call site
        return lyr
    except Exception:
        return None

def update_existing_layer(layer_item, vehicles_data: List[Dict]) -> bool:
    try:
        print(f"Updating existing layer: {layer_item.title}")
        feature_layer = get_editable_layer(layer_item)
        if feature_layer is None:
            print("✗ Could not access a sublayer to edit.")
            return False

        # Build features
        features = []
        for v in vehicles_data:
            lat = v.get('latitude'); lon = v.get('longitude')
            if lat is None or lon is None:
                continue
            attrs = {
                "VehicleID": v.get('vehicle_id'),
                "VehicleLabel": v.get('vehicle_label'),
                "LicensePlate": v.get('license_plate'),
                "TripID": v.get('trip_id'),
                "RouteID": v.get('route_id'),
                "DirectionID": v.get('direction_id'),
                "StartTime": v.get('start_time'),
                "StartDate": v.get('start_date'),
                "Bearing": v.get('bearing'),
                "Speed": v.get('speed'),
                "CurrentStopID": v.get('current_stop_id'),
                "PositionTimestamp": to_epoch_ms(v.get('position_timestamp')),
                "LastUpdated": to_epoch_ms(v.get('last_updated')),
                "VehicleType": classify_vehicle_type(v.get('route_id'))
            }
            features.append({
                "geometry": {"x": float(lon), "y": float(lat), "spatialReference": {"wkid": 4326}},
                "attributes": attrs
            })

        print("Truncating existing features...")
        try:
            feature_layer.manager.truncate()
        except Exception as te:
            # If truncate not supported (e.g., view layer), try deleteFeatures(1=1)
            print(f"  truncate() not available, falling back to deleteFeatures: {te}")
            feature_layer.delete_features(where="1=1")

        print(f"Adding {len(features)} features in batches...")
        add_total = 0
        for batch in chunk(features, MAX_ADD_PER_REQUEST):
            add_result = feature_layer.edit_features(adds=batch)
            add_errors = [r for r in add_result.get('addResults', []) if not r.get('success')]
            add_total += len(add_result.get('addResults', []))
            if add_errors:
                print(f"  Batch add had {len(add_errors)} errors; first error: {add_errors[0]}")
                return False

        print(f"✓ Added {add_total} features.")
        return True

    except Exception as e:
        print(f"Error updating existing layer: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        return False

def create_feature_layer_with_unique_name(gis: GIS, vehicles_data: List[Dict], base_name: str):
    temp_csv_item = None
    try:
        ts = utc_now().strftime("%Y%m%d_%H%M%S")
        unique_name = f"{base_name}_{ts}"
        print(f"Creating new hosted feature layer '{unique_name}' from {len(vehicles_data)} vehicles...")

        df = pd.DataFrame(vehicles_data)
        if df.empty:
            print("✗ No vehicles to publish.")
            return None, None

        # Prepare fields
        df['VehicleType'] = df['route_id'].apply(classify_vehicle_type)
        df['PositionTimestamp'] = df['position_timestamp'].apply(to_epoch_ms)
        df['LastUpdated'] = df['last_updated'].apply(to_epoch_ms)

        # Rename columns to final schema
        df = df.rename(columns={
            'vehicle_id': 'VehicleID',
            'vehicle_label': 'VehicleLabel',
            'license_plate': 'LicensePlate',
            'trip_id': 'TripID',
            'route_id': 'RouteID',
            'direction_id': 'DirectionID',
            'start_time': 'StartTime',
            'start_date': 'StartDate',
            'bearing': 'Bearing',
            'speed': 'Speed',
            'current_stop_id': 'CurrentStopID'
        })

        # Filter valid coords
        df = df.dropna(subset=['longitude', 'latitude'])
        if df.empty:
            print("✗ No valid coordinates to publish.")
            return None, None

        # Save to CSV (control types by pre-casting)
        cast_int = ['DirectionID', 'PositionTimestamp', 'LastUpdated']
        for c in cast_int:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')

        temp_dir = tempfile.gettempdir()
        csv_filename = f"{unique_name}_{uuid.uuid4().hex[:8]}.csv"
        csv_path = os.path.join(temp_dir, csv_filename)
        df.to_csv(csv_path, index=False)

        item_properties = {
            'title': unique_name,
            'description': f'Adelaide Metro real-time vehicle positions - Created {utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")}',
            'tags': 'Adelaide Metro, GTFS-RT, Real-time',
            'type': 'CSV',
            'typeKeywords': 'CSV, Location, GPS, RT'
        }

        temp_csv_item = gis.content.add(item_properties, data=csv_path)

        # Publish as point layer using lat/long
        publish_params = {
            'name': unique_name,
            'locationType': 'coordinates',
            'latitudeFieldName': 'latitude',
            'longitudeFieldName': 'longitude'
        }
        published_layer = temp_csv_item.publish(publish_parameters=publish_params)
        print(f"Created new hosted feature layer: {published_layer.title}")
        print(f"Feature layer URL: {published_layer.url}")
        return published_layer, get_editable_layer(published_layer)
    except Exception as e:
        print(f"Error creating feature layer: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        return None, None
    finally:
        try:
            if temp_csv_item:
                temp_csv_item.delete()
        except Exception:
            pass
        try:
            if 'csv_path' in locals() and os.path.exists(csv_path):
                os.remove(csv_path)
        except Exception:
            pass

# === Main ===
def main():
    try:
        print("Starting Adelaide GTFS-RT ArcGIS Online fetcher...")

        print("Connecting to ArcGIS Online...")
        gis = GIS("home")
        print(f"Connected as: {gis.properties.user.username}")

        vehicles = fetch_and_parse_gtfs_data(GTFS_URL)
        if not vehicles:
            print("No vehicle data retrieved. Exiting.")
            return

        valid_vehicles = validate_and_filter_positions(vehicles)
        if not valid_vehicles:
            print("No valid vehicle data found. Exiting.")
            return

        # Look for existing items owned by this user with the exact title
        print("Searching for existing layer/service…")
        owned = search_owned_exact(gis, FEATURE_LAYER_NAME, types=["Feature Service", "Feature Layer"])
        target_item = prefer_feature_service(owned)

        if target_item:
            print(f"Found existing item: {target_item.title} ({target_item.type})")
            success = update_existing_layer(target_item, valid_vehicles)
            if success:
                print("\n=== SUCCESS ===")
                print(f"Updated: {target_item.title}")
                print(f"URL: {getattr(target_item, 'url', '(no url)')}")
            else:
                print("Failed to update existing layer. Exiting.")
            return

        print("No existing Feature Service/Layer found. Creating new layer with timestamp suffix…")
        layer_item, feature_layer = create_feature_layer_with_unique_name(gis, valid_vehicles, FEATURE_LAYER_NAME)
        if layer_item:
            print("\n=== SUCCESS ===")
            print(f"New Layer: {layer_item.title}")
            print(f"URL: {layer_item.url}")
        else:
            print("Failed to create new feature layer.")

    except Exception as e:
        print(f"Script execution error: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")

# Optional utility retained for manual cleanup (safer: skips protected items)
def cleanup_conflicting_services(gis: GIS, service_name: str):
    print(f"Manual cleanup for services named: {service_name}")
    try:
        items = search_owned_exact(gis, service_name)
        deleted = 0
        for it in items:
            try:
                if getattr(it, "protected", False):
                    print(f"  Skipping protected item: {it.title} ({it.id})")
                    continue
                if it.delete():
                    print(f"  ✓ Deleted: {it.title} ({it.id})")
                    deleted += 1
                else:
                    print(f"  ✗ Failed to delete: {it.title} ({it.id})")
            except Exception as de:
                print(f"  ✗ Error deleting {it.title}: {de}")
        print(f"Cleanup complete. Deleted {deleted} item(s).")
        return True
    except Exception as e:
        print(f"Error during cleanup: {e}")
        return False

if __name__ == '__main__':
    main()
