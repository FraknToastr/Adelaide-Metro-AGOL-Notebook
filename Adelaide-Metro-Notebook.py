#!/usr/bin/env python3
"""
Adelaide Metro GTFS-RT Vehicle Position Fetcher
Version 5.13 - Fixed service name conflict handling

This script fetches real-time vehicle position data from Adelaide Metro's GTFS-RT feed,
and creates/updates a hosted feature layer in ArcGIS Online.

This version includes improved error handling for service name conflicts.

Author: Converted for ArcGIS Online Integration
"""

import urllib.request
import certifi
import ssl
import sys
import datetime
import traceback
import json
import pandas as pd
import tempfile
import os
import uuid
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
import urllib3

# Suppress InsecureRequestWarning for cleaner output in Notebooks
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Install gtfs-realtime-bindings if not available
try:
    from google.transit import gtfs_realtime_pb2
    print("✓ Using gtfs-realtime-bindings for parsing")
except ImportError:
    print("Installing gtfs-realtime-bindings...")
    import subprocess
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'gtfs-realtime-bindings'])
    from google.transit import gtfs_realtime_pb2
    print("✓ gtfs-realtime-bindings installed and imported")

# --- Configuration Constants ---
FEATURE_LAYER_NAME = "Adelaide_Metro_Vehicles"
GTFS_URL = "https://gtfs.adelaidemetro.com.au/v1/realtime/vehicle_positions"

# === GTFS-RT Bindings Parser ===
def parse_with_bindings(feed_data):
    """Parse GTFS-RT data using official bindings."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(feed_data)
    vehicles = []

    for entity in feed.entity:
        if entity.HasField('vehicle'):
            vehicle_data = {}
            vehicle_pos = entity.vehicle

            if vehicle_pos.HasField('vehicle'):
                vehicle_desc = vehicle_pos.vehicle
                vehicle_data['vehicle_id'] = vehicle_desc.id if vehicle_desc.id else None
                vehicle_data['vehicle_label'] = vehicle_desc.label if vehicle_desc.label else None
                vehicle_data['license_plate'] = vehicle_desc.license_plate if vehicle_desc.license_plate else None

            if vehicle_pos.HasField('trip'):
                trip_desc = vehicle_pos.trip
                vehicle_data['trip_id'] = trip_desc.trip_id if trip_desc.trip_id else None
                vehicle_data['route_id'] = trip_desc.route_id if trip_desc.route_id else None
                vehicle_data['direction_id'] = trip_desc.direction_id if trip_desc.HasField('direction_id') else None
                vehicle_data['start_time'] = trip_desc.start_time if trip_desc.start_time else None
                vehicle_data['start_date'] = trip_desc.start_date if trip_desc.start_date else None

            if vehicle_pos.HasField('position'):
                pos = vehicle_pos.position
                vehicle_data['latitude'] = pos.latitude
                vehicle_data['longitude'] = pos.longitude
                vehicle_data['bearing'] = pos.bearing if pos.HasField('bearing') else None
                vehicle_data['speed'] = pos.speed if pos.HasField('speed') else None

            vehicle_data['current_stop_id'] = vehicle_pos.stop_id if vehicle_pos.stop_id else None

            if vehicle_pos.HasField('timestamp'):
                vehicle_data['position_timestamp'] = datetime.datetime.fromtimestamp(vehicle_pos.timestamp)

            vehicle_data['last_updated'] = datetime.datetime.now()

            vehicles.append(vehicle_data)

    return vehicles

# === Data Fetching and Parsing ===
def fetch_and_parse_gtfs_data(gtfs_url):
    """Fetch and parse GTFS-RT data."""
    print(f"Fetching data from: {gtfs_url}")
    try:
        context = ssl.create_default_context(cafile=certifi.where())
        request = urllib.request.Request(
            gtfs_url,
            headers={'User-Agent': 'ArcGIS Online Notebook GTFS-RT Client'}
        )
        with urllib.request.urlopen(request, context=context, timeout=30) as response:
            feed_data = response.read()
        print("Data fetched successfully. Parsing...")
        vehicles = parse_with_bindings(feed_data)
        print(f"Parsed {len(vehicles)} vehicles from feed")
        return vehicles
    except Exception as e:
        print(f"Error fetching/parsing data: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        return []

def validate_and_filter_positions(vehicles):
    """Validate coordinates for Adelaide region."""
    valid_vehicles = []
    for vehicle in vehicles:
        latitude = vehicle.get('latitude')
        longitude = vehicle.get('longitude')
        if latitude is not None and longitude is not None:
            if -36.5 <= latitude <= -33.5 and 137.5 <= longitude <= 140.5:
                valid_vehicles.append(vehicle)
    print(f"Found {len(valid_vehicles)} valid vehicles within Adelaide bounds.")
    return valid_vehicles

def classify_vehicle_type(route_id):
    """Classify vehicle type based on RouteID patterns."""
    if not route_id:
        return "Unknown"

    route_id = route_id.upper().strip()

    # Tram routes (specific route IDs)
    if route_id in ["GLNELG", "BTANIC"]:
        return "Tram"

    # Check if purely alphabetic (train lines)
    if route_id.isalpha():
        return "Train"

    # Everything else (numbers or number/alpha combinations) is bus
    return "Bus"

# === Enhanced ArcGIS Online Feature Layer Management ===
def find_existing_service(gis, service_name):
    """Find any existing service (Feature Layer, Map Service, etc.) by name."""
    try:
        print(f"Searching for existing services with name: {service_name}")
        
        # Search for any content with this name owned by the current user
        search_results = gis.content.search(f'title:"{service_name}" AND owner:{gis.users.me.username}')
        
        if search_results:
            print(f"Found {len(search_results)} item(s) with matching name:")
            for item in search_results:
                print(f"  - {item.title} (Type: {item.type}, ID: {item.id})")
            return search_results
        else:
            print("No existing items found with this name.")
            return []
            
    except Exception as e:
        print(f"Error searching for existing services: {e}")
        print(f"Full traceback: {traceback.format_exc()}")
        return []

def delete_conflicting_services(gis, service_name):
    """Delete any existing services that might conflict with the new layer name."""
    try:
        existing_items = find_existing_service(gis, service_name)
        
        if existing_items:
            print(f"\nFound {len(existing_items)} existing item(s) with the name '{service_name}'.")
            print("Attempting to delete conflicting items...")
            
            for item in existing_items:
                try:
                    print(f"Deleting {item.type}: {item.title} (ID: {item.id})")
                    delete_result = item.delete()
                    if delete_result:
                        print(f"  ✓ Successfully deleted {item.title}")
                    else:
                        print(f"  ✗ Failed to delete {item.title}")
                except Exception as delete_error:
                    print(f"  ✗ Error deleting {item.title}: {delete_error}")
            
            print("Cleanup completed.")
            return True
        else:
            print("No conflicting items found.")
            return True
            
    except Exception as e:
        print(f"Error during cleanup: {e}")
        return False

def find_existing_feature_layer(gis, layer_name):
    """Find existing feature layer by name and type."""
    try:
        search_results = gis.content.search(f'title:"{layer_name}" AND owner:{gis.users.me.username} AND type:"Feature Layer"')
        if search_results:
            # Return the first Feature Layer found
            for item in search_results:
                if item.type == "Feature Layer":
                    return item
    except Exception as e:
        print(f"Error searching for existing feature layer: {e}")
        print(f"Full traceback: {traceback.format_exc()}")
    return None

def update_existing_layer(layer_item, vehicles_data):
    """Update an existing feature layer with new vehicle data."""
    try:
        print(f"Updating existing layer: {layer_item.title}")
        feature_layer = layer_item.layers[0]

        # Convert vehicle data to features
        features = []
        for vehicle in vehicles_data:
            if vehicle.get('latitude') is not None and vehicle.get('longitude') is not None:
                attributes = {
                    "VehicleID": vehicle.get('vehicle_id'),
                    "VehicleLabel": vehicle.get('vehicle_label'),
                    "LicensePlate": vehicle.get('license_plate'),
                    "TripID": vehicle.get('trip_id'),
                    "RouteID": vehicle.get('route_id'),
                    "DirectionID": vehicle.get('direction_id'),
                    "StartTime": vehicle.get('start_time'),
                    "StartDate": vehicle.get('start_date'),
                    "Bearing": vehicle.get('bearing'),
                    "Speed": vehicle.get('speed'),
                    "CurrentStopID": vehicle.get('current_stop_id'),
                    "PositionTimestamp": int(vehicle['position_timestamp'].timestamp() * 1000) if vehicle.get('position_timestamp') else None,
                    "LastUpdated": int(vehicle['last_updated'].timestamp() * 1000),
                    "VehicleType": classify_vehicle_type(vehicle.get('route_id'))
                }
                feature = {
                    "geometry": {
                        "x": vehicle['longitude'],
                        "y": vehicle['latitude'],
                        "spatialReference": {"wkid": 4326}
                    },
                    "attributes": attributes
                }
                features.append(feature)

        # Use a single `truncate` and `add` operation for efficiency
        print("Truncating existing features...")
        feature_layer.manager.truncate()
        print("Adding new features...")
        add_result = feature_layer.edit_features(adds=features)

        add_count = len(add_result.get('addResults', []))
        print(f"Added {add_count} new features.")

        # Check for errors in the add operation
        add_errors = [r for r in add_result.get('addResults', []) if not r['success']]
        if add_errors:
            print(f"Errors occurred while adding features: {add_errors}")
            return False

        return True

    except Exception as e:
        print(f"Error updating existing layer: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        return False

def create_feature_layer_with_unique_name(gis, vehicles_data, base_name):
    """Create a new feature layer with a unique name if conflicts exist."""
    temp_csv_item = None
    try:
        # Generate a unique service name
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_name = f"{base_name}_{timestamp}"
        
        print(f"Creating new hosted feature layer '{unique_name}' from {len(vehicles_data)} vehicles...")

        # Convert to DataFrame
        df = pd.DataFrame(vehicles_data)

        # Clean and prepare data
        df['PositionTimestamp'] = pd.to_datetime(df['position_timestamp'])
        df['LastUpdated'] = pd.to_datetime(df['last_updated'])
        df['VehicleType'] = df['route_id'].apply(classify_vehicle_type)

        # Rename columns to match the schema
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

        # Filter out records without valid coordinates
        df = df.dropna(subset=['longitude', 'latitude'])

        # Use a temporary CSV file with a unique name
        temp_dir = tempfile.gettempdir()
        csv_filename = f"{unique_name}_{str(uuid.uuid4())[:8]}.csv"
        csv_path = os.path.join(temp_dir, csv_filename)

        # Save the dataframe to CSV
        df.to_csv(csv_path, index=False)

        # Set item properties with the unique layer name
        item_properties = {
            'title': unique_name,
            'description': f'Adelaide Metro real-time vehicle positions - Created {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
            'tags': 'Adelaide Metro, GTFS-RT, Real-time',
            'type': 'CSV',
            'typeKeywords': 'CSV, Location, GPS, RT'
        }

        # Add the CSV as an item
        temp_csv_item = gis.content.add(item_properties, data=csv_path)

        # Publish the CSV as a feature layer with unique name
        published_layer = temp_csv_item.publish(
            publish_parameters={
                'name': unique_name,
                'locationType': 'coordinates',
                'latitudeFieldName': 'latitude',
                'longitudeFieldName': 'longitude'
            }
        )
        
        print(f"Created new hosted feature layer: {published_layer.title}")
        print(f"Feature layer URL: {published_layer.url}")
        
        return published_layer, published_layer.layers[0]

    except Exception as e:
        print(f"Error creating feature layer: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        return None, None
    finally:
        # Cleanup temporary files and items
        if 'csv_path' in locals() and os.path.exists(csv_path):
            os.remove(csv_path)
        if temp_csv_item:
            temp_csv_item.delete()

def main():
    """Main function with improved error handling."""
    try:
        print("Starting Adelaide GTFS-RT ArcGIS Online fetcher...")

        # Connect to ArcGIS Online
        print("Connecting to ArcGIS Online...")
        gis = GIS("home")
        print(f"Connected as: {gis.properties.user.username}")

        # Fetch and validate data
        vehicles = fetch_and_parse_gtfs_data(GTFS_URL)
        if not vehicles:
            print("No vehicle data retrieved. Exiting.")
            return

        valid_vehicles = validate_and_filter_positions(vehicles)
        if not valid_vehicles:
            print("No valid vehicle data found. Exiting.")
            return

        # Check for existing Feature Layer specifically
        existing_layer = find_existing_feature_layer(gis, FEATURE_LAYER_NAME)
        
        # Also check if we should use the existing Feature Service found earlier
        if not existing_layer:
            # Look specifically for Feature Service type
            feature_services = gis.content.search(f'title:"{FEATURE_LAYER_NAME}" AND owner:{gis.users.me.username} AND type:"Feature Service"')
            if feature_services:
                existing_layer = feature_services[0]
                print(f"Found existing Feature Service to use: {existing_layer.title}")

        if existing_layer:
            print(f"Found existing Feature Layer: {existing_layer.title}")
            success = update_existing_layer(existing_layer, valid_vehicles)
            if success:
                print(f"\n=== SUCCESS ===")
                print(f"Successfully updated existing layer: {existing_layer.title}")
                print(f"URL: {existing_layer.url}")
            else:
                print("Failed to update existing layer. Exiting.")
        else:
            print("No existing Feature Layer found.")
            
            # Check if there are any conflicting services and handle accordingly
            conflicting_services = find_existing_service(gis, FEATURE_LAYER_NAME)
            
            if conflicting_services:
                print(f"\nFound {len(conflicting_services)} conflicting service(s) with the same name.")
                print("Options:")
                print("1. Delete conflicting services and create new layer")
                print("2. Create layer with timestamp suffix")
                
                # For automation, we'll create with timestamp suffix
                print("Creating layer with unique timestamp suffix...")
                layer_item, feature_layer = create_feature_layer_with_unique_name(gis, valid_vehicles, FEATURE_LAYER_NAME)
            else:
                print("No naming conflicts detected. Creating new layer...")
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

# Additional utility function for manual cleanup if needed
def cleanup_conflicting_services(gis, service_name):
    """Utility function to manually clean up conflicting services."""
    print(f"Manual cleanup for services named: {service_name}")
    return delete_conflicting_services(gis, service_name)

# Run the script
if __name__ == '__main__':
    main()
