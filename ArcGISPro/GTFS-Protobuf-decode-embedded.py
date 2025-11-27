#!/usr/bin/env python3
"""
Adelaide Metro GTFS-RT Vehicle Position Updater (Local GDB Only)
Version 6.0 – Embedded GTFS-RT Protobuf Parser (No AGOL / No Publishing)

- No dependency on gtfs-realtime-bindings or protobuf package
- Pure-Python GTFS-RT parser for VehiclePosition messages
- NO ArcGIS Online / Portal / publishing logic
- Writes to a local file geodatabase feature class only
"""

import arcpy
import urllib.request
import ssl
import sys
import os
import datetime
import traceback
import struct

# =============================================================================
# Simple Environment Info (no hard failures)
# =============================================================================

def log_environment_info():
    try:
        arcpy.AddMessage(f"Python version: {sys.version}")
        arcpy.AddMessage(f"ArcPy location: {arcpy.__file__}")
    except Exception:
        # If arcpy isn't fully available, AddMessage may not work
        print(f"Python version: {sys.version}")
        print(f"ArcPy location: {getattr(arcpy, '__file__', 'unknown')}")

log_environment_info()

# Optional: add user site-packages if present (harmless)
try:
    user_site_packages = os.path.join(
        os.path.expanduser('~'),
        'AppData',
        'Roaming',
        'Python',
        f'Python{sys.version_info.major}{sys.version_info.minor}',
        'site-packages'
    )
    if os.path.isdir(user_site_packages) and user_site_packages not in sys.path:
        sys.path.insert(0, user_site_packages)
        arcpy.AddMessage(f"Added user site-packages to path: {user_site_packages}")
except Exception as e:
    arcpy.AddWarning(f"Failed to add user site-packages to path: {e}")

# =============================================================================
# Embedded GTFS-RT Protobuf Parser (VehiclePositions)
# =============================================================================
#
# Minimal but complete decoder for GTFS-RT VehiclePosition messages.

class _ProtoReader:
    """Low-level reader for Protocol Buffers binary format."""

    __slots__ = ("data", "i", "n")

    def __init__(self, data: bytes):
        self.data = data
        self.i = 0
        self.n = len(data)

    def eof(self) -> bool:
        return self.i >= self.n

    def _require(self, count: int):
        if self.i + count > self.n:
            raise ValueError("Truncated protobuf message")
        start = self.i
        self.i += count
        return start

    def read_varint(self) -> int:
        """Read a protobuf varint (up to 64 bits)."""
        result = 0
        shift = 0
        while True:
            if self.i >= self.n:
                raise ValueError("Truncated varint")
            b = self.data[self.i]
            self.i += 1
            result |= (b & 0x7F) << shift
            if not (b & 0x80):
                return result
            shift += 7
            if shift >= 64:
                raise ValueError("Varint too long")

    def read_bytes(self, length: int) -> bytes:
        start = self._require(length)
        return self.data[start:start + length]

    def read_float(self) -> float:
        start = self._require(4)
        return struct.unpack("<f", self.data[start:start + 4])[0]

    def read_double(self) -> float:
        start = self._require(8)
        return struct.unpack("<d", self.data[start:start + 8])[0]

    def skip_field(self, wire_type: int):
        """Skip a field with the given wire type."""
        if wire_type == 0:        # varint
            _ = self.read_varint()
        elif wire_type == 1:      # 64-bit
            _ = self.read_bytes(8)
        elif wire_type == 2:      # length-delimited
            length = self.read_varint()
            _ = self.read_bytes(length)
        elif wire_type == 5:      # 32-bit
            _ = self.read_bytes(4)
        else:
            raise ValueError(f"Unsupported protobuf wire type: {wire_type}")


def _parse_trip_descriptor(data: bytes) -> dict:
    """
    Parse TripDescriptor, focusing on:
      - trip_id (1, string)
      - start_time (2, string)
      - start_date (3, string)
      - route_id (5, string)
      - direction_id (6, uint32)
    """
    r = _ProtoReader(data)
    out = {
        "trip_id": None,
        "route_id": None,
        "direction_id": None,
        "start_time": None,
        "start_date": None,
    }

    while not r.eof():
        tag = r.read_varint()
        field = tag >> 3
        wt = tag & 0x07

        if wt == 2 and field in (1, 2, 3, 5):
            length = r.read_varint()
            s = r.read_bytes(length).decode("utf-8", "ignore")
            if field == 1:
                out["trip_id"] = s
            elif field == 2:
                out["start_time"] = s
            elif field == 3:
                out["start_date"] = s
            elif field == 5:
                out["route_id"] = s
        elif wt == 0 and field == 6:
            out["direction_id"] = r.read_varint()
        else:
            r.skip_field(wt)

    return out


def _parse_vehicle_descriptor(data: bytes) -> dict:
    """
    Parse VehicleDescriptor, focusing on:
      - id (1, string)
      - label (2, string)
      - license_plate (3, string)
    """
    r = _ProtoReader(data)
    out = {
        "vehicle_id": None,
        "vehicle_label": None,
        "license_plate": None,
    }

    while not r.eof():
        tag = r.read_varint()
        field = tag >> 3
        wt = tag & 0x07

        if wt == 2 and field in (1, 2, 3):
            length = r.read_varint()
            s = r.read_bytes(length).decode("utf-8", "ignore")
            if field == 1:
                out["vehicle_id"] = s
            elif field == 2:
                out["vehicle_label"] = s
            elif field == 3:
                out["license_plate"] = s
        else:
            r.skip_field(wt)

    return out


def _parse_position(data: bytes) -> dict:
    """
    Parse Position:
      - latitude  (1, float)
      - longitude (2, float)
      - bearing   (3, float, optional)
      - speed     (5, float, optional)
    """
    r = _ProtoReader(data)
    out = {
        "latitude": None,
        "longitude": None,
        "bearing": None,
        "speed": None,
    }

    while not r.eof():
        tag = r.read_varint()
        field = tag >> 3
        wt = tag & 0x07

        if wt == 5 and field == 1:        # float latitude
            out["latitude"] = r.read_float()
        elif wt == 5 and field == 2:      # float longitude
            out["longitude"] = r.read_float()
        elif wt == 5 and field == 3:      # float bearing
            out["bearing"] = r.read_float()
        elif wt == 5 and field == 5:      # float speed
            out["speed"] = r.read_float()
        else:
            r.skip_field(wt)

    return out


def _parse_vehicle_position(data: bytes) -> dict:
    """
    Parse VehiclePosition, combining:
      - TripDescriptor     (field 1, message)
      - Position           (field 2, message)
      - current_stop_sequence (3, uint32) [ignored here]
      - timestamp          (5, uint64)
      - stop_id            (7, string)
      - VehicleDescriptor  (field 8, message)
    """
    r = _ProtoReader(data)

    out = {
        # Trip fields
        "trip_id": None,
        "route_id": None,
        "direction_id": None,
        "start_time": None,
        "start_date": None,
        # VehicleDescriptor fields
        "vehicle_id": None,
        "vehicle_label": None,
        "license_plate": None,
        # Position fields
        "latitude": None,
        "longitude": None,
        "bearing": None,
        "speed": None,
        # VehiclePosition-level fields
        "current_stop_id": None,
        "timestamp": None,
    }

    while not r.eof():
        tag = r.read_varint()
        field = tag >> 3
        wt = tag & 0x07

        if field == 1 and wt == 2:  # trip (TripDescriptor)
            length = r.read_varint()
            sub = r.read_bytes(length)
            out.update(_parse_trip_descriptor(sub))

        elif field == 8 and wt == 2:  # vehicle (VehicleDescriptor)
            length = r.read_varint()
            sub = r.read_bytes(length)
            out.update(_parse_vehicle_descriptor(sub))

        elif field == 2 and wt == 2:  # position (Position)
            length = r.read_varint()
            sub = r.read_bytes(length)
            out.update(_parse_position(sub))

        elif field == 7 and wt == 2:  # stop_id (string)
            length = r.read_varint()
            out["current_stop_id"] = r.read_bytes(length).decode("utf-8", "ignore")

        elif field == 5 and wt == 0:  # timestamp (uint64)
            out["timestamp"] = r.read_varint()

        else:
            # Skip fields we don't use
            r.skip_field(wt)

    return out


def _parse_feed_entity_vehicle(data: bytes):
    """
    Parse a FeedEntity and return a dict representing VehiclePosition
    if present, otherwise None.
    """
    r = _ProtoReader(data)
    vehicle = None

    while not r.eof():
        tag = r.read_varint()
        field = tag >> 3
        wt = tag & 0x07

        # FeedEntity fields:
        #   1 - id (string)
        #   2 - is_deleted (bool)
        #   3 - trip_update (TripUpdate, message)
        #   4 - vehicle (VehiclePosition, message) <-- what we care about
        #   5 - alert (Alert, message)
        if field == 4 and wt == 2:
            length = r.read_varint()
            sub = r.read_bytes(length)
            vehicle = _parse_vehicle_position(sub)
        else:
            r.skip_field(wt)

    return vehicle


def _parse_feed_message_vehicles(data: bytes):
    """
    Parse FeedMessage and return all VehiclePosition dicts.
    We ignore the header and any non-vehicle entities.
    """
    r = _ProtoReader(data)
    vehicles = []

    while not r.eof():
        try:
            tag = r.read_varint()
        except ValueError:
            break  # defensive break

        field = tag >> 3
        wt = tag & 0x07

        if field == 1 and wt == 2:
            # header (FeedHeader) - skip
            length = r.read_varint()
            _ = r.read_bytes(length)

        elif field == 2 and wt == 2:
            # entity (FeedEntity)
            length = r.read_varint()
            sub = r.read_bytes(length)
            v = _parse_feed_entity_vehicle(sub)
            if v is not None:
                vehicles.append(v)
        else:
            r.skip_field(wt)

    return vehicles


def parse_with_bindings(feed_data: bytes):
    """
    Replacement for the old gtfs-realtime-bindings-based parser.

    Returns a list of dictionaries, each representing a vehicle with keys:
      - vehicle_id
      - vehicle_label
      - license_plate
      - trip_id
      - route_id
      - direction_id
      - start_time
      - start_date
      - latitude
      - longitude
      - bearing
      - speed
      - current_stop_id
      - position_timestamp (datetime or None)
      - last_updated      (datetime, now)
    """
    raw_vehicles = _parse_feed_message_vehicles(feed_data)
    now = datetime.datetime.now()
    vehicles = []

    for vp in raw_vehicles:
        timestamp = vp.get("timestamp")
        pos_dt = None
        if isinstance(timestamp, int):
            try:
                pos_dt = datetime.datetime.fromtimestamp(timestamp)
            except (OverflowError, OSError, ValueError):
                pos_dt = None

        v = {
            "vehicle_id": vp.get("vehicle_id"),
            "vehicle_label": vp.get("vehicle_label"),
            "license_plate": vp.get("license_plate"),
            "trip_id": vp.get("trip_id"),
            "route_id": vp.get("route_id"),
            "direction_id": vp.get("direction_id"),
            "start_time": vp.get("start_time"),
            "start_date": vp.get("start_date"),
            "latitude": vp.get("latitude"),
            "longitude": vp.get("longitude"),
            "bearing": vp.get("bearing"),
            "speed": vp.get("speed"),
            "current_stop_id": vp.get("current_stop_id"),
            "position_timestamp": pos_dt,
            "last_updated": now,
        }

        vehicles.append(v)

    return vehicles

# =============================================================================
# Constants (LOCAL ONLY)
# =============================================================================

DEFAULT_GDB_PATH = r"C:\Temp\GIS\NetworkDataset\GTFS.gdb"
DEFAULT_FEATURE_CLASS = "Adelaide_PT_Vehicles"
DEFAULT_ID_FIELD = "VehicleID"
WGS84_SR = arcpy.SpatialReference(4326)

# =============================================================================
# GTFS Fetch
# =============================================================================

def fetch_and_parse_gtfs_data(url):
    arcpy.AddMessage(f"Fetching data from: {url}")
    try:
        ctx = ssl.create_default_context()
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "ArcGIS Pro GTFS-RT Client"}
        )
        with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
            data = r.read()

        arcpy.AddMessage("Parsing GTFS-RT feed (embedded protobuf parser)...")
        return parse_with_bindings(data)

    except Exception as e:
        arcpy.AddError(f"Error fetching GTFS data: {e}")
        arcpy.AddError(traceback.format_exc())
        return []

# =============================================================================
# Spatial Filter
# =============================================================================

def validate_and_filter_positions(vehicles):
    valid = []
    for v in vehicles:
        lat = v.get("latitude")
        lon = v.get("longitude")
        if lat is None or lon is None:
            continue
        # Rough Adelaide bounding box
        if -36.5 <= lat <= -33.5 and 137.5 <= lon <= 140.5:
            valid.append(v)

    arcpy.AddMessage(f"Vehicles inside Adelaide extent: {len(valid)}")
    return valid

# =============================================================================
# Create/Reset Local FC
# =============================================================================

def create_comprehensive_layer(gdb, fc_name, id_field):
    out_fc = os.path.join(gdb, fc_name)

    if not arcpy.Exists(gdb):
        arcpy.AddMessage(f"GDB not found, creating: {gdb}")
        gdb_folder = os.path.dirname(gdb)
        gdb_name = os.path.basename(gdb)
        if not os.path.isdir(gdb_folder):
            os.makedirs(gdb_folder, exist_ok=True)
        arcpy.CreateFileGDB_management(gdb_folder, gdb_name)

    if arcpy.Exists(out_fc):
        arcpy.Delete_management(out_fc)
        arcpy.AddWarning(f"Deleted old FC: {out_fc}")

    arcpy.CreateFeatureclass_management(
        gdb,
        fc_name,
        "POINT",
        spatial_reference=WGS84_SR
    )

    fields = [
        (id_field, "TEXT", 50),
        ("VehicleLabel", "TEXT", 100),
        ("LicensePlate", "TEXT", 20),
        ("TripID", "TEXT", 50),
        ("RouteID", "TEXT", 20),
        ("DirectionID", "SHORT", None),
        ("StartTime", "TEXT", 8),
        ("StartDate", "TEXT", 8),
        ("Bearing", "FLOAT", None),
        ("Speed", "FLOAT", None),
        ("CurrentStopID", "TEXT", 50),
        ("PositionTimestamp", "DATE", None),
        ("LastUpdated", "DATE", None),
    ]

    for name, ftype, length in fields:
        arcpy.AddField_management(
            out_fc,
            name,
            ftype,
            field_length=length
        )

    return out_fc

# =============================================================================
# Populate Local FC
# =============================================================================

def populate_local_feature_class(fc_path, id_field, vehicles):
    arcpy.AddMessage(f"Populating local feature class: {fc_path}")

    fields = [
        "SHAPE@XY",
        id_field,
        "VehicleLabel",
        "LicensePlate",
        "TripID",
        "RouteID",
        "DirectionID",
        "StartTime",
        "StartDate",
        "Bearing",
        "Speed",
        "CurrentStopID",
        "PositionTimestamp",
        "LastUpdated",
    ]

    count = 0
    with arcpy.da.InsertCursor(fc_path, fields) as cur:
        for v in vehicles:
            lat = v.get("latitude")
            lon = v.get("longitude")
            if lat is None or lon is None:
                continue

            row = [
                (lon, lat),                 # SHAPE@XY
                v.get("vehicle_id"),        # ID
                v.get("vehicle_label"),
                v.get("license_plate"),
                v.get("trip_id"),
                v.get("route_id"),
                v.get("direction_id"),
                v.get("start_time"),
                v.get("start_date"),
                v.get("bearing"),
                v.get("speed"),
                v.get("current_stop_id"),
                v.get("position_timestamp"),
                v.get("last_updated"),
            ]
            cur.insertRow(row)
            count += 1

    arcpy.AddMessage(f"Inserted {count} vehicle records into {fc_path}")

# =============================================================================
# Main Execution (Local GDB Only)
# =============================================================================

def main():
    arcpy.AddMessage("Starting Adelaide GTFS-RT updater (LOCAL GDB ONLY)...")

    url = "https://gtfs.adelaidemetro.com.au/v1/realtime/vehicle_positions"
    gdb = DEFAULT_GDB_PATH
    fc_name = DEFAULT_FEATURE_CLASS
    id_field = DEFAULT_ID_FIELD

    vehicles = fetch_and_parse_gtfs_data(url)
    if not vehicles:
        arcpy.AddWarning("No vehicles returned from GTFS-RT feed.")
        return

    valid = validate_and_filter_positions(vehicles)
    if not valid:
        arcpy.AddWarning("No vehicles within Adelaide extent.")
        return

    fc_path = create_comprehensive_layer(gdb, fc_name, id_field)
    populate_local_feature_class(fc_path, id_field, valid)

    arcpy.AddMessage("✔ Local GTFS-RT update complete.")
    arcpy.AddMessage(f"Output feature class: {fc_path}")

if __name__ == "__main__":
    main()
