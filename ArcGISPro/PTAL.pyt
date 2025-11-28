# -*- coding: utf-8 -*-
# ============================================================
# PTAL_Accessibility_Tools.pyt
#
# Tools:
#   1) GTFSHeadwayGenerator  - static GTFS -> stops w/ HEADWAY_MIN
#   2) PTALLike              - demand points -> PTAL fields
#   3) PTALParcelIndex       - parcels -> centroids -> PTAL -> parcels
#
# Author: ChatGPT for Josh Roberts (City of Adelaide)
# Version: 1.4 (adds parcel-based PTAL tool)
# ============================================================

import arcpy
import csv
import zipfile
import os
import traceback

arcpy.env.overwriteOutput = True


# ============================================================
# Helper functions
# ============================================================
def msg(x): arcpy.AddMessage(str(x))
def warn(x): arcpy.AddWarning(str(x))
def err(x): arcpy.AddError(str(x))


def parse_gtfs_time(t):
    """Convert HH:MM:SS or H:MM:SS into seconds past midnight."""
    if not t:
        return None
    parts = t.split(":")
    if len(parts) != 3:
        return None
    try:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except:
        return None


def load_csv_from_zip(zip_path, filename):
    """Load a CSV (UTF-8) from inside a GTFS ZIP."""
    with zipfile.ZipFile(zip_path, 'r') as z:
        with z.open(filename) as f:
            text = f.read().decode("utf-8-sig").splitlines()
            return list(csv.DictReader(text))


# ============================================================
# Tool 1: GTFS Headway Generator
# ============================================================
class GTFSHeadwayGenerator(object):
    def __init__(self):
        self.label = "GTFS Headway Generator"
        self.description = (
            "Reads static GTFS (ZIP), counts scheduled services per stop "
            "within a time window, and outputs a stops feature class with "
            "HEADWAY_MIN (average minutes between services)."
        )
        self.canRunInBackground = False

    def getParameterInfo(self):

        p0 = arcpy.Parameter(
            displayName="GTFS ZIP File",
            name="gtfs_zip",
            datatype="File",
            parameterType="Required",
            direction="Input"
        )

        p1 = arcpy.Parameter(
            displayName="Output Stops Feature Class",
            name="out_fc",
            datatype="Feature Class",
            parameterType="Required",
            direction="Output"
        )

        p2 = arcpy.Parameter(
            displayName="Start Time (HH:MM:SS)",
            name="start_time",
            datatype="String",
            parameterType="Required",
            direction="Input"
        )
        p2.value = "07:00:00"

        p3 = arcpy.Parameter(
            displayName="End Time (HH:MM:SS)",
            name="end_time",
            datatype="String",
            parameterType="Required",
            direction="Input"
        )
        p3.value = "10:00:00"

        return [p0, p1, p2, p3]

    def updateMessages(self, params):
        for idx in (2, 3):
            val = params[idx].valueAsText
            if val and parse_gtfs_time(val) is None:
                params[idx].setErrorMessage("Time must be HH:MM:SS")

        if params[2].value and params[3].value:
            s = parse_gtfs_time(params[2].valueAsText)
            e = parse_gtfs_time(params[3].valueAsText)
            if s is not None and e is not None and e <= s:
                params[3].setErrorMessage("End time must be after start time.")
        return

    def execute(self, params, messages):

        gtfs_zip = params[0].valueAsText
        out_fc = params[1].valueAsText
        start_sec = parse_gtfs_time(params[2].valueAsText)
        end_sec = parse_gtfs_time(params[3].valueAsText)
        window_minutes = (end_sec - start_sec) / 60.0

        try:
            msg("Loading GTFS tables...")
            stops = load_csv_from_zip(gtfs_zip, "stops.txt")
            stop_times = load_csv_from_zip(gtfs_zip, "stop_times.txt")
            trips = load_csv_from_zip(gtfs_zip, "trips.txt")

            msg("Indexing stop_times by trip_id...")
            times_by_trip = {}
            for st in stop_times:
                times_by_trip.setdefault(st["trip_id"], []).append(st)

            msg("Counting arrivals per stop in time window...")
            stop_arrivals = {}
            for trip in trips:
                t_id = trip["trip_id"]
                for st in times_by_trip.get(t_id, []):
                    arr = parse_gtfs_time(st["arrival_time"])
                    if arr is not None and start_sec <= arr <= end_sec:
                        sid = st["stop_id"]
                        stop_arrivals[sid] = stop_arrivals.get(sid, 0) + 1

            msg("Creating output stops feature class...")
            out_path, out_name = os.path.dirname(out_fc), os.path.basename(out_fc)
            arcpy.management.CreateFeatureclass(
                out_path, out_name, "POINT",
                spatial_reference=arcpy.SpatialReference(4326)
            )

            arcpy.management.AddField(out_fc, "stop_id", "TEXT")
            arcpy.management.AddField(out_fc, "stop_name", "TEXT")
            arcpy.management.AddField(out_fc, "HEADWAY_MIN", "DOUBLE")

            msg("Populating stops with headways...")
            with arcpy.da.InsertCursor(out_fc, ["SHAPE@", "stop_id", "stop_name", "HEADWAY_MIN"]) as cur:
                for s in stops:
                    sid = s["stop_id"]
                    name = s.get("stop_name", "")
                    lat = float(s["stop_lat"])
                    lon = float(s["stop_lon"])
                    if sid in stop_arrivals:
                        headway = window_minutes / float(stop_arrivals[sid])
                    else:
                        headway = None
                    cur.insertRow([arcpy.Point(lon, lat), sid, name, headway])

            msg("GTFS Headway generation completed.")

        except Exception as ex:
            err("GTFS Headway Generator failed.")
            err(str(ex))
            err(traceback.format_exc())
            raise


# ============================================================
# Tool 2: PTAL-like Accessibility Index (points)
# ============================================================
class PTALLike(object):
    def __init__(self):
        self.label = "PTAL-like Accessibility Index (Points)"
        self.description = (
            "Computes a PTAL-like index for demand points using walking "
            "distance to transit stops and service headways."
        )
    def canRunInBackground(self):
        return False

    def getParameterInfo(self):

        p0 = arcpy.Parameter(
            displayName="Demand Points",
            name="demand_fc",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )

        p1 = arcpy.Parameter(
            displayName="Public Transport Stops (with HEADWAY_MIN)",
            name="stops_fc",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )

        p2 = arcpy.Parameter(
            displayName="Headway Field (minutes)",
            name="headway_field",
            datatype="Field",
            parameterType="Required",
            direction="Input"
        )
        # Only numeric fields, dependent on stops_fc
        p2.filter.list = ["Double", "Single", "Integer"]
        p2.parameterDependencies = ["stops_fc"]

        p3 = arcpy.Parameter(
            displayName="Output Feature Class",
            name="out_fc",
            datatype="Feature Class",
            parameterType="Required",
            direction="Output"
        )

        p4 = arcpy.Parameter(
            displayName="Max Walking Distance (meters)",
            name="max_walk_m",
            datatype="Double",
            parameterType="Required",
            direction="Input"
        )
        p4.value = 640.0

        p5 = arcpy.Parameter(
            displayName="Walking Speed (km/h)",
            name="walk_speed_kmh",
            datatype="Double",
            parameterType="Required",
            direction="Input"
        )
        p5.value = 4.8

        return [p0, p1, p2, p3, p4, p5]

    def updateMessages(self, params):
        for idx in (4, 5):
            try:
                if float(params[idx].value) <= 0:
                    params[idx].setErrorMessage("Value must be > 0.")
            except:
                params[idx].setErrorMessage("Numeric value required.")
        return

    def execute(self, params, messages):

        demand_fc = params[0].valueAsText
        stops_fc = params[1].valueAsText
        headway_field = params[2].valueAsText
        out_fc = params[3].valueAsText
        max_walk_m = float(params[4].value)
        walk_speed_kmh = float(params[5].value)

        try:
            msg("Copying demand points to output...")
            arcpy.management.CopyFeatures(demand_fc, out_fc)
            oid_field = arcpy.Describe(out_fc).OIDFieldName

            msg("Building stop headway lookup...")
            stop_headways = {}
            stop_oid = arcpy.Describe(stops_fc).OIDFieldName
            with arcpy.da.SearchCursor(stops_fc, [stop_oid, headway_field]) as cur:
                for oid, hv in cur:
                    try:
                        v = float(hv)
                        if v > 0:
                            stop_headways[oid] = v
                    except:
                        pass

            msg("Generating near table (points to stops)...")
            near_table = os.path.join(arcpy.env.scratchGDB, "PTAL_NearPoints")
            arcpy.analysis.GenerateNearTable(
                out_fc, stops_fc, near_table,
                search_radius=max_walk_m,
                location="NO_LOCATION",
                angle="NO_ANGLE",
                closest="ALL",
                method="PLANAR"
            )

            msg("Computing PTAL for demand points...")
            walk_speed_m_min = (walk_speed_kmh * 1000.0) / 60.0
            ptal_vals = {}

            with arcpy.da.SearchCursor(near_table, ["IN_FID", "NEAR_FID", "NEAR_DIST"]) as cur:
                for inid, stopid, dist in cur:
                    hv = stop_headways.get(stopid)
                    if hv is None:
                        continue

                    walk_t = dist / walk_speed_m_min
                    wait_t = 0.5 * hv
                    total_t = walk_t + wait_t

                    freq_hr = 60.0 / hv
                    weight = 1.0 / (1.0 + total_t)
                    contribution = freq_hr * weight

                    ptal_vals[inid] = ptal_vals.get(inid, 0.0) + contribution

            msg("Adding PTAL fields to output...")
            if "PTAL_RAW" not in [f.name for f in arcpy.ListFields(out_fc)]:
                arcpy.management.AddField(out_fc, "PTAL_RAW", "DOUBLE")
            if "PTAL_CLASS" not in [f.name for f in arcpy.ListFields(out_fc)]:
                arcpy.management.AddField(out_fc, "PTAL_CLASS", "SHORT")

            msg("Writing PTAL values for demand points...")
            with arcpy.da.UpdateCursor(out_fc, [oid_field, "PTAL_RAW", "PTAL_CLASS"]) as cur:
                for oid, raw, cls in cur:
                    v = ptal_vals.get(oid, 0.0)

                    if v <= 0:
                        c = 0
                    elif v < 2:
                        c = 1
                    elif v < 4:
                        c = 2
                    elif v < 6:
                        c = 3
                    elif v < 8:
                        c = 4
                    elif v < 10:
                        c = 5
                    else:
                        c = 6

                    cur.updateRow([oid, v, c])

            msg("PTAL (points) computation complete.")

        except Exception as ex:
            err("PTAL-like Accessibility Index (points) failed")
            err(str(ex))
            err(traceback.format_exc())
            raise


# ============================================================
# Tool 3: PTAL Parcel Index (parcels -> centroids -> PTAL -> parcels)
# ============================================================
class PTALParcelIndex(object):
    def __init__(self):
        self.label = "PTAL Parcel Index (Centroid POC)"
        self.description = (
            "Proof-of-concept: converts parcel polygons to centroids, "
            "computes PTAL for centroids, and writes PTAL values back "
            "to the parcel polygons."
        )
    def canRunInBackground(self):
        return False

    def getParameterInfo(self):

        p0 = arcpy.Parameter(
            displayName="Parcel Polygons",
            name="parcels_fc",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )

        p1 = arcpy.Parameter(
            displayName="Public Transport Stops (with HEADWAY_MIN)",
            name="stops_fc",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )

        p2 = arcpy.Parameter(
            displayName="Headway Field (minutes)",
            name="headway_field",
            datatype="Field",
            parameterType="Required",
            direction="Input"
        )
        p2.filter.list = ["Double", "Single", "Integer"]
        p2.parameterDependencies = ["stops_fc"]

        p3 = arcpy.Parameter(
            displayName="Output Parcel Feature Class (with PTAL)",
            name="out_parcels_fc",
            datatype="Feature Class",
            parameterType="Required",
            direction="Output"
        )

        p4 = arcpy.Parameter(
            displayName="Max Walking Distance (meters)",
            name="max_walk_m",
            datatype="Double",
            parameterType="Required",
            direction="Input"
        )
        p4.value = 640.0

        p5 = arcpy.Parameter(
            displayName="Walking Speed (km/h)",
            name="walk_speed_kmh",
            datatype="Double",
            parameterType="Required",
            direction="Input"
        )
        p5.value = 4.8

        return [p0, p1, p2, p3, p4, p5]

    def updateMessages(self, params):
        for idx in (4, 5):
            try:
                if float(params[idx].value) <= 0:
                    params[idx].setErrorMessage("Value must be > 0.")
            except:
                params[idx].setErrorMessage("Numeric value required.")
        return

    def execute(self, params, messages):

        parcels_fc = params[0].valueAsText
        stops_fc = params[1].valueAsText
        headway_field = params[2].valueAsText
        out_parcels_fc = params[3].valueAsText
        max_walk_m = float(params[4].value)
        walk_speed_kmh = float(params[5].value)

        try:
            # 1) Copy parcels to output
            msg("Copying parcels to output...")
            arcpy.management.CopyFeatures(parcels_fc, out_parcels_fc)
            parcel_oid_field = arcpy.Describe(out_parcels_fc).OIDFieldName

            # 2) Add a stable ID field for mapping centroids back to parcels
            stable_id_field = "PARC_OID"
            existing_fields = [f.name for f in arcpy.ListFields(out_parcels_fc)]
            if stable_id_field not in existing_fields:
                arcpy.management.AddField(out_parcels_fc, stable_id_field, "LONG")

            msg("Populating parcel stable ID field...")
            with arcpy.da.UpdateCursor(out_parcels_fc, [parcel_oid_field, stable_id_field]) as cur:
                for oid, sid in cur:
                    cur.updateRow([oid, oid])

            # 3) Create centroids from parcels (inside polygons)
            scratch_gdb = arcpy.env.scratchGDB
            centroids_fc = os.path.join(scratch_gdb, "PTAL_ParcelCentroids")
            msg("Generating parcel centroids in scratch GDB: {}".format(centroids_fc))
            arcpy.management.FeatureToPoint(out_parcels_fc, centroids_fc, "INSIDE")

            # Ensure centroids have PARC_OID
            cent_fields = [f.name for f in arcpy.ListFields(centroids_fc)]
            if stable_id_field not in cent_fields:
                err("Centroids are missing PARC_OID field; FeatureToPoint did not preserve attributes.")
                raise RuntimeError("Centroids missing PARC_OID.")

            cent_oid_field = arcpy.Describe(centroids_fc).OIDFieldName

            # Build centroid -> parcel OID map
            msg("Building centroid-to-parcel mapping...")
            cent_to_parc = {}
            with arcpy.da.SearchCursor(centroids_fc, [cent_oid_field, stable_id_field]) as cur:
                for cent_oid, parc_oid in cur:
                    cent_to_parc[cent_oid] = parc_oid

            # 4) Build stop headway lookup
            msg("Building stop headway lookup...")
            stop_headways = {}
            stop_oid = arcpy.Describe(stops_fc).OIDFieldName
            with arcpy.da.SearchCursor(stops_fc, [stop_oid, headway_field]) as cur:
                for oid, hv in cur:
                    try:
                        v = float(hv)
                        if v > 0:
                            stop_headways[oid] = v
                    except:
                        pass

            # 5) Generate near table from centroids to stops
            msg("Generating near table (centroids to stops)...")
            near_table = os.path.join(scratch_gdb, "PTAL_ParcelNear")
            arcpy.analysis.GenerateNearTable(
                centroids_fc, stops_fc, near_table,
                search_radius=max_walk_m,
                location="NO_LOCATION",
                angle="NO_ANGLE",
                closest="ALL",
                method="PLANAR"
            )

            # 6) Compute PTAL per parcel (via centroid associations)
            msg("Computing PTAL for parcels via centroids...")
            walk_speed_m_min = (walk_speed_kmh * 1000.0) / 60.0
            ptal_by_parcel = {}

            with arcpy.da.SearchCursor(near_table, ["IN_FID", "NEAR_FID", "NEAR_DIST"]) as cur:
                for in_centroid, stop_id, dist in cur:
                    parc_oid = cent_to_parc.get(in_centroid)
                    if parc_oid is None:
                        continue

                    hv = stop_headways.get(stop_id)
                    if hv is None or hv <= 0:
                        continue

                    walk_t = dist / walk_speed_m_min
                    wait_t = 0.5 * hv
                    total_t = walk_t + wait_t

                    freq_hr = 60.0 / hv
                    weight = 1.0 / (1.0 + total_t)
                    contribution = freq_hr * weight

                    ptal_by_parcel[parc_oid] = ptal_by_parcel.get(parc_oid, 0.0) + contribution

            # 7) Add PTAL fields to parcel output and write values
            msg("Adding PTAL fields to parcel output...")
            existing_fields = [f.name for f in arcpy.ListFields(out_parcels_fc)]
            if "PTAL_RAW" not in existing_fields:
                arcpy.management.AddField(out_parcels_fc, "PTAL_RAW", "DOUBLE")
            if "PTAL_CLASS" not in existing_fields:
                arcpy.management.AddField(out_parcels_fc, "PTAL_CLASS", "SHORT")

            msg("Writing PTAL values to parcels...")
            with arcpy.da.UpdateCursor(out_parcels_fc, [parcel_oid_field, "PTAL_RAW", "PTAL_CLASS"]) as cur:
                for oid, raw, cls in cur:
                    v = ptal_by_parcel.get(oid, 0.0)

                    if v <= 0:
                        c = 0
                    elif v < 2:
                        c = 1
                    elif v < 4:
                        c = 2
                    elif v < 6:
                        c = 3
                    elif v < 8:
                        c = 4
                    elif v < 10:
                        c = 5
                    else:
                        c = 6

                    cur.updateRow([oid, v, c])

            msg("PTAL Parcel Index (centroid POC) computation complete.")

        except Exception as ex:
            err("PTAL Parcel Index tool failed")
            err(str(ex))
            err(traceback.format_exc())
            raise


# ============================================================
# Toolbox class (must be last)
# ============================================================
class Toolbox(object):
    def __init__(self):
        self.label = "PTAL Accessibility Tools"
        self.alias = "ptaltools"
        self.tools = [GTFSHeadwayGenerator, PTALLike, PTALParcelIndex]
