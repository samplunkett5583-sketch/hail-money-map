#!/usr/bin/env python3
"""Convert one MRMS MESH_Max_1440min GRIB2 grid into nested hail polygons."""

import argparse
import json
import math
import os
import sys

try:
    import numpy as np
    from osgeo import gdal, ogr, osr
except ImportError as exc:
    raise SystemExit(
        "QGIS/GDAL Python is required (osgeo.gdal, osgeo.ogr, numpy): " + str(exc)
    )

gdal.UseExceptions()

BANDS_INCHES = [round(value / 4, 2) for value in range(3, 17)]
MIN_AREA_SQ_MI = 0.50
SQ_METERS_PER_SQ_MILE = 2_589_988.110336
METERS_PER_MILE = 1609.344
GROUND_ANCHOR_RADIUS_MILES = 22.0
GROUND_ANCHOR_SIGMA_MILES = 8.0


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--anchors")
    return parser.parse_args()


def clone_to_wgs84(geometry, source_srs):
    geom = geometry.Clone()
    if source_srs:
        target = osr.SpatialReference()
        target.ImportFromEPSG(4326)
        target.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        source_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        if not source_srs.IsSame(target):
            geom.Transform(osr.CoordinateTransformation(source_srs, target))
    return geom


def area_sq_mi(wgs84_geometry):
    equal_area = osr.SpatialReference()
    equal_area.ImportFromEPSG(5070)
    equal_area.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    wgs84 = osr.SpatialReference()
    wgs84.ImportFromEPSG(4326)
    wgs84.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    geom = wgs84_geometry.Clone()
    geom.Transform(osr.CoordinateTransformation(wgs84, equal_area))
    return abs(geom.GetArea()) / SQ_METERS_PER_SQ_MILE


def iter_polygon_parts(geometry):
    geometry_type = geometry.GetGeometryType()
    flatten = getattr(ogr, "wkbFlatten", None)
    flat = flatten(geometry_type) if flatten else ogr.GT_Flatten(geometry_type)
    if flat == ogr.wkbPolygon:
        yield geometry
    elif flat == ogr.wkbMultiPolygon:
        for idx in range(geometry.GetGeometryCount()):
            yield geometry.GetGeometryRef(idx)


def smooth_geometry(wgs84_geometry, band_min):
    """Round cell stair-steps without inventing a new storm footprint."""
    equal_area = osr.SpatialReference()
    equal_area.ImportFromEPSG(5070)
    equal_area.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    wgs84 = osr.SpatialReference()
    wgs84.ImportFromEPSG(4326)
    wgs84.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    projected = wgs84_geometry.Clone()
    projected.Transform(osr.CoordinateTransformation(wgs84, equal_area))
    # Keep the broad 0.75" footprint close to the radar observation. Apply
    # progressively stronger rounding only to the nested orange/red cores,
    # where individual MRMS pixels are most visually obvious.
    interior_strength = max(0.0, min(3.0, (float(band_min) - 0.75) / 0.25))
    closing_meters = 650 + (interior_strength * 165)
    simplify_meters = 180 + (interior_strength * 55)
    smoothed = projected.Buffer(closing_meters).Buffer(-closing_meters)
    if not smoothed or smoothed.IsEmpty():
        smoothed = projected
    simplified = smoothed.SimplifyPreserveTopology(simplify_meters)
    if simplified and not simplified.IsEmpty():
        smoothed = simplified
    smoothed.Transform(osr.CoordinateTransformation(equal_area, wgs84))
    return smoothed


def clean_mask(mask):
    """Remove isolated grid specks and fill tiny holes before polygonizing."""
    current = mask.astype(np.uint8)
    padded = np.pad(current, 1, mode="constant")
    neighbors = np.zeros_like(current, dtype=np.uint8)
    for row_offset in range(3):
        for col_offset in range(3):
            if row_offset == 1 and col_offset == 1:
                continue
            neighbors += padded[
                row_offset:row_offset + current.shape[0],
                col_offset:col_offset + current.shape[1],
            ]
    cleaned = current.copy()
    cleaned[(current == 0) & (neighbors >= 6)] = 1
    cleaned[(current == 1) & (neighbors <= 1)] = 0
    return cleaned


def source_to_pixel(dataset, source_x, source_y):
    inverse = gdal.InvGeoTransform(dataset.GetGeoTransform())
    if inverse is None:
        return None
    # GDAL bindings differ by version: some return the six coefficients
    # directly, while older builds return (success, coefficients).
    if (
        len(inverse) == 2
        and isinstance(inverse[0], (bool, int))
        and isinstance(inverse[1], (tuple, list))
    ):
        if not inverse[0]:
            return None
        inverse = inverse[1]
    pixel = inverse[0] + inverse[1] * source_x + inverse[2] * source_y
    line = inverse[3] + inverse[4] * source_x + inverse[5] * source_y
    return int(round(pixel)), int(round(line))


def apply_ground_anchors(values, dataset, source_srs, anchors):
    """Use verified reports as local size anchors inside the radar footprint."""
    if not anchors:
        return values
    rows, cols = values.shape
    original = values.copy()
    correction_sum = np.zeros_like(values, dtype=np.float32)
    weight_sum = np.zeros_like(values, dtype=np.float32)
    wgs84 = osr.SpatialReference()
    wgs84.ImportFromEPSG(4326)
    wgs84.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    transform = None
    if source_srs:
        source_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        if not source_srs.IsSame(wgs84):
            transform = osr.CoordinateTransformation(wgs84, source_srs)
    geo = dataset.GetGeoTransform()
    cell_miles = max(abs(geo[1]), abs(geo[5]))
    if source_srs and source_srs.IsGeographic():
        cell_miles *= 69.0
    else:
        cell_miles /= METERS_PER_MILE
    cell_miles = max(cell_miles, 0.05)
    radius_cells = max(1, int(math.ceil(GROUND_ANCHOR_RADIUS_MILES / cell_miles)))
    sigma_cells = max(1.0, GROUND_ANCHOR_SIGMA_MILES / cell_miles)
    accepted = 0
    for anchor in anchors:
        try:
            lat = float(anchor["lat"])
            lon = float(anchor["lon"])
            hail_mm = float(anchor["hail_in"]) * 25.4
            confidence = max(0.0, min(1.0, float(anchor.get("confidence", 0.9))))
        except (KeyError, TypeError, ValueError):
            continue
        point = ogr.Geometry(ogr.wkbPoint)
        point.AddPoint(lon, lat)
        if transform:
            point.Transform(transform)
        pixel_line = source_to_pixel(dataset, point.GetX(), point.GetY())
        if not pixel_line:
            continue
        pixel, line = pixel_line
        if line < 0 or line >= rows or pixel < 0 or pixel >= cols:
            continue
        radar_mm = float(original[line, pixel])
        if not math.isfinite(radar_mm) or radar_mm < 12.7:
            # Reports calibrate a radar-defined footprint; they do not create a
            # new swath where MRMS detected no hail-producing storm.
            continue
        delta = max(-50.8, min(50.8, hail_mm - radar_mm))
        row_min, row_max = max(0, line - radius_cells), min(rows, line + radius_cells + 1)
        col_min, col_max = max(0, pixel - radius_cells), min(cols, pixel + radius_cells + 1)
        yy, xx = np.ogrid[row_min:row_max, col_min:col_max]
        distance_sq = (yy - line) ** 2 + (xx - pixel) ** 2
        weights = np.exp(-distance_sq / (2 * sigma_cells ** 2)).astype(np.float32)
        weights[distance_sq > radius_cells ** 2] = 0
        weights *= confidence
        correction_sum[row_min:row_max, col_min:col_max] += weights * delta
        weight_sum[row_min:row_max, col_min:col_max] += weights
        accepted += 1
    calibrated = values.copy()
    affected = weight_sum > 0
    calibrated[affected] += correction_sum[affected] / weight_sum[affected]
    calibrated[np.isfinite(original) & (original < 12.7)] = original[
        np.isfinite(original) & (original < 12.7)
    ]
    calibrated[calibrated < 0] = 0
    print(f"[MRMS-CALIBRATION] applied {accepted} verified ground anchor(s)", flush=True)
    return calibrated


def polygonize_mask(mask, dataset, source_srs, band_min):
    rows, cols = mask.shape
    memory = gdal.GetDriverByName("MEM").Create("", cols, rows, 1, gdal.GDT_Byte)
    memory.SetGeoTransform(dataset.GetGeoTransform())
    memory.SetProjection(dataset.GetProjection())
    mask_band = memory.GetRasterBand(1)
    mask_band.WriteArray(mask)
    mask_band.SetNoDataValue(0)

    vector_driver = ogr.GetDriverByName("Memory")
    vector = vector_driver.CreateDataSource("")
    layer = vector.CreateLayer("mesh", srs=source_srs, geom_type=ogr.wkbPolygon)
    field = ogr.FieldDefn("inside", ogr.OFTInteger)
    layer.CreateField(field)
    gdal.Polygonize(mask_band, mask_band, layer, 0, [], callback=None)

    geometries = []
    layer.ResetReading()
    for feature in layer:
        if feature.GetField("inside") != 1:
            continue
        geometry = feature.GetGeometryRef()
        if not geometry or geometry.IsEmpty():
            continue
        wgs84_geometry = clone_to_wgs84(geometry, source_srs)
        if not wgs84_geometry.IsValid():
            wgs84_geometry = wgs84_geometry.MakeValid()
        for part in iter_polygon_parts(wgs84_geometry):
            candidate = smooth_geometry(part, band_min)
            area = area_sq_mi(candidate)
            if area < MIN_AREA_SQ_MI:
                continue
            geometries.append((candidate, area))
    return geometries


def main():
    args = parse_args()
    dataset = gdal.Open(args.input, gdal.GA_ReadOnly)
    if dataset is None:
        raise RuntimeError("GDAL could not open " + args.input)
    band = dataset.GetRasterBand(1)
    values = band.ReadAsArray().astype(np.float32)
    scale = band.GetScale()
    offset = band.GetOffset()
    if scale not in (None, 1.0):
        values *= float(scale)
    if offset not in (None, 0.0):
        values += float(offset)
    nodata = band.GetNoDataValue()
    if nodata is not None:
        values[np.isclose(values, nodata)] = np.nan
    values[values < 0] = np.nan

    source_srs = None
    projection = dataset.GetProjection()
    if projection:
        source_srs = osr.SpatialReference()
        source_srs.ImportFromWkt(projection)
    anchors = []
    if args.anchors:
        with open(args.anchors, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
            anchors = payload if isinstance(payload, list) else payload.get("anchors", [])
    values = apply_ground_anchors(values, dataset, source_srs, anchors)

    features = []
    for index, band_min in enumerate(BANDS_INCHES):
        band_max = BANDS_INCHES[index + 1] if index + 1 < len(BANDS_INCHES) else None
        threshold_mm = band_min * 25.4
        mask = np.where(np.isfinite(values) & (values >= threshold_mm), 1, 0).astype(np.uint8)
        mask = clean_mask(mask)
        if band_min >= 1.00:
            mask = clean_mask(mask)
        if not np.any(mask):
            continue
        for geometry, area in polygonize_mask(mask, dataset, source_srs, band_min):
            centroid = geometry.Centroid()
            props = {
                "band_min": band_min,
                "band_max": band_max,
                "band_label": (
                    f'{band_min:.2f}"–{band_max:.2f}"'
                    if band_max is not None
                    else f'{band_min:.2f}"+'
                ),
                "centroid_lat": centroid.GetY(),
                "centroid_lon": centroid.GetX(),
                "area_sq_mi": round(area, 3),
                "threshold_mm": threshold_mm,
            }
            features.append({
                "type": "Feature",
                "properties": props,
                "geometry": json.loads(geometry.ExportToJson()),
            })
        print(
            f"[MRMS-CONTOUR] {band_min:.2f}+ inches: "
            f"{sum(1 for f in features if f['properties']['band_min'] == band_min)} polygon(s)",
            flush=True,
        )

    output = {
        "type": "FeatureCollection",
        "source": "MRMS MESH_Max_1440min",
        "features": features,
    }
    with open(args.output, "w", encoding="utf-8") as handle:
        json.dump(output, handle, separators=(",", ":"))


if __name__ == "__main__":
    main()
