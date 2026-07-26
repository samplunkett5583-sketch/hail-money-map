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

BANDS_INCHES = [0.50, 1.00, 1.25, 1.50, 1.75, 2.00, 2.50, 3.00]
MIN_AREA_SQ_MI = 0.50
SQ_METERS_PER_SQ_MILE = 2_589_988.110336


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
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


def polygonize_mask(mask, dataset, source_srs):
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
            candidate = part.Clone()
            area = area_sq_mi(candidate)
            if area < MIN_AREA_SQ_MI:
                continue
            # Keep the native cell boundary. Simplifying even by a small amount
            # turns short, irregular high-hail tracks into straight diagonal
            # edges and rounded-looking blobs at normal map zoom levels.
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

    features = []
    for index, band_min in enumerate(BANDS_INCHES):
        band_max = BANDS_INCHES[index + 1] if index + 1 < len(BANDS_INCHES) else None
        threshold_mm = band_min * 25.4
        mask = np.where(np.isfinite(values) & (values >= threshold_mm), 1, 0).astype(np.uint8)
        if not np.any(mask):
            continue
        for geometry, area in polygonize_mask(mask, dataset, source_srs):
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
