# ===============================================================================
# Module:       pipeline/outputs.py
# Purpose:      Probability surfaces, TARR contour extraction, and the main
#               analysis orchestrator.
# Author:       Jamie F. Weleber
# Created:      March 2026
# ===============================================================================

import numpy as np
import rasterio
import os
import math
from shapely.geometry import shape

from pipeline.shared import WORK_DIR, repair_geometry
from pipeline.shared import get_bbox_from_ipp
from pipeline.downloads import download_dem, download_nlcd, download_osm_features, download_nhd_features
from pipeline.cost_surface import build_cost_surface
from pipeline.cost_distance import compute_cost_distance
from pipeline.jacobs_masks import compute_jacobs_masks


# ===============================================================================
# STEP 1: Probability surface generation
# ===============================================================================

def generate_probability_surface(cost_distance_path, pct_25_km, pct_50_km, pct_75_km, output_path=None):
    if output_path is None:
        output_path = os.path.join(WORK_DIR, 'probability.tif')
    p25 = pct_25_km * 1000
    p50 = pct_50_km * 1000
    p75 = pct_75_km * 1000
    with rasterio.open(cost_distance_path) as src:
        dist = src.read(1).astype(np.float64)
        profile = src.profile.copy()
    nodata_mask = (dist == -9999) | (dist < 0)
    prob = np.zeros_like(dist, dtype=np.float32)
    prob[dist <= p25] = 4.0
    prob[(dist > p25) & (dist <= p50)] = 3.0
    prob[(dist > p50) & (dist <= p75)] = 2.0
    prob[dist > p75] = 1.0
    prob[nodata_mask] = 0.0
    profile['dtype'] = 'float32'
    profile['nodata'] = 0
    with rasterio.open(output_path, 'w', **profile) as dst:
        dst.write(prob, 1)
    print(f"  Probability surface written.")
    return output_path

def round_coords(geom_dict, precision=5):
    """Round all coordinates in a GeoJSON geometry dict to the given precision.
    
    At 30m source resolution, 5 decimal places (~1.1m) captures all meaningful
    spatial information without false precision. This also significantly reduces
    GeoJSON file size and CalTopo URL payload length.
    """
    def _round(coords):
        if isinstance(coords[0], (list, tuple)):
            return [_round(c) for c in coords]
        return [round(v, precision) for v in coords]
    result = dict(geom_dict)
    result['coordinates'] = _round(geom_dict['coordinates'])
    return result


def extract_contour_polygons(cost_distance_path, pct_25_km, pct_50_km, pct_75_km):
    """Extract percentile contour boundaries as GeoJSON polygons.
    
    Returns polygons for the 25th, 50th, and 75th percentile zones
    suitable for rendering as vector layers or exporting to CalTopo.
    """
    from rasterio.features import shapes
    from shapely.geometry import shape, mapping
    from shapely.ops import unary_union
    
    p25 = pct_25_km * 1000
    p50 = pct_50_km * 1000
    p75 = pct_75_km * 1000
    
    with rasterio.open(cost_distance_path) as src:
        cd = src.read(1).astype(np.float64)
        transform = src.transform
    
    nodata_mask = (cd <= 0) | (cd == -9999) | np.isinf(cd) | np.isnan(cd)
    
    contours = []
    for threshold, label, color in [(p25, '25%', '#ffffff'), (p50, '50%', '#ffca00'), (p75, '75%', '#ff6a1a')]:
        binary = np.zeros_like(cd, dtype=np.uint8)
        binary[(cd <= threshold) & (~nodata_mask)] = 1
        
        try:
            polys = []
            for geom, val in shapes(binary, transform=transform):
                if val == 1:
                    poly = shape(geom)
                    if poly.is_valid and not poly.is_empty:
                        polys.append(poly)
            
            if polys:
                merged = unary_union(polys)
                if merged.geom_type == 'MultiPolygon':
                    largest = max(merged.geoms, key=lambda g: g.area)
                else:
                    largest = merged
                
                largest = repair_geometry(largest)
                # Smooth the polygon: buffer out then back in to round jagged edges
                try:
                    smoothed = largest.buffer(0.001).buffer(-0.0008)
                    smoothed = repair_geometry(smoothed)
                    if smoothed is None or smoothed.is_empty:
                        smoothed = largest
                except Exception:
                    smoothed = largest
                # Then simplify to reduce coordinate count
                simplified = smoothed.simplify(0.0001, preserve_topology=True)
                if simplified.is_empty or not simplified.is_valid:
                    simplified = largest.simplify(0.0001, preserve_topology=True)
                
                # Get centroid for label placement
                centroid = simplified.centroid
                
                contours.append({
                    'type': 'Feature',
                    'properties': {
                        'percentile': label,
                        'threshold_m': threshold,
                        'color': color,
                        'label_lat': round(centroid.y, 5),
                        'label_lng': round(centroid.x, 5),
                    },
                    'geometry': round_coords(mapping(simplified))
                })
                print(f"    {label} contour: {len(simplified.exterior.coords)} vertices")
            else:
                print(f"    {label} contour: no polygons found")
        except Exception as e:
            print(f"    {label} contour error: {e}")
    
    return {'type': 'FeatureCollection', 'features': contours}

# ===============================================================================
# STEP 3: Isochrone extraction — time-based reachability contours
# ===============================================================================

# Default isochrone color ramp — progresses from cool (near IPP, short travel
# time) to warm (far from IPP, long travel time). The 6-color sequence covers
# the most operationally useful time horizons for WiSAR: 1h through 24h.
# Colors are chosen to remain distinguishable on both satellite and topo basemaps.
ISOCHRONE_COLORS = [
    '#00bcd4',   # 1h  — cyan (closest reachable area)
    '#4caf50',   # 2h  — green
    '#ffeb3b',   # 4h  — yellow
    '#ff9800',   # 8h  — orange
    '#f44336',   # 12h — red
    '#9c27b0',   # 24h — purple (outer containment boundary)
]


def extract_isochrone_polygons(cost_distance_path, base_speed_kmh, time_intervals_hours):
    """Extract time-based reachability contours (isochrones) as GeoJSON polygons.

    Converts the existing cost-distance raster (in terrain-equivalent meters)
    to travel time using the coordinator's specified flat-ground travel speed,
    then extracts vector polygons at each requested time interval. This is
    conceptually identical to Doherty et al.'s (2014) mobility model — the
    cost-distance surface represents minimum travel time under terrain friction,
    so each isochrone is the outer bound of where a subject *could* physically
    be after N hours at the given base speed.

    The conversion is: hours = cost_distance_meters / (speed_km/h * 1000)

    This works because the Dijkstra accumulates cost in terrain-equivalent
    meters — on flat ground with friction 1.0, the cost equals the actual
    surface distance in meters. Dividing by the user's flat-ground speed
    (converted to m/h) yields hours of travel time.

    Args:
        cost_distance_path: Path to cost-distance GeoTIFF (values in cost-meters)
        base_speed_kmh: Flat-ground travel speed in km/h (e.g., 1.61 for 1 mph)
        time_intervals_hours: List of time thresholds in hours (e.g., [1, 2, 4, 8])
    Returns:
        GeoJSON FeatureCollection with one polygon per isochrone interval
    """
    from rasterio.features import shapes
    from shapely.geometry import shape, mapping
    from shapely.ops import unary_union

    # Convert speed to meters per hour for unit compatibility with cost-distance
    speed_m_per_h = base_speed_kmh * 1000.0

    with rasterio.open(cost_distance_path) as src:
        cd = src.read(1).astype(np.float64)
        transform = src.transform

    nodata_mask = (cd <= 0) | (cd == -9999) | np.isinf(cd) | np.isnan(cd)

    # Convert cost-distance (meters) to travel time (hours).
    # Every cell now holds the minimum hours to reach it from the IPP
    # at the given base speed, modulated by terrain and land cover friction.
    travel_time = np.where(nodata_mask, np.nan, cd / speed_m_per_h)

    # Sort intervals so contours are extracted from smallest to largest
    sorted_intervals = sorted(time_intervals_hours)

    # Assign colors from the ramp, cycling if more intervals than colors
    contours = []
    for i, hours in enumerate(sorted_intervals):
        color = ISOCHRONE_COLORS[i % len(ISOCHRONE_COLORS)]
        # Label format: "1h", "2h", "4h", etc. — concise for map legends
        label = f"{hours}h"

        # Binary mask: 1 = reachable within this time threshold
        binary = np.zeros_like(cd, dtype=np.uint8)
        binary[(travel_time <= hours) & (~nodata_mask)] = 1

        try:
            polys = []
            for geom, val in shapes(binary, transform=transform):
                if val == 1:
                    poly = shape(geom)
                    if poly.is_valid and not poly.is_empty:
                        polys.append(poly)

            if polys:
                merged = unary_union(polys)
                # Keep only the largest contiguous polygon — small islands
                # are typically artifacts from narrow trail corridors that
                # briefly dip below the time threshold
                if merged.geom_type == 'MultiPolygon':
                    largest = max(merged.geoms, key=lambda g: g.area)
                else:
                    largest = merged

                largest = repair_geometry(largest)
                # Smooth jagged raster edges: buffer out then back in.
                # Same approach used for TARR contours in extract_contour_polygons.
                try:
                    smoothed = largest.buffer(0.001).buffer(-0.0008)
                    smoothed = repair_geometry(smoothed)
                    if smoothed is None or smoothed.is_empty:
                        smoothed = largest
                except Exception:
                    smoothed = largest
                # Simplify to reduce vertex count for CalTopo export and KML
                simplified = smoothed.simplify(0.0001, preserve_topology=True)
                if simplified.is_empty or not simplified.is_valid:
                    simplified = largest.simplify(0.0001, preserve_topology=True)

                centroid = simplified.centroid

                contours.append({
                    'type': 'Feature',
                    'properties': {
                        'hours': hours,
                        'label': label,
                        # Store the cost-distance threshold (meters) that corresponds
                        # to this time interval, useful for debugging and metadata
                        'threshold_m': hours * speed_m_per_h,
                        'color': color,
                        'label_lat': round(centroid.y, 5),
                        'label_lng': round(centroid.x, 5),
                    },
                    'geometry': round_coords(mapping(simplified))
                })
                print(f"    {label} isochrone: {len(simplified.exterior.coords)} vertices")
            else:
                print(f"    {label} isochrone: no polygons found (speed may be too slow or radius too small)")
        except Exception as e:
            print(f"    {label} isochrone error: {e}")

    return {'type': 'FeatureCollection', 'features': contours}


# ===============================================================================
# STEP 4: Isochrone analysis orchestrator
# ===============================================================================

def run_isochrone_analysis(ipp_lat, ipp_lng, base_speed_kmh, time_intervals_hours,
                          radius_km=10.0):
    """Run the full analysis pipeline for time-based isochrone mode.

    This is the second mode alongside TARR Analysis. It reuses the same
    data downloads, cost surface, and
    cost-distance computation — the only difference is how the output
    is interpreted. Instead of thresholding at Koester percentile distances,
    we convert cost-distance to travel time using the coordinator's specified
    base speed and extract isochrone polygons at requested time intervals.

    The coordinator supplies a flat-ground travel speed (e.g., 1 mph for an
    impaired subject) and time horizons (e.g., 1h, 2h, 4h). The output
    shows where the subject could physically be after each time period,
    accounting for terrain, trails, land cover, and slope — the same
    anisotropic cost model used for TARRs.

    Args:
        ipp_lat, ipp_lng: IPP coordinates in decimal degrees (WGS84)
        base_speed_kmh: Assumed flat-ground travel speed in km/h
        time_intervals_hours: List of time thresholds in hours
        radius_km: Analysis radius from IPP in km (default 10 km —
                   larger than TARR default because slow speeds over
                   long time horizons can cover surprising distance
                   along trails)
    Returns:
        Dict with paths to all intermediate files, contour GeoJSON, and bbox
    """
    print("=" * 60)
    print("WiSAR Isochrone Analysis Pipeline")
    print(f"  Base speed: {base_speed_kmh:.2f} km/h ({base_speed_kmh / 1.609:.2f} mph)")
    print(f"  Time intervals: {time_intervals_hours}")
    print("=" * 60)

    # --- Estimate required radius from speed and max time interval ---
    # The farthest an isochrone can extend is speed * max_hours on flat,
    # frictionless terrain. We add a 2 km pad for edge effects in the
    # cost-distance computation. This mirrors the TARR pipeline's
    # p75 + 2 km rule, keeping bbox sizing consistent across both modes.
    max_hours = max(time_intervals_hours)
    estimated_reach_km = base_speed_kmh * max_hours + 2.0
    # Use the larger of user-specified radius or estimated reach
    effective_radius = max(radius_km, estimated_reach_km)
    print(f"  Estimated max reach: {estimated_reach_km:.1f} km")
    print(f"  Effective analysis radius: {effective_radius:.1f} km")

    print("\n[1/7] Computing bounding box...")
    bbox = get_bbox_from_ipp(ipp_lat, ipp_lng, effective_radius)
    print(f"  Bbox: W={bbox[0]:.4f}, S={bbox[1]:.4f}, E={bbox[2]:.4f}, N={bbox[3]:.4f}")

    print("\n[2/7] Downloading DEM...")
    dem_path = download_dem(bbox)

    print("\n[3/7] Downloading NLCD...")
    nlcd_path = download_nlcd(bbox)

    print("\n[4/7] Downloading OSM features...")
    osm_features = download_osm_features(bbox)
    # Strip warnings from the osm_features dict so build_cost_surface sees
    # only the expected 'trails'/'roads'/'waterways'/'powerlines' keys.
    # The warnings are threaded up to the server response for the UI.
    osm_warnings = osm_features.pop('_warnings', [])

    print("\n[5/7] Downloading NHD hydrology...")
    nhd_features = download_nhd_features(bbox)

    print("\n[6/7] Building cost surface...")
    cost_path = build_cost_surface(dem_path, nlcd_path, osm_features, nhd_features=nhd_features)

    print("\n[7/7] Computing cost-distance...")
    cd_path = compute_cost_distance(cost_path, ipp_lat, ipp_lng, dem_path)

    # --- Compute Jacobs terrain-attractor masks (visualization layer) ---
    # Same hook as TARR mode — the Pure heatmap renders underneath the
    # isochrone polygons, so Travel Time analyses also need the masks.
    # See run_analysis() for rationale on the try/except wrapper.
    jacobs_masks_path = None
    try:
        print("\n[8] Computing Jacobs terrain-attractor masks...")
        jacobs_masks_path = compute_jacobs_masks(
            cost_distance_path=cd_path,
            dem_path=dem_path,
            osm_features=osm_features,
            nhd_features=nhd_features,
        )
    except Exception as e:
        print(f"  Jacobs mask computation failed (non-fatal): {e}")
        import traceback
        traceback.print_exc()

    # Extract isochrone polygons — no probability surface needed for this mode
    # since we're showing reachability, not Koester-based likelihood
    print("\n[9] Extracting isochrone polygons...")
    isochrone_geojson = extract_isochrone_polygons(cd_path, base_speed_kmh, time_intervals_hours)

    print("\nIsochrone analysis complete.")
    return {
        'bbox': bbox,
        'dem_path': dem_path,
        'nlcd_path': nlcd_path,
        'cost_surface_path': cost_path,
        'cost_distance_path': cd_path,
        # No probability_path — isochrone mode doesn't use Koester percentiles
        'probability_path': None,
        'work_dir': WORK_DIR,
        'contour_geojson': isochrone_geojson,
        'isochrone_mode': True,
        'base_speed_kmh': base_speed_kmh,
        'time_intervals_hours': time_intervals_hours,
        'warnings': osm_warnings,
        # Path to the 5-band Jacobs attractor masks GeoTIFF (see run_analysis)
        'jacobs_masks_path': jacobs_masks_path,
    }


# ===============================================================================
# STEP 4: TARR analysis orchestrator
# ===============================================================================

def run_analysis(ipp_lat, ipp_lng, pct_25_km, pct_50_km, pct_75_km, radius_km=5.0):
    print("=" * 60)
    print("WiSAR TARR Analysis Pipeline")
    print("=" * 60)
    print("\n[1/7] Computing bounding box...")
    bbox = get_bbox_from_ipp(ipp_lat, ipp_lng, radius_km)
    print(f"  Bbox: W={bbox[0]:.4f}, S={bbox[1]:.4f}, E={bbox[2]:.4f}, N={bbox[3]:.4f}")
    print("\n[2/7] Downloading DEM...")
    dem_path = download_dem(bbox)
    print("\n[3/7] Downloading NLCD...")
    nlcd_path = download_nlcd(bbox)
    print("\n[4/7] Downloading OSM features...")
    osm_features = download_osm_features(bbox)
    # Strip warnings from the osm_features dict before passing it into
    # build_cost_surface (which doesn't expect a '_warnings' key). The
    # warnings are threaded up to the server response so the UI can
    # surface them to the SAR coordinator.
    osm_warnings = osm_features.pop('_warnings', [])
    print("\n[5/7] Downloading NHD hydrology...")
    nhd_features = download_nhd_features(bbox)
    print("\n[6/7] Building cost surface...")
    cost_path = build_cost_surface(dem_path, nlcd_path, osm_features, nhd_features=nhd_features)
    print("\n[7/7] Computing cost-distance...")
    cd_path = compute_cost_distance(cost_path, ipp_lat, ipp_lng, dem_path)

    # --- Compute Jacobs terrain-attractor masks (visualization layer) ---
    # The Pure-heatmap renderer in server.py uses these masks to color each
    # pixel by its strongest terrain attractor signal per Jacobs (2015).
    # Wrapped in try/except so a mask-computation failure (missing NHD,
    # corrupt DEM) doesn't kill the whole analysis; the heatmap render
    # endpoint gracefully degrades to a cold surface if jacobs_masks_path
    # ends up None.
    jacobs_masks_path = None
    try:
        print("\n[8/8] Computing Jacobs terrain-attractor masks...")
        jacobs_masks_path = compute_jacobs_masks(
            cost_distance_path=cd_path,
            dem_path=dem_path,
            osm_features=osm_features,
            nhd_features=nhd_features,
        )
    except Exception as e:
        print(f"  Jacobs mask computation failed (non-fatal): {e}")
        import traceback
        traceback.print_exc()

    print("\n[9] Generating probability surface...")
    prob_path = generate_probability_surface(cd_path, pct_25_km, pct_50_km, pct_75_km)
    print("\n[10] Extracting TARR contour polygons...")
    contour_geojson = extract_contour_polygons(cd_path, pct_25_km, pct_50_km, pct_75_km)
    print("\nTARR analysis complete.")
    return {
        'bbox': bbox, 'dem_path': dem_path, 'nlcd_path': nlcd_path,
        'cost_surface_path': cost_path, 'cost_distance_path': cd_path,
        'probability_path': prob_path, 'work_dir': WORK_DIR,
        'contour_geojson': contour_geojson,
        'warnings': osm_warnings,
        # Path to the 5-band Jacobs attractor masks GeoTIFF used by the
        # heatmap renderer. May be None if mask computation failed; the
        # renderer handles that gracefully (cold surface).
        'jacobs_masks_path': jacobs_masks_path,
    }
