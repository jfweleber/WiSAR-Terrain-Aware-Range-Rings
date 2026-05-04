from flask import Flask, jsonify, request, send_from_directory, send_file
import urllib.request
import urllib.parse
import json
import os
import threading
import traceback
import base64
import hmac as hmac_mod
import hashlib
import time

app = Flask(__name__, static_folder='static')
analyses = {}
analysis_lock = threading.Lock()

# Coconino County calibration multipliers derived from Phase 2 validation
# (n=360 subjects, 253 missions). Each multiplier is the single value that
# minimizes total absolute error across p25/p50/p75 containment rates.
# Profiles with n<20 fall back to the global multiplier (1.40).
CALIBRATION_MULTIPLIERS = {
    'Hiker':          1.15,   # n=183
    'Skier (Alpine)': 2.15,   # n=36
    'Dementia':       2.55,   # n=29
    'Mental Illness': 4.05,   # n=25
    'Despondent':     1.65,   # n=21
    'Hunter':         0.80,   # n=21
    'Child (10-12)':  1.55,   # n=20
}
CALIBRATION_DEFAULT = 1.40
RESULTS_DIR = '/tmp/wisar_results'
os.makedirs(RESULTS_DIR, exist_ok=True)

# ============================================================
# CalTopo API write credentials (CCSO-SAR service account)
# ============================================================
CALTOPO_ACCOUNT_ID = 'N452G1'
CALTOPO_CREDENTIAL_ID = '1CUE13M39CBE'
CALTOPO_CREDENTIAL_KEY = '8Jqk7A97Fa8Kl5AjKf20RSx+RGQWJwCr8/wieujD+3Q='
CALTOPO_BASE_URL = 'https://caltopo.com'

def caltopo_sign(method, url_path, expires, payload_string):
    """Generate HMAC-SHA256 signature for a CalTopo API request."""
    message = f"{method} {url_path}\n{expires}\n{payload_string}"
    secret = base64.b64decode(CALTOPO_CREDENTIAL_KEY)
    signature = hmac_mod.new(secret, message.encode(), hashlib.sha256).digest()
    return base64.b64encode(signature).decode()

def caltopo_api_request(method, endpoint, payload=None):
    """Send an authenticated request to the CalTopo API.

    Wire format (v1.14): auth parameters (id, expires, signature) are always
    transmitted as URL query parameters. The JSON payload, when present,
    goes in the request body as form-encoded data under the 'json' key —
    NOT appended to the URL. This matches CalTopo's official reference
    implementations in their Team API documentation and lifts the practical
    URL-length limit that previously forced aggressive geometry simplification
    on large TARR / travel-time polygons (see server.py git history — the
    old URL-encoded path capped at roughly 500 vertices per feature before
    CalTopo would reject the request).

    Signature computation is unchanged: HMAC over the canonical string
    "{METHOD} {PATH}\\n{expires}\\n{payload_string}". The signature doesn't
    care where the payload is transmitted, only that it matches what
    CalTopo sees on arrival — which we control by sending the exact
    payload_string as the value of the body's 'json' key.

    For GET requests (or POST/PUT with no payload), the behavior is
    effectively unchanged — there's no body to send, so we issue a simple
    authenticated request against the URL.
    """
    expires = int((time.time() + 120) * 1000)
    payload_string = json.dumps(payload) if payload else ''
    signature = caltopo_sign(method, endpoint, expires, payload_string)

    # Auth params always go on the URL regardless of method.
    sep = '&' if '?' in endpoint else '?'
    url = (f"{CALTOPO_BASE_URL}{endpoint}{sep}"
           f"id={CALTOPO_CREDENTIAL_ID}"
           f"&expires={expires}"
           f"&signature={urllib.request.quote(signature, safe='')}")

    # For POST/PUT with a payload, send 'json=<payload>' as the body in
    # application/x-www-form-urlencoded format. urllib.parse.urlencode
    # handles the percent-encoding so the payload string round-trips
    # byte-for-byte to CalTopo's parser — critical for signature match.
    body_bytes = None
    headers = {'User-Agent': 'WiSAR-Decision-Support/0.1'}
    if payload_string and method.upper() in ('POST', 'PUT'):
        body_bytes = urllib.parse.urlencode({'json': payload_string}).encode('utf-8')
        headers['Content-Type'] = 'application/x-www-form-urlencoded'

    req = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=30) as response:
        body = response.read().decode()
        if body.strip():
            return json.loads(body)
        else:
            return {'status': 'ok', 'result': {}}

def save_result(analysis_id, result):
    path = os.path.join(RESULTS_DIR, analysis_id + '.json')
    with open(path, 'w') as f:
        json.dump(result, f)
    with analysis_lock:
        analyses[analysis_id] = result

def load_result(analysis_id):
    with analysis_lock:
        if analysis_id in analyses:
            return analyses[analysis_id]
    path = os.path.join(RESULTS_DIR, analysis_id + '.json')
    if os.path.exists(path):
        with open(path, 'r') as f:
            result = json.load(f)
        with analysis_lock:
            analyses[analysis_id] = result
        return result
    return None

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/api/caltopo/<map_id>')
def get_caltopo_data(map_id):
    try:
        url = f'https://caltopo.com/api/v1/map/{map_id}/since/0'
        req = urllib.request.Request(url, headers={'User-Agent': 'WiSAR-Decision-Support/0.1'})
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode())
        features = data.get('result', {}).get('state', {}).get('features', [])
        segments = [f for f in features if f.get('properties', {}).get('class') == 'Assignment']
        markers = [f for f in features if f.get('properties', {}).get('class') == 'Marker']
        ipp = None
        for m in markers:
            title = (m.get('properties', {}).get('title', '') or '').strip().upper()
            if title == 'IPP':
                coords = m.get('geometry', {}).get('coordinates', [])
                if len(coords) >= 2:
                    ipp = {'lat': coords[1], 'lng': coords[0], 'source': 'caltopo'}
                break
        return jsonify({'status':'ok','segment_count':len(segments),
            'segments':{'type':'FeatureCollection','features':segments},
            'ipp':ipp,'marker_count':len(markers)})
    except urllib.error.URLError as e:
        return jsonify({'status':'error','message':f'Could not reach CalTopo: {str(e)}'}), 502
    except Exception as e:
        return jsonify({'status':'error','message':str(e)}), 500

@app.route('/api/analyze', methods=['POST'])
def run_analysis_endpoint():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'status':'error','message':'No JSON data provided'}), 400
        ipp = data.get('ipp', {})
        ipp_lat = float(ipp.get('lat', 0))
        ipp_lng = float(ipp.get('lng', 0))
        if ipp_lat == 0 or ipp_lng == 0:
            return jsonify({'status':'error','message':'Invalid IPP coordinates'}), 400
        percentiles = data.get('percentiles', {})
        p25 = float(percentiles.get('p25', 0)) if percentiles else 0
        p50 = float(percentiles.get('p50', 0)) if percentiles else 0
        p75 = float(percentiles.get('p75', 0)) if percentiles else 0
        has_percentiles = p25 > 0 and p50 > 0 and p75 > 0
        if has_percentiles and (p25 >= p50 or p50 >= p75):
            return jsonify({'status':'error','message':'Percentiles must be three increasing positive values'}), 400
        # Apply Coconino calibration multiplier to Koester percentiles
        profile_name = data.get('profile', '')
        multiplier = 1.0
        if has_percentiles and profile_name:
            multiplier = CALIBRATION_MULTIPLIERS.get(profile_name, CALIBRATION_DEFAULT)
            p25 *= multiplier
            p50 *= multiplier
            p75 *= multiplier
            print(f"  Calibration: {profile_name} x{multiplier:.2f} -> p25={p25:.2f}, p50={p50:.2f}, p75={p75:.2f} km")
        if not has_percentiles:
            p25, p50, p75 = 1.0, 2.0, 3.0  # dummy values, won't be used
        mode = data.get('mode', 'ipp')
        radius_km = float(data.get('radius', 5000)) / 1000
        # CalTopo mode: bbox is union of segment extent + 1 km and IPP + p75 + 1 km
        # (computed in run_analysis). buffer_km here is just the segment padding.
        # IPP mode: user-specified radius
        if mode == 'caltopo':
            buffer_km = 1.0
        else:
            buffer_km = float(data.get('buffer', 2000)) / 1000
        segments_geojson = data.get('segments', None)
        from pipeline import run_analysis
        result = run_analysis(ipp_lat=ipp_lat, ipp_lng=ipp_lng,
            pct_25_km=p25, pct_50_km=p50, pct_75_km=p75,
            mode=mode, radius_km=radius_km, buffer_km=buffer_km,
            segments_geojson=segments_geojson)
        analysis_id = f"{ipp_lat:.4f}_{ipp_lng:.4f}"
        # Store percentiles in result for PNG renderer
        result['percentiles'] = {'p25': p25, 'p50': p50, 'p75': p75}
        save_result(analysis_id, result)
        import rasterio
        with rasterio.open(result['probability_path']) as src:
            bounds = src.bounds
        poa_results = result.get('poa_results', [])
        contour_geojson = result.get('contour_geojson', None)
        # Data-source warnings (e.g., OSM fell back to cache, or cache was
        # unavailable entirely). Default to empty list so the frontend can
        # always iterate over it without a null check.
        warnings = result.get('warnings', [])
        return jsonify({'status':'ok','analysis_id':analysis_id,
            'has_percentiles':has_percentiles,
            'calibration': {'profile': profile_name, 'multiplier': multiplier} if multiplier != 1.0 else None,
            'poa_results':poa_results,
            'contour_geojson':contour_geojson,
            'warnings':warnings,
            'bounds':{'west':bounds.left,'south':bounds.bottom,'east':bounds.right,'north':bounds.top},
            'cost_surface_url':f'/api/results/{analysis_id}/cost_surface.png',
            'percentiles_url':f'/api/results/{analysis_id}/percentiles.png',
            'cost_distance_url':f'/api/results/{analysis_id}/cost_distance.tif'})
    except Exception as e:
        traceback.print_exc()
        return jsonify({'status':'error','message':str(e)}), 500


# ============================================================
# Isochrone analysis endpoint — time-based reachability mode
#
# Unlike /api/analyze which uses Koester LPB percentiles to
# define TARR contours, this endpoint lets the SAR coordinator
# specify a flat-ground travel speed and time intervals. The
# pipeline runs identically through cost-distance, then converts
# the output to travel-time isochrones.
#
# Expected JSON payload:
#   {
#     "ipp": {"lat": 36.05, "lng": -112.14},
#     "speed": 1.61,              // km/h (flat ground)
#     "speed_unit": "kmh",        // "kmh" or "mph" — server normalizes to km/h
#     "intervals": [1, 2, 4, 8],  // hours
#     "radius": 10000             // meters (analysis extent from IPP)
#   }
# ============================================================
@app.route('/api/analyze-isochrone', methods=['POST'])
def run_isochrone_endpoint():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'status': 'error', 'message': 'No JSON data provided'}), 400

        # --- Parse and validate IPP coordinates ---
        ipp = data.get('ipp', {})
        ipp_lat = float(ipp.get('lat', 0))
        ipp_lng = float(ipp.get('lng', 0))
        if ipp_lat == 0 or ipp_lng == 0:
            return jsonify({'status': 'error', 'message': 'Invalid IPP coordinates'}), 400

        # --- Parse travel speed ---
        # Accept either km/h or mph; normalize to km/h internally.
        # Coordinators in the US typically think in mph, so the frontend
        # may send mph with a unit flag. 1 mph ≈ 1.609 km/h.
        speed = float(data.get('speed', 0))
        speed_unit = data.get('speed_unit', 'kmh').lower()
        if speed_unit == 'mph':
            base_speed_kmh = speed * 1.609344
        else:
            base_speed_kmh = speed
        if base_speed_kmh <= 0 or base_speed_kmh > 20:
            return jsonify({'status': 'error',
                            'message': 'Speed must be between 0 and 20 km/h (0-12.4 mph)'}), 400

        # --- Parse time intervals ---
        intervals = data.get('intervals', [])
        if not intervals or not isinstance(intervals, list):
            return jsonify({'status': 'error',
                            'message': 'Provide at least one time interval (hours)'}), 400
        # Sanitize: convert to floats, remove non-positive values, cap at 72h
        time_intervals = sorted(set(
            float(h) for h in intervals if float(h) > 0 and float(h) <= 72
        ))
        if not time_intervals:
            return jsonify({'status': 'error',
                            'message': 'No valid time intervals (must be 0-72 hours)'}), 400

        # --- Parse analysis radius ---
        radius_km = float(data.get('radius', 10000)) / 1000

        # --- Run the isochrone pipeline ---
        from pipeline import run_isochrone_analysis
        result = run_isochrone_analysis(
            ipp_lat=ipp_lat, ipp_lng=ipp_lng,
            base_speed_kmh=base_speed_kmh,
            time_intervals_hours=time_intervals,
            radius_km=radius_km
        )

        # Store result for subsequent tile/file requests using the same
        # analysis_id pattern as the TARR endpoint
        analysis_id = f"iso_{ipp_lat:.4f}_{ipp_lng:.4f}"
        result['isochrone_params'] = {
            'base_speed_kmh': round(base_speed_kmh, 4),
            'base_speed_mph': round(base_speed_kmh / 1.609344, 2),
            'intervals': time_intervals,
        }
        save_result(analysis_id, result)

        # Read bounds from the cost-distance raster for map fitting
        import rasterio
        with rasterio.open(result['cost_distance_path']) as src:
            bounds = src.bounds

        contour_geojson = result.get('contour_geojson', None)
        warnings = result.get('warnings', [])

        return jsonify({
            'status': 'ok',
            'analysis_id': analysis_id,
            'mode': 'isochrone',
            'isochrone_params': result['isochrone_params'],
            'contour_geojson': contour_geojson,
            'warnings': warnings,
            'bounds': {
                'west': bounds.left, 'south': bounds.bottom,
                'east': bounds.right, 'north': bounds.top
            },
            # Cost surface PNG is still useful for visual inspection
            'cost_surface_url': f'/api/results/{analysis_id}/cost_surface.png',
            'cost_distance_url': f'/api/results/{analysis_id}/cost_distance.tif',
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/results/<analysis_id>/<filename>')
def serve_result(analysis_id, filename):
    result = load_result(analysis_id)
    if not result:
        return jsonify({'status':'error','message':'Analysis not found'}), 404
    file_map = {'probability.tif':result.get('probability_path'),
        'cost_distance.tif':result.get('cost_distance_path'),
        'cost_surface.tif':result.get('cost_surface_path'),'dem.tif':result.get('dem_path')}
    filepath = file_map.get(filename)
    if not filepath or not os.path.exists(filepath):
        return jsonify({'status':'error','message':'File not found'}), 404
    return send_file(filepath, mimetype='image/tiff', as_attachment=True, download_name=filename)

@app.route('/api/results/<analysis_id>/cost_surface.png')
def serve_cost_png(analysis_id):
    result = load_result(analysis_id)
    if not result:
        return jsonify({'status':'error','message':'Analysis not found'}), 404
    cd_path = result.get('cost_distance_path')
    if not cd_path or not os.path.exists(cd_path):
        return jsonify({'status':'error','message':'File not found'}), 404
    try:
        import rasterio
        import numpy as np
        from PIL import Image
        import io
        import math
        with rasterio.open(cd_path) as src:
            data = src.read(1).astype(np.float64)
        nodata_mask = (data <= 0) | (data == -9999) | np.isinf(data) | np.isnan(data)
        height, width = data.shape
        # Retrieve percentiles from stored analysis result (in km, convert to meters)
        pct = result.get('percentiles', {})
        p25_m = float(pct.get('p25', 1.0)) * 1000
        p50_m = float(pct.get('p50', 2.0)) * 1000
        p75_m = float(pct.get('p75', 3.0)) * 1000
        # --- Percentile-band normalization ---
        # Map cost-distance to a 0-1 "priority" value based on which TARR
        # band the cell falls in. Each band gets an equal share of the color
        # ramp so that planners see meaningful differentiation across the
        # entire search area, not just a hot spot at the IPP.
        #
        # Band mapping (priority 1.0 = highest, 0.0 = lowest):
        #   0 to p25       -> 1.0 down to 0.67  (hottest third: red/orange)
        #   p25 to p50     -> 0.67 down to 0.33  (middle third: yellow/green)
        #   p50 to p75     -> 0.33 down to 0.0   (coolest third: teal/blue)
        #   beyond p75     -> 0.0                 (fades to transparent)
        #
        # Within each band, priority decreases linearly with cost-distance.
        # This ensures monotonic decay from IPP outward with no cold spot
        # at the origin (unlike raw log-normal PDF which goes to zero at d=0).
        safe_data = np.where(nodata_mask, p75_m + 1, data)
        norm = np.zeros_like(safe_data)
        # Band 1: inside p25 (priority 1.0 -> 0.67)
        in_p25 = safe_data <= p25_m
        norm = np.where(in_p25,
            1.0 - (safe_data / max(p25_m, 1)) * 0.33,
            norm)
        # Band 2: p25 to p50 (priority 0.67 -> 0.33)
        in_p50 = (safe_data > p25_m) & (safe_data <= p50_m)
        norm = np.where(in_p50,
            0.67 - ((safe_data - p25_m) / max(p50_m - p25_m, 1)) * 0.34,
            norm)
        # Band 3: p50 to p75 (priority 0.33 -> 0.0)
        in_p75 = (safe_data > p50_m) & (safe_data <= p75_m)
        norm = np.where(in_p75,
            0.33 - ((safe_data - p50_m) / max(p75_m - p50_m, 1)) * 0.33,
            norm)
        # Beyond p75: floor at 0
        norm = np.clip(norm, 0, 1)
        norm[nodata_mask] = 0
        rgba = np.zeros((height, width, 4), dtype=np.uint8)
        # Red (high priority) -> Orange -> Yellow -> Green -> Teal -> Blue (low)
        stops = [(0.0,  30, 80,180),  (0.08, 43,108,196), (0.17, 46,140,160),
                 (0.25, 46,165,120),  (0.33, 60,185, 80), (0.42, 130,205,50),
                 (0.52, 200,210, 35), (0.63, 235,180, 30), (0.75, 240,120,30),
                 (0.87, 232, 70, 38), (1.0,  210, 45, 35)]
        r_arr = np.full_like(norm, 30.0)
        g_arr = np.full_like(norm, 80.0)
        b_arr = np.full_like(norm, 180.0)
        for i in range(len(stops)-1):
            t0, r0, g0, b0 = stops[i]
            t1, r1, g1, b1 = stops[i+1]
            mask = (norm >= t0) & (norm < t1) if i < len(stops)-2 else (norm >= t0) & (norm <= t1)
            frac = np.where(mask, (norm - t0) / (t1 - t0), 0)
            r_arr = np.where(mask, r0 + frac * (r1 - r0), r_arr)
            g_arr = np.where(mask, g0 + frac * (g1 - g0), g_arr)
            b_arr = np.where(mask, b0 + frac * (b1 - b0), b_arr)
        rgba[:,:,0] = r_arr.clip(0,255).astype(np.uint8)
        rgba[:,:,1] = g_arr.clip(0,255).astype(np.uint8)
        rgba[:,:,2] = b_arr.clip(0,255).astype(np.uint8)
        # --- Travel corridor highlighting ---
        # Read the cost surface (friction) raster and identify cells where
        # friction == 1.0, which are trails, roads, power line ROWs, and
        # developed open space burned in during cost_surface.py. Blend these
        # cells toward white to make corridors visually pop as brighter lines
        # through the priority heatmap. This gives planners an immediate read
        # on where easy-travel corridors intersect each priority band.
        cs_path = result.get('cost_surface_path')
        print(f"  Corridor debug: cs_path={cs_path}, exists={os.path.exists(cs_path) if cs_path else 'N/A'}")
        if cs_path and os.path.exists(cs_path):
            with rasterio.open(cs_path) as cs_src:
                friction = cs_src.read(1)
            print(f"  Corridor debug: friction shape={friction.shape}, dtype={friction.dtype}, data shape={data.shape}")
            print(f"  Corridor debug: friction min={np.nanmin(friction):.4f}, max={np.nanmax(friction):.4f}")
            print(f"  Corridor debug: friction==1.0 count={int(np.sum(friction == 1.0))}, abs<0.01 count={int(np.sum(np.abs(friction - 1.0) < 0.01))}")
            print(f"  Corridor debug: nodata_mask True count={int(np.sum(nodata_mask))}")
            if friction.shape == data.shape:
                corridor_mask = (np.abs(friction - 1.0) < 0.01) & (~nodata_mask)
                corridor_count = int(np.sum(corridor_mask))
                print(f"  Corridor highlighting: {corridor_count} cells at friction~=1.0")
                if corridor_count > 0:
                    # Blend toward white: mix 35% white into the existing color
                    blend = 0.55
                    rgba[corridor_mask, 0] = (rgba[corridor_mask, 0].astype(np.float64) * (1 - blend) + 255 * blend).clip(0, 255).astype(np.uint8)
                    rgba[corridor_mask, 1] = (rgba[corridor_mask, 1].astype(np.float64) * (1 - blend) + 255 * blend).clip(0, 255).astype(np.uint8)
                    rgba[corridor_mask, 2] = (rgba[corridor_mask, 2].astype(np.float64) * (1 - blend) + 255 * blend).clip(0, 255).astype(np.uint8)
            else:
                print(f"  Corridor highlighting skipped: shape mismatch cd={data.shape} cs={friction.shape}")
        else:
            print(f"  Corridor highlighting skipped: path missing or not found")
        # Alpha: full opacity in main area, fade beyond p75
        base_alpha = 170
        alpha = np.where(nodata_mask, 0, base_alpha).astype(np.float64)
        beyond_p75 = (data > p75_m) & (~nodata_mask)
        fade = np.clip(1.0 - (data - p75_m) / (p75_m * 0.8), 0.15, 1.0)
        alpha = np.where(beyond_p75, alpha * fade, alpha)
        rgba[:,:,3] = alpha.clip(0,255).astype(np.uint8)
        img = Image.fromarray(rgba, 'RGBA')
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        buf.seek(0)
        return send_file(buf, mimetype='image/png')
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'status':'error','message':str(e)}), 500

@app.route('/api/results/<analysis_id>/terrain.png')
def serve_terrain_png(analysis_id):
    result = load_result(analysis_id)
    if not result:
        return jsonify({'status':'error','message':'Analysis not found'}), 404
    nlcd_path = result.get('nlcd_path')
    cost_path = result.get('cost_surface_path')
    dem_path = result.get('dem_path')
    if not cost_path or not os.path.exists(cost_path):
        return jsonify({'status':'error','message':'File not found'}), 404
    try:
        import rasterio
        import numpy as np
        from PIL import Image
        import io
        from scipy.signal import convolve2d
        import math
        # Read DEM and compute slope for terrain difficulty
        with rasterio.open(cost_path) as src:
            friction = src.read(1).astype(np.float64)
            transform = src.transform
            height, width = friction.shape
        with rasterio.open(dem_path) as src:
            dem = src.read(1).astype(np.float64)
            if dem.shape != (height, width):
                from rasterio.warp import reproject, Resampling
                dem2 = np.zeros((height, width), dtype=np.float64)
                reproject(source=rasterio.band(src, 1), destination=dem2,
                    src_transform=src.transform, src_crs=src.crs,
                    dst_transform=transform, dst_crs=src.crs, resampling=Resampling.bilinear)
                dem = dem2
        dem[dem < -1000] = np.nan
        dem[dem > 10000] = np.nan
        center_lat = (transform[5] + transform[5] + transform[4] * height) / 2
        cx = abs(transform[0]) * 111320 * math.cos(math.radians(center_lat))
        cy = abs(transform[4]) * 110540
        kx = np.array([[-1,0,1],[-2,0,2],[-1,0,1]]) / (8.0 * cx)
        ky = np.array([[-1,-2,-1],[0,0,0],[1,2,1]]) / (8.0 * cy)
        dzdx = convolve2d(dem, kx, mode='same', boundary='symm')
        dzdy = convolve2d(dem, ky, mode='same', boundary='symm')
        slope_deg = np.degrees(np.arctan(np.sqrt(dzdx**2 + dzdy**2)))
        # Combine slope and friction into difficulty score 0-100
        # Slope component: 0 deg=0, 15 deg=30, 30 deg=60, 45 deg=90
        slope_score = np.clip(slope_deg * 2.0, 0, 90)
        # Friction component: 1.0=0, 1.5=25, 3.0=50, 50=95
        fric_score = np.clip((friction - 1.0) * 20.0, 0, 95)
        # Combined: max of both (terrain is as hard as its hardest component)
        difficulty = np.maximum(slope_score, fric_score)
        nodata_mask = np.isnan(dem) | (friction <= 0) | (friction == -9999)
        # Normalize 0-100 to 0-1
        norm = difficulty / 100.0
        norm = np.clip(norm, 0, 1)
        rgba = np.zeros((height, width, 4), dtype=np.uint8)
        stops = [(0.0, 20,140,40), (0.1, 50,175,50), (0.2, 100,200,45),
                 (0.3, 160,210,30), (0.4, 210,215,15), (0.5, 240,195,0),
                 (0.6, 245,150,10), (0.7, 235,100,15), (0.8, 215,55,12),
                 (0.9, 185,25,10), (1.0, 140,12,10)]
        r_arr = np.full_like(norm, 140.0)
        g_arr = np.full_like(norm, 12.0)
        b_arr = np.full_like(norm, 10.0)
        for i in range(len(stops)-1):
            t0, r0, g0, b0 = stops[i]
            t1, r1, g1, b1 = stops[i+1]
            mask = (norm >= t0) & (norm < t1) if i < len(stops)-2 else (norm >= t0) & (norm <= t1)
            frac = np.where(mask, (norm - t0) / (t1 - t0), 0)
            r_arr = np.where(mask, r0 + frac * (r1 - r0), r_arr)
            g_arr = np.where(mask, g0 + frac * (g1 - g0), g_arr)
            b_arr = np.where(mask, b0 + frac * (b1 - b0), b_arr)
        rgba[:,:,0] = r_arr.clip(0,255).astype(np.uint8)
        rgba[:,:,1] = g_arr.clip(0,255).astype(np.uint8)
        rgba[:,:,2] = b_arr.clip(0,255).astype(np.uint8)
        rgba[:,:,3] = np.where(nodata_mask, 0, 150).astype(np.uint8)
        img = Image.fromarray(rgba, 'RGBA')
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        buf.seek(0)
        return send_file(buf, mimetype='image/png')
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'status':'error','message':str(e)}), 500

@app.route('/api/results/<analysis_id>/percentiles.png')
def serve_percentile_png(analysis_id):
    result = load_result(analysis_id)
    if not result:
        return jsonify({'status':'error','message':'Analysis not found'}), 404
    prob_path = result.get('probability_path')
    if not prob_path or not os.path.exists(prob_path):
        return jsonify({'status':'error','message':'File not found'}), 404
    try:
        import rasterio
        import numpy as np
        from PIL import Image, ImageDraw
        import io
        with rasterio.open(prob_path) as src:
            data = src.read(1)
        height, width = data.shape
        rgba = np.zeros((height, width, 4), dtype=np.uint8)
        # Semi-transparent filled zones
        rgba[data == 4] = [220, 38, 38, 100]
        rgba[data == 3] = [245, 158, 11, 80]
        rgba[data == 2] = [250, 204, 21, 60]
        # Draw contour lines at zone boundaries
        for zone_val, color in [(4, [255,255,255,220]), (3, [255,200,50,200]), (2, [255,100,30,200])]:
            mask = (data >= zone_val).astype(np.uint8)
            # Edge detection: find boundary pixels
            kernel_h = np.abs(np.diff(mask, axis=1))
            kernel_v = np.abs(np.diff(mask, axis=0))
            edge = np.zeros_like(mask)
            edge[:, :-1] |= kernel_h
            edge[:, 1:] |= kernel_h
            edge[:-1, :] |= kernel_v
            edge[1:, :] |= kernel_v
            # Thicken the line (2px)
            # Smooth contour lines (1px dilation for anti-aliasing)
            from scipy.ndimage import binary_dilation
            struct = np.array([[0,1,0],[1,1,1],[0,1,0]], dtype=bool)
            edge = binary_dilation(edge, structure=struct, iterations=1).astype(np.uint8)
            edge_mask = edge == 1
            rgba[edge_mask, 0] = color[0]
            rgba[edge_mask, 1] = color[1]
            rgba[edge_mask, 2] = color[2]
            rgba[edge_mask, 3] = color[3]
        img = Image.fromarray(rgba, 'RGBA')
        from PIL import ImageDraw, ImageFont
        draw = ImageDraw.Draw(img)
        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 12)
        except:
            font = ImageFont.load_default()
        label_offsets = {4: -20, 3: 0, 2: 20}
        for zone_val, label, lcolor in [(4, '25%', (255,255,255)), (3, '50%', (255,220,80)), (2, '75%', (255,120,50))]:
            zmask = (data >= zone_val).astype(np.uint8)
            kh2 = np.abs(np.diff(zmask, axis=1))
            epts = np.zeros((height, width), dtype=np.uint8)
            epts[:, :-1] |= kh2
            target_row = height // 2 + label_offsets[zone_val]
            row_start = max(0, target_row - 5)
            row_end = min(height, target_row + 5)
            row_edge = epts[row_start:row_end, :]
            ys, xs = np.where(row_edge > 0)
            if len(xs) > 0:
                lx = int(np.max(xs)) + 5
                ly = target_row - 6
            else:
                continue
            lx = min(lx, width - 30)
            ly = max(ly, 2)
            draw.rounded_rectangle([lx-2, ly-1, lx+26, ly+13], radius=2, fill=(0,0,0,160))
            draw.text((lx, ly), label, fill=lcolor+(255,), font=font)
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        buf.seek(0)
        return send_file(buf, mimetype='image/png')
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'status':'error','message':str(e)}), 500

@app.route('/api/caltopo/export-tarrs', methods=['POST'])
def export_tarrs_to_caltopo():
    """Push contour polygons to a CalTopo map as named Shape features.

    Handles both TARR contours (percentile-based) and travel-time
    isochrones (hours-based) from the same endpoint. The feature type is
    detected per-feature by checking whether the GeoJSON properties
    include an 'hours' field (present only on isochrones) — this matches
    the same detection pattern used by the frontend for KML/GeoJSON
    downloads, so a CalTopo export carries the same semantic labeling
    the coordinator sees in the locally-downloaded files.

    The endpoint URL still says 'export-tarrs' for backward compatibility
    with any existing frontend or bookmarklet callers; a future release
    can rename to 'export-contours' alongside the corresponding app.js
    update. The handler logic is mode-agnostic.
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'status': 'error', 'message': 'No JSON data provided'}), 400
        map_id = data.get('map_id', '').strip()
        contours = data.get('contours', {})
        if not map_id:
            return jsonify({'status': 'error', 'message': 'No CalTopo map ID provided'}), 400
        features = contours.get('features', [])
        if not features:
            return jsonify({'status': 'error', 'message': 'No contour features to export'}), 400

        # Detect mode from the first feature. All features in a single export
        # come from one analysis run and share the same mode. We use the
        # presence of the 'hours' property as the discriminator because
        # that's the field run_isochrone_analysis uniquely sets and the
        # frontend uses the same check elsewhere (KML/GeoJSON download).
        first_props = features[0].get('properties', {}) if features else {}
        is_isochrone = 'hours' in first_props

        # Human-readable noun for log messages and the response summary —
        # 'TARR' for percentile contours, 'travel-time contour' for isochrones.
        export_kind = 'travel-time contour' if is_isochrone else 'TARR'

        results = []
        for feature in features:
            props = feature.get('properties', {})
            geom = feature.get('geometry', {})
            color = props.get('color', '#ffffff')

            # Per-feature display label used in the CalTopo title, log lines,
            # and the results array. Different source fields depending on
            # what kind of contour this is.
            if is_isochrone:
                # Isochrones: prefer the pre-formatted 'label' (e.g. "4h")
                # set by the pipeline; fall back to constructing it from
                # 'hours' if label is missing for any reason.
                hours = props.get('hours')
                feature_label = props.get('label') or (f'{hours}h' if hours is not None else 'unknown')
                caltopo_title = f'Travel Time: {feature_label}'
                # Description contextualizes the contour for a SAR user who
                # sees it in CalTopo without the WiSAR tool open. We don't
                # include the coordinator-specified speed here because that
                # parameter is per-analysis, not per-feature, and isn't on
                # the GeoJSON payload — it's only on the pipeline's result dict.
                caltopo_description = (
                    f'{feature_label} travel-time isochrone\n'
                    f'Area reachable within {feature_label} at the coordinator-specified flat-ground speed. '
                    f'Actual travel time adjusts for terrain (Tobler slope function), land cover (NLCD), '
                    f'and available trail/road corridors (OSM).\n'
                    f'Generated by WiSAR Decision Support Tool'
                )
            else:
                # TARRs: use the existing percentile-based labeling. Keep the
                # description format identical to v1.12 — field reports and
                # screenshots in the wild reference this wording.
                feature_label = props.get('percentile', '')
                threshold_km = (props.get('threshold_m', 0) / 1000)
                caltopo_title = f'TARR {feature_label}'
                caltopo_description = (
                    f'{feature_label} Terrain-Aware Range Ring\n'
                    f'Cost-distance threshold: {threshold_km:.2f} km\n'
                    f'Generated by WiSAR Decision Support Tool'
                )

            # No geometry simplification (v1.14): body-based POST to CalTopo
            # has no practical size limit, so we can send the full-fidelity
            # polygon directly. The previous URL-encoded path forced aggressive
            # simplification to fit under CalTopo's ~16KB URL cap, which erased
            # detail on large travel-time isochrones and TARRs.
            #
            # We still round coordinates to 5 decimal places (~1.1m) because
            # that's well below both SAR display precision and the underlying
            # 30m pipeline grid resolution, and it cuts the JSON payload by
            # roughly 40% with no visible accuracy loss.
            try:
                def _round_coords(c):
                    if isinstance(c[0], (list, tuple)):
                        return [_round_coords(x) for x in c]
                    return [round(v, 5) for v in c]
                if geom.get('coordinates') is not None:
                    geom = dict(geom)
                    geom['coordinates'] = _round_coords(geom['coordinates'])
            except Exception as e:
                print(f"  Coordinate rounding warning for {feature_label}: {e}")

            shape_payload = {
                'type': 'Feature',
                'properties': {
                    'class': 'Shape',
                    'title': caltopo_title,
                    'description': caltopo_description,
                    'stroke': color, 'stroke-width': 3, 'stroke-opacity': 0.9,
                    'fill': color, 'fill-opacity': 0.08,
                },
                'geometry': geom
            }
            try:
                resp = caltopo_api_request('POST', f'/api/v1/map/{map_id}/Shape', shape_payload)
                # 'label' key covers both TARR percentiles and isochrone hours;
                # frontend only reads .status for success counting, so the
                # change from 'percentile' is safe.
                results.append({'label': feature_label, 'status': 'ok',
                                'id': resp.get('result', {}).get('id', 'unknown')})
                print(f"  Exported {export_kind} {feature_label} to CalTopo map {map_id}")
            except Exception as e:
                results.append({'label': feature_label, 'status': 'error', 'message': str(e)})
                print(f"  Failed to export {export_kind} {feature_label}: {e}")
        ok_count = sum(1 for r in results if r['status'] == 'ok')
        # Message reflects what was actually exported — mentions the kind so
        # the UI's success notice reads correctly for either mode.
        noun_plural = 'travel-time contours' if is_isochrone else 'TARRs'
        return jsonify({'status': 'ok',
                        'message': f'Exported {ok_count}/{len(features)} {noun_plural} to CalTopo',
                        'results': results})
    except Exception as e:
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/caltopo/update-segments', methods=['POST'])
def update_segments_on_caltopo():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'status': 'error', 'message': 'No JSON data provided'}), 400
        map_id = data.get('map_id', '').strip()
        segments = data.get('segments', [])
        if not map_id:
            return jsonify({'status': 'error', 'message': 'No CalTopo map ID provided'}), 400
        if not segments:
            return jsonify({'status': 'error', 'message': 'No segments to update'}), 400
        results = []
        for seg in segments:
            seg_id = seg.get('id', '')
            title = seg.get('title', 'Unknown')
            rank = seg.get('rank', 0)
            poa = seg.get('poa', 0)
            cum_poa = seg.get('cumulative_poa', 0)
            if not seg_id:
                results.append({'title': title, 'status': 'error', 'message': 'No CalTopo feature ID'})
                continue
            description = f'POA Rank #{rank} — {poa:.1f}%\nCumulative POA: {cum_poa:.1f}%\nRanked by WiSAR Decision Support Tool'
            update_payload = {
                'type': 'Feature', 'id': seg_id,
                'properties': {'class': 'Assignment', 'description': description}
            }
            try:
                resp = caltopo_api_request('POST', f'/api/v1/map/{map_id}/Assignment/{seg_id}', update_payload)
                results.append({'title': title, 'status': 'ok', 'rank': rank})
                print(f"  Updated segment '{title}' (#{rank}, {poa:.1f}%) on CalTopo map {map_id}")
            except Exception as e:
                results.append({'title': title, 'status': 'error', 'message': str(e)})
                print(f"  Failed to update segment '{title}': {e}")
        ok_count = sum(1 for r in results if r['status'] == 'ok')
        return jsonify({'status': 'ok', 'message': f'Updated {ok_count}/{len(segments)} segments on CalTopo', 'results': results})
    except Exception as e:
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
