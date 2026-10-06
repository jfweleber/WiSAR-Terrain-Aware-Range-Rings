# WiSAR Decision Support Tool

**A Terrain-Aware Planning Aid for Wilderness Search and Rescue (v1.18)**

A web-based spatial analysis tool for Wilderness SAR operations. Instead of drawing simple Euclidean distance rings around an Initial Planning Point (IPP), it builds an anisotropic cost surface from elevation, land cover, hydrology, and OSM linear features, then traces contours of equal travel cost across real terrain, compressing against steep slopes, dense forest, and water barriers, while expanding along trails and valleys where a person can move easily. The cost surface drives both Terrain-Aware Range Rings (TARRs) at Koester find-distance percentiles and travel-time isochrones at user-specified time bands, with KML/GeoJSON exports and a write-back path to CalTopo for any SAR team's own maps.

**Live site:** [https://sar.weleber.net](https://sar.weleber.net)

![TARR Example — West Fork of Oak Creek](app/tarr_example_v1_15.png)
---

## What this tool does

Given an IPP (the point where a lost person was last seen) and either a subject profile (hiker, child, dementia patient, etc.) or a travel speed, the tool:

1. **Gathers geospatial data** — elevation (USGS 3DEP) is fetched live, with the staged USGS elevation tiles as an automatic backup if that service is down; everything else comes from snapshots on the server: land cover (Annual NLCD 2024, CONUS, refreshed yearly), hydrography (USGS NHDPlus High Resolution, every basin, refreshed quarterly), and trails, roads, waterways, and power lines (OpenStreetMap, all 50 states and DC, rebuilt weekly from Geofabrik extracts).
2. **Builds a friction surface** — each 30m cell gets a cost multiplier based on land cover type, calibrated to off-trail speed literature (Imhof 1950). Trails, roads, and power line corridors are burned in at friction 1.0; water features from NHD and OSM act as high-impedance barriers.
3. **Computes anisotropic cost-distance** — Dijkstra's algorithm with per-edge Tobler's Hiking Function, cross-slope penalty, and 3D surface distance.
4. **Applies per-band calibration** — Coconino County calibration multipliers (M25, M50, M75) scale each percentile threshold independently to correct the nonlinear contraction of TARRs in rugged terrain.
5. **Computes terrain-attractor masks** — five Jacobs (2015) PDEN categories per pixel (stream-trail intersections, trails, low-elevation pockets, streams, high-elevation prominence). These drive within-envelope color in the heatmap so that planners see not just where a subject can reach, but where they statistically tend to be found.
6. **Generates output contours** — either Lost Person Behavior percentile contours (Koester 2008, "TARR Analysis mode") or time-based reachability contours at user-selected intervals ("Travel Time mode").

## Key features

- **Two analysis modes:** TARR Analysis (Koester-driven percentile envelope around an IPP) and Travel Time (reachability isochrones at a given travel speed).
- **Jacobs-driven heatmap (optional layer):** within-envelope color reflects per-pixel terrain-attractor strength per Jacobs (2015), with stream-trail intersections rendering hottest (their strongest PDEN finding), trails next, then low-elevation pockets, streams, and high-elevation prominence. Renders at full opacity across the entire search area, including past the 75th percentile, where roughly 1-in-4 finds still occur. Off by default since v1.16; enable it from the Map Layers toggles.
- **28 subject categories** from Lost Person Behavior (Koester 2008) with eco-region and terrain selectors.
- **Per-band calibration** — profile-specific multipliers at each percentile threshold, validated against 362 historical subjects from 253 Coconino County missions, re-measured September 2026 on the current snapshot data sources.
- **CalTopo write-back, any team:** push TARR contours or travel-time isochrones to a CalTopo map as named Shape features. The default option uses credentials stored on the server; other teams enter their own CalTopo API credentials in the UI and the tool passes them through without storing.
- **Predictable OSM data** — trails, roads, waterways, and power lines are read from a weekly-refreshed local snapshot covering all 50 states and DC, built from Geofabrik extracts. No dependency on public Overpass servers; the tool warns if the snapshot is more than 14 days old.
- **KML and GeoJSON export** of TARR or travel-time contours for CalTopo, Google Earth, TAK/CloudTAK, QGIS, and Avenza.
- **GeoTIFF downloads** of cost-distance, cost surface, and probability rasters.

## Calibration

Applying Euclidean-derived find-distance statistics (Koester 2008) as cost-distance thresholds systematically contracts TARR contours because terrain friction inflates effective travel distance. The contraction is nonlinear; outer contours are more compressed than inner ones as friction accumulates over longer paths.

The tool corrects this with per-band multipliers derived from Coconino County historical data. Profiles with n≥20 historical subjects receive profile-specific multipliers; remaining profiles use a global default (M25=1.05, M50=1.35, M75=1.80). Containment rates against historical find locations:

| Percentile | Nominal | Uncalibrated | Global per-band | Per-profile per-band |
|-----------|---------|--------------|-----------------|----------------------|
| 25th      | 25.0%   | 28.7%        | 28.7%           | 27.9%                |
| 50th      | 50.0%   | 45.0%        | 52.2%           | 50.8%                |
| 75th      | 75.0%   | 63.8%        | 77.6%           | 78.7%                |

Re-measured September 30, 2026 against the current NLCD, NHDPlus HR and OSM snapshots (362 subjects; the April 2026 per-profile figures were 26.2% / 50.0% / 77.1%). The tool applies the per-profile per-band multipliers.

Full validation details, including per-profile multiplier tables and per-profile containment rates, are available in the tool's Validation modal.

## Architecture

```
app/
├── server.py              Flask web server, API endpoints, PNG renderers
├── pipeline/              Analysis pipeline (modular package)
│   ├── __init__.py        Public API re-exports
│   ├── shared.py          Constants, utilities, bbox functions
│   ├── downloads.py       Data acquisition (DEM live; NLCD, NHD, OSM from snapshots)
│   ├── dem_fallback.py    Backup DEM source: staged USGS tiles, read only if 3DEP fails
│   ├── osm_cache.py       Local OSM snapshot reader (weekly Geofabrik refresh)
│   ├── nlcd_cache.py      Local NLCD snapshot reader (yearly MRLC refresh)
│   ├── nhd_cache.py       Local NHDPlus HR snapshot reader (quarterly USGS refresh)
│   ├── cost_surface.py    Friction surface construction
│   ├── cost_distance.py   Dijkstra anisotropic cost-distance
│   ├── jacobs_masks.py    Terrain-attractor masks per Jacobs (2015)
│   └── outputs.py         Probability surfaces, TARR contours, isochrones
├── static/
│   ├── index.html         Single-page Leaflet.js frontend, modals, accordion UI
│   └── app.js             Application logic, calibration, CalTopo integration
└── tools/
    ├── build_osm_cache.py    Weekly OSM cache builder (memory-bounded Arrow streaming)
    ├── build_nlcd_cache.py   Yearly NLCD snapshot installer
    ├── build_hydro_cache.py  Quarterly NHDPlus HR snapshot builder (ogr2ogr streaming)
    ├── compare_sources.py    Live-vs-snapshot regression check for one IPP
    └── prune_runs.py         Analysis-store retention: rasters 180 days, manifests and contours kept
```

## Saved analyses

Every analysis is stored on the server in its own directory under
`/var/www/sar.weleber.net/runs/<analysis_id>/`: the rasters, the contour
polygons (`contours.geojson`), and `manifest.json` with the inputs as
posted, the calibration multipliers, the versions of the OSM, NLCD and NHD
snapshots used, which elevation source answered, and per-step timings.
The id is `<UTC stamp>_<tarr|iso>_<lat>_<lng>_<random>`; the random suffix
makes the link to an analysis the credential for reopening it, since the
tool has no login. `GET /api/analyses/<id>` returns the same JSON as the
analyze endpoints, and `/?analysis=<id>` reopens the result in the UI on
any device. The browser keeps its own list of analyses it has run or
opened. Rasters are removed after 180 days by `tools/prune_runs.py`;
manifests and contours are kept and backed up nightly.

## Data sources

| Data | Source | Resolution |
|------|--------|-----------|
| Elevation | USGS 3DEP ImageServer; staged USGS 1 arc-second tiles if it is unavailable | 30m |
| Land cover | Annual NLCD 2024 (local CONUS snapshot) | 30m |
| Trails, roads, power lines | OpenStreetMap (weekly local snapshot, all 50 states + DC) | Vector |
| Hydrology | USGS NHDPlus High Resolution, 1:24k (local snapshot) — waterbodies, area features, flowlines with Strahler order | Vector |
| Subject profiles | Koester (2008), via Ferguson (2013) IGT4SAR | Statistical |
| Terrain attractor weights | Jacobs (2015) PDEN findings | Per-feature |
| Calibration | Coconino County historical missions (360 subjects, 253 missions) | Per-profile |

## Methodology

The cost-distance computation combines four factors per cell transition:

- **Tobler's Hiking Function** (directional slope cost)
- **Land cover friction** (calibrated to Imhof 1950 off-trail speed reduction)
- **Cross-slope penalty** (lateral traversal difficulty)
- **3D surface distance** (Pythagorean with elevation change)

Friction multipliers range from 1.0 (trail/road/power line corridor) to 1.80 (evergreen forest) to 50.0 (water barrier). Power line rights-of-way are buffered at ~40m to represent cleared corridors. The full friction table and methodology are documented in the tool's Metadata modal.

Calibration multipliers are applied on the frontend before percentile distances are sent to the analysis pipeline. Each percentile (p25, p50, p75) receives its own multiplier, correcting the nonlinear contraction where terrain friction accumulates more over longer travel paths. The cost surface and cost-distance computation are unaffected; calibration adjusts only the statistical thresholds, not the terrain model.

The heatmap underneath the TARR contours uses a separate visualization layer driven by Matt Jacobs's (2015) PDEN framework. Each pixel is scored by the strongest applicable terrain attractor among five categories: stream-trail intersections (weight 1.00, Jacobs's strongest empirical finding at ~10x PDEN), trails and other linear corridors (0.55), low-elevation pockets (0.35), stream proximity (0.28), and high-elevation prominence (0.18). Scores are taken as the maximum across applicable masks (not summed), matching the structure of Jacobs's findings as observations of distinct cell categories rather than additive lifts. The stream mask uses NHDPlus HR flowlines at Strahler order ≥4 rather than Jacobs's original ≥5 cutoff. On the 1:100k network used through v1.16 the cutoff was 3, chosen so that named perennial creeks like Sycamore Creek (order 3 or 4 at that scale) were not excluded; the 1:24k snapshot counts more headwaters and raises every named creek by one to two orders (Sycamore Creek 6, Oak Creek 5, West Fork Oak Creek 4), so 4 reproduces the field-reviewed display while keeping those creeks in. The heatmap renders at full opacity across the entire search area, including past the 75th percentile, where roughly 1-in-4 finds still occur per Koester's data and where Jacobs found that linear-feature PDEN actually increases with distance from the IPP.

Travel Time mode uses the same cost-distance pipeline but converts terrain-equivalent meters to hours using a user-supplied flat-ground speed, then contours at user-selected time intervals (2h, 4h, 6h, 8h, 10h, 12h). No Lost Person Behavior profile is required; this mode models physical capability rather than statistical find-distance likelihood.

## Tech stack

- **Backend:** Python 3.12, Flask, Gunicorn, Nginx
- **Frontend:** Leaflet.js, vanilla JavaScript (single-page app)
- **Geospatial:** rasterio, GDAL, shapely, geopandas, scipy, rasterstats, pyogrio
- **Server:** Ubuntu 24.04 on Linode (4GB RAM)

## References

- Danser, R.A., 2018. Applying least cost path analysis to search and rescue data [thesis]. University of Southern California.
- Doherty, P.J., Guo, Q., Doke, J., and Ferguson, D., 2014. An analysis of probability of area techniques for missing persons in Yosemite National Park. *Applied Geography*, 47, 99–110. doi:10.1016/j.apgeog.2013.11.001
- Ferguson, D., 2014. *Integrated Geospatial Tools for Search and Rescue (IGT4SAR)* [online]. GitHub. Available from: https://github.com/dferguso/IGT4SAR
- Imhof, E., 1950. *Gelände und Karte*. Erlenbach-Zürich: Eugen Rentsch Verlag.
- Jacobs, M., 2015. *Terrain Based Probability Models for SAR Executive Summary* [online]. Available from: https://mra.org/wp-content/uploads/2016/05/TerrainProbabilityModelsReport.pdf
- Koester, R.J., 2008. *Lost person behavior: a search and rescue guide on where to look — for land, air and water.* Charlottesville, VA: dbS Productions.
- Tobler, W.R., 1993. Non-isotropic geographic modeling. In: W.R. Tobler, ed. *Three presentations on geographical analysis and modeling.* Technical Report 93-1. Santa Barbara, CA: National Center for Geographic Information and Analysis.

## Author

**Jamie F. Weleber**

## License

AGPL-3.0 — see [LICENSE](LICENSE) for details.
