"""
This module processes a GeoTIFF image to aggregate building data into
hexagonal grids using the H3 library.
The resulting data is saved as a CSV file with hexagon IDs, building counts, and geometries.

Steps:
1. Read the GeoTIFF image and extract building data.
2. Convert coordinates to latitude and longitude if necessary.
3. Aggregate building data into hexagonal grids.
4. Save the aggregated data to a CSV file.

Modules:
- rasterio: For reading GeoTIFF images.
- numpy: For numerical operations.
- h3: For hexagonal grid operations.
- geopandas: For handling geospatial data.
- shapely: For geometric operations.
- pyproj: For coordinate transformations.
- src.tools.utils.constants: For constants like HEX_RESOLUTION and CRS_GLOBAL.
- src.tools.utils.common: For logging utilities.
- src.tools.utils.execution_manager: For managing execution parameters.
- src.databases.bronze.buildings.omf.config: For configuration parameters.
- config.run_mode: For run mode settings.

Constants:
- TIF_FILE: The path to the GeoTIFF file to be processed.

Functions:
- None

Classes:
- None

"""

import rasterio
import numpy as np
import h3
import geopandas as gpd
from shapely.geometry import Polygon
from pyproj import Transformer
from src.tools.utils.constants import HEX_RESOLUTION, CRS_GLOBAL
from src.tools.utils.common import write_log

from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.buildings.omf.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
BUILDING_CONTRACTS = execution_parameters["data_contracts"]["raw_data"]
manager.update_status("running_step_1")

TIF_FILE = "teste.tif"

with rasterio.open(TIF_FILE) as src:
    crs = src.crs
    transform = src.transform
    write_log(f"CRS da imagem: {crs}")
    building_fractional_count = src.read(1)  # Banda 1: Contagem fracionada de prédios
    building_presence = src.read(3)  # Banda 3: Presença de prédios (0 ou 1)
    rows, cols = np.where(building_presence > 0)
    proj_coords = [src.xy(row, col) for row, col in zip(rows, cols)]
    if crs.is_projected:
        write_log("A imagem está projetada, convertendo para lat/lon...")
        transformer = Transformer.from_crs(crs, CRS_GLOBAL, always_xy=True)
        lats_lons = [transformer.transform(x, y) for x, y in proj_coords]
    else:
        write_log("A imagem já está em lat/lon.")
        lats_lons = proj_coords
    building_counts = [
        building_fractional_count[row, col] for row, col in zip(rows, cols)
    ]

hexagons = {}

for (lon, lat), building_count in zip(lats_lons, building_counts):
    h3_index = h3.geo_to_h3(lat, lon, HEX_RESOLUTION)
    if h3_index in hexagons:
        hexagons[h3_index] += building_count
    else:
        hexagons[h3_index] = building_count
hex_geometry = [
    Polygon(h3.h3_to_geo_boundary(h3_index, geo_json=True)) for h3_index in hexagons
]
gdf = (
    gpd.GeoDataFrame(
        {
            "hex_id": list(hexagons.keys()),
            "building_count": list(hexagons.values()),
            "geometry": hex_geometry,
        }
    )
    .groupby("hex_id", as_index=False)
    .agg({"building_count": "sum", "geometry": "first"})
)
gdf.to_csv("buildings_by_hex.csv", index=False)
