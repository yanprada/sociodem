# Script Execution Order

1. `download_buildings.py`
2. `index_h3_polygon.py`
3. `group_buildings_by_hex.py`

# Process Overview

First, we download the building information from the internet. After downloading, we index the data using Uber's H3 indexing system. Finally, we group our data by H3 hexagons.

# PostgreSQL Database Schema: `buildings`

In the `buildings` schema of the PostgreSQL database, we will create and maintain four tables:

1. **`google`**: 
   - **Description**: Contains raw building data obtained from Google.

2. **`google_grouped_by_hex`**: 
   - **Description**: Contains Google building data grouped by H3 hexagons.

3. **`omf`**: 
   - **Description**: Contains raw building data obtained from the Overture Map Foundation.

4. **`omf_grouped_by_hex`**: 
   - **Description**: Contains Overture Map Foundation building data grouped by H3 hexagons.
