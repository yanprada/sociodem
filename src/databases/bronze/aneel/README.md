# Script Execution Order

1. `a_download_aneel_companies.py`
2. `b_make_bronze_aneel_dataset.py`

# Process Overview

First, we need to download the files from the internet. After downloading, we save the data into Parquet files and store it in a PostgreSQL database.

# PostgreSQL Database Schema: `infrastructure`

In the `infrastructure` schema of the PostgreSQL database, we will create and maintain five tables:

1. **`aneel_companies_id`**: 
   - **Description**: Contains the IDs established by ANEEL for each energy company in Brazil.
   - **Purpose**: These IDs are used to download data from the ANEEL website. To get this ids, we downloaded manually in the Aneel website. The problem is that sometimes the ids where repeated. So we need to make sure that all the ids where in the downloaded file.

2. **`ucbt`**: 
   - **Description**: Contains data on units of low-tension energy consumption.

3. **`ponnot`**: 
   - **Description**: Contains geolocation data of energy points.

4. **`ramlig`**: 
   - **Description**: Contains data on connections between two energy points (ponnot).
   - **Purpose**: Used to find ponnot IDs based on other features when ponnot ID is missing for some energy companies.

5. **`conj`**: 
   - **Description**: Contains data on the regions covered by each energy company on the map.
   - **Purpose**: Visual validation of the area covered by our data.