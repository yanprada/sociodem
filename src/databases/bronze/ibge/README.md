# Script Execution Order

1. `download_layers.py`
2. `make_bronze_dataset_ibge.py`

# Process Overview

First, we download the layers information from IBGE. After downloading, we index the data in the Postgres database.

# PostgreSQL Database Schema: `layers`

In the `layers` schema of the PostgreSQL database, we will create and maintain five tables:

1. **`dompp_ibge_2022`**: 
   - **Description**: Contains household information as points located in Brazil.

2. **`mun_ibge_2010`**: 
   - **Description**: Contains municipalities geometries for 2010.

3. **`mun_ibge_2022`**: 
   - **Description**: Contains municipalities geometries for 2022.

4. **`sc_ibge_2010`**: 
   - **Description**: Contains sector geometries for 2010.

5. **`sc_ibge_2022`**: 
   - **Description**: Contains sector geometries for 2022.