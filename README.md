# 🧠 SocioDemographic ELT Pipeline

This project implements a scalable, modular ELT (Extract → Load → Transform) pipeline for processing and enriching geospatial and sociodemographic datasets from diverse sources. The goal is to unify, clean, and structure raw data into high-quality, analysis-ready datasets supporting urban, environmental, and socioeconomic research and applications.

---

## 📂 Project Structure


---

## 🌍 Data Sources

The pipeline integrates and processes data from the following sources:

### 🛰 Environmental & Land Use

- **[MapBiomas](https://mapbiomas.org/)**: Annual land use and land cover maps for Brazil (2000–present)

### 🗺️ Infrastructure & Points of Interest

- **[Overture Maps Foundation](https://overturemaps.org/)**:
  - `places`: POIs with structured categories
  - `buildings`: Footprints and attributes
  - `transportation`: Roads and transport networks

### ⚡ Energy

- **[ANEEL](https://www.gov.br/aneel)**:
  - Geolocated electric poles (posts)
  - Electricity consumption per consumer (residential, commercial, industrial, etc.)

### 👥 Demographics & Census

- **[IBGE - Censo Demográfico 2022](https://censo2022.ibge.gov.br/)**:
  - Population by age, sex, race, income
  - Households, housing types, education, employment
- **IBGE Layers**:
  - Sector census geometries, administrative boundaries, road networks

### 🏗️ Urban Growth

- **[Google Temporal Buildings](https://sites.research.google/open-buildings/)**:
  - Yearly building footprints (2016–2023)
  - Temporal insights on urban expansion

---

## ⚙️ ELT Workflow

The pipeline follows a structured three-stage process:

### 🥉 Bronze – Raw Extraction

- Ingest and store raw data with minimal processing
- Convert file formats if necessary (e.g., CSV to Parquet, GeoJSON to GeoParquet)
- Extract metadata and spatial extent

### 🥈 Silver – Cleansing and Standardization

- Normalize schema and spatial reference systems (SRS)
- Remove duplicates, fix geometries, validate attributes
- Harmonize across datasets (e.g., unify place categories, join with census sectors)

### 🥇 Gold – Feature Engineering and Integration

- Derive final analytical features:
  - Population per building
  - Energy usage density per area or household
  - Land use transitions
  - POI accessibility
- Aggregate features to hexagonal grids (e.g., H3 resolution levels)
- Ready for machine learning models or dashboards

---

## 🛠️ Technologies

- **Languages**: Python 3.10+
- **Libraries**: `pandas`, `geopandas`, `pyarrow`, `h3`, `dask`, `shapely`, `rasterio`, `fiona`
- **Storage**: Parquet + GeoParquet
- **ETL Orchestration**: Can integrate with `Airflow`, `Prefect` or custom runners
- **Versioning**: Git + `.gitignore` support for raw/notebook exclusions

---

## 🚀 Getting Started

Clone the repository:

```bash
git clone https://github.com/your-org/sociodem-elt.git
cd sociodem-elt
