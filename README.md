# SocioDemographic Atlas
---
## 📘 About

This project is a modular ETL (Extract, Transform, Load) pipeline designed to process, clean, and integrate heterogeneous geospatial and sociodemographic data from both public and foundational sources. It supports scalable analytics and machine learning tasks across urban, environmental, and socioeconomic domains in Brazil.

By combining temporal and spatial datasets — such as land use maps, census microdata, energy consumption, infrastructure, and building growth — the pipeline enables the creation of rich, analysis-ready datasets for applications in:

- Urban planning and housing policy  
- Environmental impact analysis  
- Socioeconomic inequality studies  
- Infrastructure and energy demand modeling  
- Retail expansion and site selection  
- Machine learning models with spatial context (hex-based grids)

The pipeline adopts a layered architecture (`bronze → silver → gold`) that ensures data provenance, transparency, and reproducibility. It is designed for researchers, data scientists, and policy makers who need high-quality geospatial data with socioeconomic depth.

---

## 📂 Project Structure
```bash
.
├── src/                     # Source code for data processing
│   ├── bronze/              # Raw data ingestion (original format, minimal processing)
│   ├── silver/              # Cleansed, standardized, and georeferenced datasets
│   └── gold/                # Final analytical datasets with enriched features
│
├── api/                    # External data (excluded via .gitignore)
│   ├── raw/                 # Original files (e.g., zipped shapefiles, CSVs)
│   ├── interim/             # Intermediate files during transformation
│   └── processed/           # Final outputs (e.g., Parquet, GeoParquet)
│
├── .gitignore               # Specifies untracked files to ignore
├── requirements.txt         # Python dependencies
├── README.md                # Project documentation
└── LICENSE                  # Project license

---

## 🌍 Data Sources

The pipeline integrates and processes data from the following sources:

### 🛰 Environmental & Land Use

- **[MapBiomas](https://mapbiomas.org/)**: Annual land use and land cover maps for Brazil (2016–present)

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
