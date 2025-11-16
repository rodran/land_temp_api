# Land Temperature Change API

> REST API serving climate change data from a PostgreSQL Data Warehouse

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL 15](https://img.shields.io/badge/postgresql-15-blue.svg)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## Overview

A production-ready data warehouse and REST API demonstrating backend and data engineering skills by serving FAO climate data through a dimensional model (star schema) implemented in PostgreSQL.

**Portfolio Project showcasing:**
- Data warehouse design with star schema
- ETL pipeline implementation
- RESTful API development
- Docker containerization
- Professional code organization

### Current Status: Phase 1 - Foundation 

- [ ] Database schema design
- [ ] Docker infrastructure
- [ ] ETL pipeline
- [ ] Phase 2: FastAPI REST API
- [ ] Phase 3: CI/CD and deployment

---

## Data Source

**[FAO Environment Temperature Change Dataset](http://www.fao.org/faostat/en/#data/ET)**

- **Period**: 1961-2024 (64 years)
- **Coverage**: 284 areas (countries, subregions, continents, world)
- **Granularity**: Monthly, Seasonal, Annual
- **Metrics**: Temperature change (°C), Standard Deviation
- **Size**: ~7MB raw → ~616K records after ETL

---

## Architecture

```
CSV Data (FAO) → ETL Pipeline → PostgreSQL DW → FastAPI (Phase 2) → Notebooks (Phase 3)
                                      ↓
                                  pgAdmin
```

### Data Warehouse: Star Schema

**3 Dimension Tables:**
- `dim_area`: Geographic hierarchy (world → continent → subregion → country)
- `dim_time_period`: Temporal hierarchy (annual → season → month)
- `dim_metric`: Measurement types (Temperature change, Standard Deviation)

**1 Fact Table:**
- `fact_temperature`: ~616K measurements with foreign keys to dimensions

**Analytics Layer:**
- 3 materialized views for pre-aggregated insights

### Schema Layers

- **`staging`**: Raw data landing zone
- **`core`**: Dimensional model (star schema)
- **`analytics`**: Materialized views for common queries

---

## Tech Stack

**Phase 1 - Foundation (Current):**
- Python 3.11+
- PostgreSQL 15
- Pandas 2.x (ETL)
- SQLAlchemy 2.0
- Psycopg2
- Docker, Docker Compose

**Phase 2 - REST API:**
- FastAPI 0.104+
- Pydantic v2
- Uvicorn
- Pytest (>70% coverage)
- httpx (testing)

**Phase 3 - Analysis & Documentation:**
- Jupyter / JupyterLab
- Plotly (visualizations)
- Requests (API consumption)
- Black, isort, Flake8 (code quality)

---

## Project Structure

```
land_temp_api/
├── README.md
├── LICENSE
├── .gitignore
├── .env.example
├── docker-compose.yml
├── requirements.txt
│
├── database/sql/           # PostgreSQL schema (6 scripts)
│   ├── 01_create_schemas.sql
│   ├── 02_create_staging.sql
│   ├── 03_create_dimensions.sql
│   ├── 04_create_facts.sql
│   ├── 05_create_indexes.sql
│   └── 06_create_analytics_views.sql
│
├── data/                   # Data files (not committed)
│   ├── raw/
│   └── metadata/
│
├── etl/                    # ETL Pipeline (Phase 1)
│   ├── config.py
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── run_pipeline.py
│   └── utils/
│
├── api/                    # FastAPI (Phase 2)
├── tests/                  # Test suite
├── notebooks/              # Analysis notebooks
└── docker/                 # Dockerfiles
```

---

## Development Roadmap

### Phase 1: Foundation (Current)
**Goal**: Data warehouse with working ETL pipeline

- [ ] PostgreSQL star schema
- [ ] Dimension tables with hierarchies
- [ ] Fact table with proper indexes
- [ ] Analytics materialized views
- [ ] ETL pipeline (extract, transform, load)
- [ ] Docker orchestration
- [ ] Data validation

**Estimated**: 10-15 hours

### Phase 2: REST API (Next)
**Goal**: Functional API with core endpoints

- [ ] FastAPI application setup
- [ ] SQLAlchemy ORM models
- [ ] Pydantic request/response schemas
- [ ] 6 core API endpoints:
  - Health check
  - List/get areas
  - Temperature time series
  - Compare areas
  - Analytics (top warming)
- [ ] Automatic documentation (Swagger/ReDoc)
- [ ] Error handling and validation
- [ ] Pytest suite (>70% coverage)

**Estimated**: 12-18 hours

### Phase 3: Analysis & Documentation (Final)
**Goal**: Complete portfolio project

- [ ] 3 Jupyter notebooks:
  - API quickstart guide
  - Exploratory analysis
  - Seasonal patterns analysis
- [ ] Enhanced README with API examples
- [ ] Code quality improvements (Black, isort, Flake8)
- [ ] Final documentation review

**Estimated**: 6-10 hours

---

## Contributing

This is a portfolio project, but suggestions are welcome!

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'feat: add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## License

MIT License - See LICENSE.txt file for details.

Free to use for learning and portfolio purposes.

---

## Acknowledgments

- **Data Source**: [FAO FAOSTAT](http://www.fao.org/faostat/)
- **Purpose**: Backend and data engineering skills demonstration

---

## Author

**[Rodrigo Rangel]**

- Portfolio: [rodran.xyz](https://rodran.xyz)  _(coming soon)_
- GitHub: [@rodran](https://github.com/rodran)
- LinkedIn: [linkedin.com/in/rodran/](https://www.linkedin.com/in/rodran/)

---

**⭐ If you find this project helpful, please give it a star!**
