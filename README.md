# Weather Data Pipeline for Energy Forecasting

**Enpal Energy Take-Home Assignment**  

---

## 🎯 Project Overview

This project implements a weather data pipeline that ingests German weather data from the DWD (German Weather Service) via BrightSky API, transforms it into ML-ready format at postal code granularity, and provides comprehensive data quality monitoring.

### Key Features
- ✅ **Medallion Architecture**: Bronze (Raw) → Silver (Cleaned) → Gold (ML-Ready)
- ✅ **Spatial Intelligence**: Inverse Distance Weighting (IDW) for postal code aggregation
- ✅ **Data Quality**: 2+ validation steps with quality scoring
- ✅ **Idempotent**: Safe to re-run without duplicates
- ✅ **Scalable**: Handles 900+ postal codes, 1000+ weather stations
- ✅ **Observable**: Comprehensive logging and metrics

### Pipeline Output
- **269 unique postal codes** covered (Berlin/Brandenburg region)
- **Hourly temporal resolution** (144+ hours of historical data)
- **ML-ready format**: Cleaned, validated, spatially aggregated
- **Quality scores**: Per-record quality metrics (0.0-1.0 scale)

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    BrightSky API (DWD Data)                  │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼ Ingestion (Every 6h)
┌─────────────────────────────────────────────────────────────┐
│                  BRONZE LAYER (Raw)                          │
│  • raw_weather_observations (station-level)                 │
│  • raw_weather_forecasts (station-level)                    │
│  • weather_stations (metadata)                              │
│  • postal_codes (geometries)                                │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼ Transformation (Every 1h)
┌─────────────────────────────────────────────────────────────┐
│                  SILVER LAYER (Staging)                      │
│  • stg_observations (validated)                             │
│  • stg_forecasts (validated)                                │
│  Data Quality Steps:                                        │
│    1. Remove incomplete records (>50% nulls)                │
│    2. Flag outliers (physical limits)                       │
│    3. Temporal consistency checks                           │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼ Aggregation (IDW)
┌─────────────────────────────────────────────────────────────┐
│                  GOLD LAYER (ML-Ready)                       │
│  • analytics_weather_by_postal_code                         │
│    - Postal code level (269 codes)                          │
│    - Hourly resolution                                      │
│    - Quality scores                                         │
│    - Contributing station metadata                          │
└─────────────────────────────────────────────────────────────┘
```


### Tech Stack

- **PostgreSQL + PostGIS**: For storing geospatial and time-series data
- **Python 3.13**: Core programming language
- **Apache Airflow**: Task orchestration and workflow management
- **Docker Compose**: Containerization and portability
- **Poetry**: Dependency management

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python + Poetry
- Git

### Setup
```bash
git clone <your-repo-url>
cd weather-data-pipeline-sv

# Copy environment template
cp .env.example .env


# 1. Start all services

# For the first time, to load postal_codes
docker-compose up --build

# All other times
docker-compose up -d

# Check service health
docker-compose ps

# Expected output:
#   weather_postgres        healthy
#   weather_airflow_webserver   healthy
#   weather_airflow_scheduler   healthy
#   weather_init_dirs          Started                                                                          16.3s 
#   weather_airflow_init                                                                                  30.2s 
#   weather-init               Started                                                                          17.0s 

# 2. Access services
# Access Airflow UI: http://localhost:8080
# Username: admin, Password: admin

# 3. Enable DAGs in Airflow UI
# - `ingest_observations_dag` - Fetches weather observations
# - `ingest_forecasts_dag` - Fetches weather forecasts
# - `transform_weather_dag` - Transforms to ML-ready format


# Alternatively, trigger from console:
docker-compose exec airflow-webserver airflow dags trigger ingest_observations_dag

```

### Verify Output

```bash
# Check service status
docker-compose ps

# Access database
docker exec weather_postgres psql -U postgres -d weather_db

# Sample queries

# Verify postal codes loaded
docker exec weather_postgres psql -U postgres -d weather_db -c \
  "SELECT COUNT(*) FROM postal_codes WHERE geometry IS NOT NULL;"
# Expected: ~900 postal codes

# Check data pipeline status
docker exec weather_postgres psql -U postgres -d weather_db -c "
SELECT 
    'RAW OBSERVATIONS' as layer, COUNT(*) as count, COUNT(DISTINCT station_id) as stations
FROM raw_weather_observations
UNION ALL
SELECT 'STAGING OBSERVATIONS', COUNT(*), COUNT(DISTINCT station_id)
FROM stg_observations
UNION ALL
SELECT 'ANALYTICS (ML-READY)', COUNT(*), COUNT(DISTINCT postal_code)
FROM analytics_weather_by_postal_code
WHERE data_type = 'observation';
"

# Expected output:
#   RAW OBSERVATIONS      | 100-200  | 1-2
#   STAGING OBSERVATIONS  | 100-200  | 1-2
#   ANALYTICS (ML-READY)  | 30,000+  | 200-300
```

### 6. Query ML-Ready Data
```python
import psycopg2

conn = psycopg2.connect("postgresql://postgres:postgres@localhost:5432/weather_db")

# Get latest weather for a postal code
query = """
SELECT 
    postal_code,
    timestamp,
    temperature_avg,
    precipitation_sum,
    wind_speed_avg,
    num_stations,
    avg_quality_score
FROM analytics_weather_by_postal_code
WHERE postal_code = '10115'  -- Berlin Mitte
  AND data_type = 'observation'
ORDER BY timestamp DESC
LIMIT 24;  -- Last 24 hours
"""

```

---

## Documentation

### Tech Stack Justification

- **PostgreSQL + PostGIS**: Industry-standard for geospatial data, ACID compliance, excellent for time-series with proper indexing

- **Apache Airflow**: De-facto standard for data pipeline orchestration, retry logic, monitoring, backfilling capabilities

- **Docker Compose**: Perfect for local development and testing, easy to transition to production environments (eg. Kubernetes)

- **Python 3.11**: ML ecosystem compatibility, rich data libraries (pandas, geopandas)

### Enhancements for Production & Deployment

1. **Infrastructure**
   - Kubernetes deployment (with Helm charts)
   - Managed PostgreSQL (AWS RDS, Google Cloud SQL)
   - Managed Airflow (AWS MWAA, Google Cloud Composer)
   - Separate compute for transformations (Spark, dbt)

2. **Scalability**
   - API Layer: Modern async framework, like FastAPI, with auto-generated docs, type safety, and ML model serving ready
   - dbt: Better for pure SQL transformations
   - Spark: Unnecessary for <1M records, PostGIS more efficient for spatial
   - Jupyter notebooks: Exploratory analysis

3. **Reliability**
   - Dead letter queue for failed records
   - Rate limiting and backoff for BrightSky API
   - Database replication and backups
   - Multi-region deployment

4. **Monitoring & Observability**
   - Prometheus metrics export
   - Grafana dashboards
   - Distributed tracing (OpenTelemetry)
   - Quality Alerting: Email notifications, Slack integration, PagerDuty alerts
   - Great Expectations: Data quality validation and monitoring
   - Cost monitoring per pipeline

5. **Data Governance**
   - SQLAlchemy as an interface
   - Pydantic: Data validation and serialization
   - Data lineage tracking (Apache Atlas, DataHub)
   - Schema evolution management (Alembic migrations)
   - Partition tables by time (monthly partitions)
   - GDPR compliance (data retention policies)
   - Access control (RBAC in Airflow + DB)

6. **CI/CD & Testing**
   - Unit tests with Pytest
   - GitHub Actions for testing and deployment
   - Schema migration tests
   - Data quality regression tests
   - Blue-green deployments
   - Smoke Tests: Fast validation tests for CI/CD pipelines
     - API module imports and initialization tests
     - Service layer instantiation tests
     - Model validation tests
     - Ingestion module smoke tests
   - Comprehensive Test Suite: Full integration and unit tests
   - Performance Tests: Load testing for API endpoints

7. **API Framework Enhancements**
   - FastAPI: REST API for downstream ML services
   - Enhanced Response Models: Comprehensive metadata, pagination, quality summaries
   - Caching Layer: Redis-based response caching for performance
   - Rate Limiting: API throttling and usage controls
   - Webhook Support: Real-time notifications for data quality alerts

8. **Performance**
   - Parallel ingestion by postal code regions
   - Materialized views for common queries
   - Columnar storage (Parquet) for historical data
   - Query optimization (EXPLAIN ANALYZE)
   - Connection pooling (PgBouncer)


### Currently Implemented

- **Idempotency**: All inserts use `ON CONFLICT` for safe re-runs
- **Retry Logic**: 3 attempts with exponential backoff
- **Health Checks**: All services monitored via Docker
- **Logging**: Structured logging with context
- **Data Quality**: Automated validation with metrics
- **Schema Versioning**: SQL scripts in version control
- **Data Model**: Key Design Decision**: Postal code level granularity enables
    - Direct ML model consumption without spatial joins
    - Consistent feature engineering across regions
    - Easy integration with customer/property data (via PLZ)

### Pipeline Components

### 1. Ingestion DAGs

**`ingest_observations_dag`**
- **Schedule**: Every 6 hours (`0 */6 * * *`)
- **Function**: Fetches last 7 days of observations
- **Incremental**: Only new data since last run
- **Output**: `raw_weather_observations`

**`ingest_forecasts`**
- **Schedule**: Every 6 hours (`0 */6 * * *`)
- **Function**: Fetches next 10 days of forecasts
- **Output**: `raw_weather_forecasts`

**Key Features:**
- Automatic station discovery from API responses
- Retry logic (3 attempts with exponential backoff)
- Idempotent: Uses `ON CONFLICT` for upserts
- Rate limiting: Respects BrightSky API limits

### 2. Transformation DAG

**`transform_weather`**
- **Schedule**: Every hour at :30 (`30 * * * *`)
- **Processing Window**: Last 24 hours (idempotent)
- **Steps**:
  1. **Cleaning**: Remove incomplete records (>50% nulls)
  2. **Validation**: Flag outliers (physical limits: -40°C to 50°C)
  3. **Consistency**: Check temporal jumps (>20°C/hour)
  4. **Aggregation**: IDW spatial interpolation to postal codes

**Transformation Logic:**
```python
# Step 1: Data Cleaning
- Remove records with >50% missing critical fields (temp, wind, precip)
- Flag outliers: temperature NOT BETWEEN -40 AND 50
- Calculate quality score based on completeness

# Step 2: Spatial Aggregation (IDW)
weight = 1 / distance²  # Inverse Distance Weighting
temperature_avg = Σ(temp_i × weight_i) / Σ(weight_i)

# Step 3: Wind Direction (Circular Mean)
direction = atan2(Σ(sin(θ_i) × weight_i), Σ(cos(θ_i) × weight_i))
```

### Data Quality Framework

### Validation Steps (2+ Required)

#### 1. Completeness Check
```sql
-- Remove records with >50% missing critical fields
WHERE NOT (
    (CASE WHEN temperature IS NULL THEN 1 ELSE 0 END) +
    (CASE WHEN wind_speed IS NULL THEN 1 ELSE 0 END) +
    (CASE WHEN precipitation IS NULL THEN 1 ELSE 0 END)
) > 1
```

#### 2. Outlier Detection (Physical Limits)
```sql
-- Flag records outside physically possible ranges
has_outliers = CASE WHEN (
    temperature NOT BETWEEN -40 AND 50 OR
    wind_speed NOT BETWEEN 0 AND 200 OR
    precipitation NOT BETWEEN 0 AND 200 OR
    humidity NOT BETWEEN 0 AND 100
) THEN TRUE ELSE FALSE END
```

#### 3. Temporal Consistency (Bonus)
```sql
-- Detect unrealistic jumps
WITH temp_changes AS (
    SELECT 
        ABS(temperature - LAG(temperature) OVER (ORDER BY timestamp)) as temp_diff,
        timestamp - LAG(timestamp) OVER (ORDER BY timestamp) as time_diff
    FROM observations
)
SELECT COUNT(*) FROM temp_changes
WHERE temp_diff > 20 AND time_diff <= INTERVAL '1 hour'
```

### Quality Scoring Algorithm
```python
quality_score = (
    1.0 if no_missing_values else 0.6 +  # 40% penalty
    0.0 if has_outliers else 0.0 +        # Fail outliers
    0.0 if temporal_inconsistent else 0.0 # Fail inconsistencies
)

# For aggregated postal code data:
final_score = (
    data_quality * 0.6 +                  # 60% from source quality
    (num_stations / 3.0) * 0.2 +          # 20% from station density
    (1 - distance/50km) * 0.2             # 20% from proximity
)
```

### Additional Validation Ideas (Outlined, Not Implemented)
```python
# 4. Cross-field validation
if humidity > 95% and abs(temperature - dew_point) > 2:
    flag_issue("humidity_dewpoint_mismatch")

# 5. Statistical outlier detection (z-score)
z_score = (value - historical_mean) / historical_std
if z_score > 3:
    flag_issue("statistical_outlier")

# 6. Forecast accuracy tracking
mae = mean(abs(forecast - actual))
update_accuracy_score(1.0 - mae/10.0)
```

- ML feature engineering
- Forecast Accuracy tracking

### Airflow UI Monitoring
**Access http://localhost:8080:**
- **DAG Success Rate**: Should be >95%
- **Task Duration**: Ingestion <5min, Transform <10min
- **SLA Misses**: Should be 0

## 📝 Assignment Completion Checklist

- ✅ **Task 1**: Discovered BrightSky API & postal code sources
- ✅ **Task 2**: PostgreSQL + PostGIS database with medallion architecture
- ✅ **Task 3**: Python script for postal code ingestion (`load_postal_codes_berlin.py`)
- ✅ **Task 4**: Two ingestion pipelines (observations + forecasts)
  - ✅ Incremental loading
  - ✅ Station discovery
  - ✅ Error handling & retries
- ✅ **Task 5**: Transformation pipeline with 2+ data quality steps
  - ✅ Completeness validation (>50% nulls)
  - ✅ Outlier detection (physical limits)
  - ✅ Temporal consistency check (bonus)
  - ✅ ML-ready output (postal code + hourly resolution)
- ✅ **Task 6**: Apache Airflow for scheduling
- ✅ **Task 7**: Production considerations documented

### Deliverables
- ✅ Working code (Python + SQL)
- ✅ Docker Compose for deployment
- ✅ Comprehensive documentation (this README)
- ✅ Data quality framework
- ✅ Monitoring & logging
- ✅ Production enhancement roadmap

---
