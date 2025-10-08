# Airflow Dockerfile
FROM apache/airflow:2.8.1

# Switch to root to install system dependencies
USER root

# Install system dependencies for PostGIS and geospatial libraries
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    libgeos-dev \
    libproj-dev \
    libgdal-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install core dependencies directly (compatible with Python 3.8)
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    sqlalchemy==1.4.53 \
    geoalchemy2==0.14.3 \
    apache-airflow-providers-postgres==5.10.0 \
    httpx==0.26.0 \
    tenacity==8.2.3 \
    shapely==1.8.5 \
    geojson==3.1.0 \
    python-dotenv==1.0.0 \
    structlog==23.2.0 \
    pandas==2.0.3 \
    numpy==1.24.3

# Copy source code
COPY src/ /opt/airflow/src/
COPY dags/ /opt/airflow/dags/
COPY config/ /opt/airflow/config/

# Set Python path
ENV PYTHONPATH=/opt/airflow/src:/opt/airflow

# Set Airflow home
ENV AIRFLOW_HOME=/opt/airflow

# Expose port
EXPOSE 8080

# Default command (will be overridden in docker-compose)
CMD ["airflow", "webserver"]
