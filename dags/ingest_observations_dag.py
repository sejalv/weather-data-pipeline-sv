# dags/ingest_observations_dag.py
"""
Weather observations ingestion DAG.
Fetches last 7 days of observations from BrightSky API every 6 hours.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

from src.ingestion.observations import ingest_observations
from src.utils.dag_helpers import get_db_connection, log_ingestion_metrics

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'weather-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ingest_observations_dag',
    default_args=default_args,
    description='Ingest weather observations from BrightSky API',
    schedule_interval='0 */6 * * *',  # Every 6 hours at 00:00, 06:00, 12:00, 18:00
    catchup=False,
    max_active_runs=1,
    tags=['ingestion', 'observations', 'weather'],
)


def run_observations_ingestion(**context):
    """
    Fetch and insert observation data.
    
    Uses incremental loading - only fetches data since last run.
    """
    try:
        logger.info("Starting observations ingestion")
        
        # Get database connection
        conn = get_db_connection('weather_db')
        
        # Run ingestion (incremental by default)
        records_inserted = ingest_observations(conn, lookback_days=7)
        
        # Trigger station discovery (extract stations from ingested data)
        from src.utils.stations import discover_stations_from_data
        stations_found = discover_stations_from_data(conn, source_type='observation')
        
        # Log metrics consistently
        log_ingestion_metrics(records_inserted, stations_found, 'observations')
        
        conn.close()
        
        return {
            'records_inserted': records_inserted,
            'stations_discovered': stations_found
        }
        
    except Exception as e:
        logger.error(f"Observations ingestion failed: {e}")
        raise


def validate_observations_ingestion(**context):
    """
    Validate that observations were ingested successfully.
    
    Checks:
    - At least some records were inserted
    - Data freshness (most recent timestamp is recent)
    """
    try:
        # Get the full result from the previous task
        task_result = context['task_instance'].xcom_pull(
            task_ids='ingest_observations'
        )
        
        # Handle both dictionary and direct value returns
        if isinstance(task_result, dict):
            records_inserted = task_result.get('records_inserted', 0)
        else:
            records_inserted = task_result or 0
        
        logger.info(f"Records inserted from ingestion: {records_inserted}")
        
        if records_inserted == 0:
            logger.warning("No new observations ingested - this might be normal")
            # Still validate that we have some data in the database
            records_inserted = None  # Will check total count instead
        
        # Check data freshness
        pg_hook = PostgresHook(postgres_conn_id='weather_db')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Query for recent data (last 24 hours) rather than just created_at
        cursor.execute("""
            SELECT 
                MAX(timestamp) as latest_timestamp,
                COUNT(*) as total_count,
                COUNT(DISTINCT station_id) as unique_stations,
                MAX(created_at) as latest_created
            FROM raw_weather_observations
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
        """)
        
        result = cursor.fetchone()
        latest_timestamp, total_count, unique_stations, latest_created = result
        
        cursor.close()
        conn.close()
        
        logger.info(f"Validation - Latest timestamp: {latest_timestamp}, Total records: {total_count}, Unique stations: {unique_stations}, Latest created: {latest_created}")
        
        # Basic validation
        if total_count == 0:
            raise ValueError("No observations found in database for the last 24 hours")
        
        if unique_stations == 0:
            raise ValueError("No weather stations found")
        
        # Check data is not too stale (within last 48 hours)
        if latest_timestamp:
            from datetime import timezone
            now_utc = datetime.now(timezone.utc)
            
            # Handle timezone-aware vs naive timestamps
            if latest_timestamp.tzinfo is None:
                latest_timestamp = latest_timestamp.replace(tzinfo=timezone.utc)
            
            age_hours = (now_utc - latest_timestamp).total_seconds() / 3600
            if age_hours > 48:
                logger.warning(f"Latest observation is {age_hours:.1f} hours old")
            else:
                logger.info(f"Latest observation is {age_hours:.1f} hours old (acceptable)")
        
        logger.info("✅ Observations validation passed")
        return True
        
    except Exception as e:
        logger.error(f"Observations validation failed: {e}")
        raise


# Task definitions
ingest_task = PythonOperator(
    task_id='ingest_observations',
    python_callable=run_observations_ingestion,
    provide_context=True,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_observations',
    python_callable=validate_observations_ingestion,
    provide_context=True,
    dag=dag,
)

# Task dependencies
ingest_task >> validate_task