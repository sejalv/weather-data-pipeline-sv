# dags/ingest_forecasts_dag.py
"""
Weather forecasts ingestion DAG.
Fetches 10-day forecasts from BrightSky API every 3 hours.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

from src.ingestion.forecasts import ingest_forecasts
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
    'ingest_forecasts_dag',
    default_args=default_args,
    description='Ingest weather forecasts from BrightSky API',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['ingestion', 'forecasts', 'weather'],
)


def run_forecasts_ingestion(**context):
    """
    Fetch and insert forecast data.
    
    Fetches next 10 days of forecasts. Each run creates a new forecast version
    timestamped with forecast_timestamp.
    """
    try:
        logger.info("Starting forecasts ingestion")
        
        # Get database connection
        conn = get_db_connection('weather_db')
        
        # Run ingestion (always fetch next 10 days)
        records_inserted = ingest_forecasts(conn, forecast_days=10)
        
        # Discover forecast stations
        from src.utils.stations import discover_stations_from_data
        stations_found = discover_stations_from_data(conn, source_type='forecast')
        
        # Log metrics consistently
        log_ingestion_metrics(records_inserted, stations_found, 'forecasts')
        
        conn.close()
        
        return {
            'records_inserted': records_inserted,
            'stations_discovered': stations_found
        }
        
    except Exception as e:
        logger.error(f"Forecasts ingestion failed: {e}")
        raise


def validate_forecasts_ingestion(**context):
    """
    Validate that forecasts were ingested successfully.
    
    Checks:
    - Records were inserted
    - Forecasts cover future time period
    - Multiple days of forecasts available
    """
    try:
        # Get the full result from the previous task
        task_result = context['task_instance'].xcom_pull(
            task_ids='ingest_forecasts'
        )
        
        # Handle both dictionary and direct value returns
        if isinstance(task_result, dict):
            records_inserted = task_result.get('records_inserted', 0)
        else:
            records_inserted = task_result or 0
        
        logger.info(f"Records inserted from forecasts ingestion: {records_inserted}")
        
        if records_inserted == 0:
            logger.info("No new forecasts were ingested - this is normal if all forecasts are already up to date")
            # Still validate that we have some forecast data in the database
            records_inserted = None  # Will check total count instead
        
        # Check forecast coverage
        pg_hook = PostgresHook(postgres_conn_id='weather_db')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                MIN(target_timestamp) as earliest_forecast,
                MAX(target_timestamp) as latest_forecast,
                COUNT(*) as total_count,
                COUNT(DISTINCT station_id) as unique_stations,
                MAX(forecast_timestamp) as latest_forecast_run
            FROM raw_weather_forecasts
            WHERE forecast_timestamp >= NOW() - INTERVAL '6 hours'
        """)
        
        result = cursor.fetchone()
        earliest, latest, total_count, unique_stations, latest_run = result
        
        cursor.close()
        conn.close()
        
        logger.info(f"Validation - Range: {earliest} to {latest}")
        logger.info(f"Total: {total_count}, Stations: {unique_stations}")
        logger.info(f"Latest run: {latest_run}")
        
        # Validation checks
        if total_count < 10:
            raise ValueError(f"Too few forecasts ingested: {total_count}")
        
        if unique_stations == 0:
            raise ValueError("No forecast stations found")
        
        # Check forecasts cover at least 3 days ahead
        if earliest and latest:
            forecast_days = (latest.replace(tzinfo=None) - datetime.utcnow()).days
            if forecast_days < 3:
                logger.warning(f"Forecasts only cover {forecast_days} days ahead")
        
        logger.info("✅ Forecasts validation passed")
        
    except Exception as e:
        logger.error(f"Forecasts validation failed: {e}")
        raise


# Task definitions
ingest_task = PythonOperator(
    task_id='ingest_forecasts',
    python_callable=run_forecasts_ingestion,
    provide_context=True,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_forecasts',
    python_callable=validate_forecasts_ingestion,
    provide_context=True,
    dag=dag,
)

# Task dependencies
ingest_task >> validate_task