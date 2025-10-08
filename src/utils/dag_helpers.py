"""
DAG utility functions for common patterns.
"""
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def get_db_connection(postgres_conn_id: str = 'weather_db'):
    """
    Get database connection with proper error handling.
    
    Args:
        postgres_conn_id: Airflow connection ID for PostgreSQL
        
    Returns:
        Database connection object
        
    Raises:
        Exception: If connection fails
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        logger.debug(f"Database connection established using connection ID: {postgres_conn_id}")
        return conn
    except Exception as e:
        logger.error(f"Failed to establish database connection: {e}")
        raise


def log_ingestion_metrics(records_inserted: int, stations_discovered: int, data_type: str):
    """
    Log ingestion metrics consistently across DAGs.
    
    Args:
        records_inserted: Number of records inserted
        stations_discovered: Number of stations discovered
        data_type: Type of data (e.g., 'observations', 'forecasts')
    """
    logger.info(f"{data_type.title()} ingestion completed: {records_inserted} records, {stations_discovered} stations")
