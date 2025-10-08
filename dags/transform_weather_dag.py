"""
Weather data transformation DAG - REBUILT

Orchestrates the transformation pipeline:
1. Clean observations (2 quality checks + 1 validation)
2. Clean forecasts (2 quality checks + 1 validation)
3. Aggregate to postal code level (1h resolution)
4. Validate output quality

Runs: Every hour at :30 (offset from ingestion DAGs)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

from src.transformation.transform import (
    transform_observations_pipeline,
    transform_forecasts_pipeline,
    validate_ml_ready_data
)

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
    'transform_weather_dag',
    default_args=default_args,
    description='Transform raw weather data to ML-ready format',
    schedule_interval='30 * * * *',  # Every hour at :30
    catchup=False,
    max_active_runs=1,
    tags=['transformation', 'ml-ready', 'weather'],
)


def run_observations_transform(**context):
    """
    Transform observations: raw → staging → analytics.
    
    Steps:
    1. Data Cleaning Step 1: Remove incomplete records (>50% missing critical fields)
    2. Data Cleaning Step 2: Flag outliers (physical + statistical limits)
    3. Data Validation: Check temporal consistency
    4. Aggregate to postal code level (1h resolution)
    """
    try:
        logger.info("=" * 60)
        logger.info("OBSERVATIONS TRANSFORMATION PIPELINE")
        logger.info("=" * 60)
        
        pg_hook = PostgresHook(postgres_conn_id='weather_db')
        conn = pg_hook.get_conn()
        
        # Run full transformation pipeline
        stats = transform_observations_pipeline(conn, lookback_hours=168)  # 7 days
        
        conn.close()
        
        logger.info("Observations transformation completed:")
        logger.info(f"  - Raw records processed: {stats['raw_count']}")
        logger.info(f"  - Staging records created: {stats['staging_count']}")
        logger.info(f"  - Postal code records: {stats['postal_code_count']}")
        logger.info(f"  - Avg quality score: {stats['avg_quality_score']:.2f}")
        
        # Push to XCom for monitoring
        context['ti'].xcom_push(key='obs_stats', value=stats)
        
        return stats
        
    except Exception as e:
        logger.error(f"Observations transformation failed: {e}", exc_info=True)
        raise


def run_forecasts_transform(**context):
    """
    Transform forecasts: raw → staging → analytics.
    
    Same pipeline as observations.
    """
    try:
        logger.info("=" * 60)
        logger.info("FORECASTS TRANSFORMATION PIPELINE")
        logger.info("=" * 60)
        
        pg_hook = PostgresHook(postgres_conn_id='weather_db')
        conn = pg_hook.get_conn()
        
        # Run full transformation pipeline
        stats = transform_forecasts_pipeline(conn, lookback_hours=168)  # 7 days
        
        conn.close()
        
        logger.info("Forecasts transformation completed:")
        logger.info(f"  - Raw records processed: {stats['raw_count']}")
        logger.info(f"  - Staging records created: {stats['staging_count']}")
        logger.info(f"  - Postal code records: {stats['postal_code_count']}")
        logger.info(f"  - Avg quality score: {stats['avg_quality_score']:.2f}")
        
        context['ti'].xcom_push(key='fcst_stats', value=stats)
        
        return stats
        
    except Exception as e:
        logger.error(f"Forecasts transformation failed: {e}", exc_info=True)
        raise


def run_quality_validation(**context):
    """
    Validate the final ML-ready datasets.
    
    Checks:
    - Data coverage (% of postal codes with data)
    - Data freshness (latest timestamp)
    - Quality scores (avg, min)
    - Record counts (within expected ranges)
    """
    try:
        logger.info("=" * 60)
        logger.info("ML-READY DATA VALIDATION")
        logger.info("=" * 60)
        
        pg_hook = PostgresHook(postgres_conn_id='weather_db')
        conn = pg_hook.get_conn()
        
        # Run validation
        validation_results = validate_ml_ready_data(conn)
        
        conn.close()
        
        # Check if validation passed
        if not validation_results['passed']:
            logger.error("❌ Data quality validation FAILED")
            for issue in validation_results['issues']:
                logger.error(f"  - {issue}")
            # Don't raise exception - log warnings but continue
        else:
            logger.info("✅ Data quality validation PASSED")
        
        # Log summary
        logger.info("Validation Summary:")
        logger.info(f"  - Observations postal codes: {validation_results['obs_postal_codes']}")
        logger.info(f"  - Forecasts postal codes: {validation_results['fcst_postal_codes']}")
        logger.info(f"  - Observations quality: {validation_results['obs_quality']:.2f}")
        logger.info(f"  - Forecasts quality: {validation_results['fcst_quality']:.2f}")
        
        context['ti'].xcom_push(key='validation', value=validation_results)
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Quality validation failed: {e}", exc_info=True)
        raise


# Task definitions
transform_obs_task = PythonOperator(
    task_id='transform_observations',
    python_callable=run_observations_transform,
    dag=dag,
)

transform_fcst_task = PythonOperator(
    task_id='transform_forecasts',
    python_callable=run_forecasts_transform,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_ml_ready_data',
    python_callable=run_quality_validation,
    dag=dag,
)

# Task dependencies: Transform in parallel, then validate
[transform_obs_task, transform_fcst_task] >> validate_task