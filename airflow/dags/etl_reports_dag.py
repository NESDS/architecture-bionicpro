"""
ETL DAG для формирования витрины отчётов BionicPRO.

Процесс:
1. EXTRACT: Извлечение данных из CRM (клиенты) и PostgreSQL (телеметрия)
2. TRANSFORM: Объединение и агрегация данных по пользователям
3. LOAD: Запись в ClickHouse (витрина reports_mart)

Расписание: ежедневно в 02:00
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import logging

logger = logging.getLogger(__name__)

# Конфигурация ClickHouse
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_PORT = os.getenv('CLICKHOUSE_PORT', '8123')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DATABASE', 'bionicpro')
CLICKHOUSE_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"

default_args = {
    'owner': 'bionicpro',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}


def execute_clickhouse_query(query: str) -> str:
    """Выполнение запроса к ClickHouse через HTTP."""
    response = requests.post(
        CLICKHOUSE_URL,
        params={'database': CLICKHOUSE_DB},
        data=query,
        headers={'Content-Type': 'text/plain'}
    )
    if response.status_code != 200:
        raise Exception(f"ClickHouse error: {response.text}")
    return response.text


def create_tables(**context):
    """Создание таблиц в ClickHouse если не существуют."""
    
    # Витрина отчётов
    execute_clickhouse_query('''
        CREATE TABLE IF NOT EXISTS reports_mart (
            user_id String,
            username String,
            email String,
            first_name String,
            last_name String,
            prosthetic_model String,
            report_date Date,
            total_usage_hours Float64,
            movement_count UInt32,
            avg_response_time_ms Float64,
            battery_cycles UInt32,
            calibration_count UInt32,
            last_sync_at DateTime,
            etl_processed_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (user_id, report_date)
    ''')
    
    logger.info("Tables created successfully")


def extract_crm_data(**context):
    """
    Извлечение данных о клиентах из CRM.
    В демо-режиме используем mock данные.
    """
    execution_date = context['execution_date']
    
    # Mock данные клиентов (в реальности - запрос к CRM API)
    customers = [
        {
            'user_id': 'prothetic1',
            'username': 'prothetic1',
            'email': 'prothetic1@example.com',
            'first_name': 'Prothetic',
            'last_name': 'One',
            'prosthetic_model': 'BionicHand Pro v2'
        },
        {
            'user_id': 'prothetic2',
            'username': 'prothetic2',
            'email': 'prothetic2@example.com',
            'first_name': 'Prothetic',
            'last_name': 'Two',
            'prosthetic_model': 'BionicArm Elite v1'
        },
        {
            'user_id': 'prothetic3',
            'username': 'prothetic3',
            'email': 'prothetic3@example.com',
            'first_name': 'Prothetic',
            'last_name': 'Three',
            'prosthetic_model': 'BionicHand Pro v3'
        }
    ]
    
    context['ti'].xcom_push(key='crm_customers', value=customers)
    logger.info(f"Extracted {len(customers)} customers from CRM")


def extract_telemetry(**context):
    """
    Извлечение данных телеметрии.
    В демо-режиме генерируем mock данные.
    """
    execution_date = context['execution_date']
    
    # Mock данные телеметрии
    telemetry = {
        'prothetic1': {
            'usage_hours': 6.5,
            'movement_count': 520,
            'response_time_ms': 65.3,
            'battery_cycles': 1,
            'calibration_count': 0
        },
        'prothetic2': {
            'usage_hours': 5.2,
            'movement_count': 410,
            'response_time_ms': 72.1,
            'battery_cycles': 1,
            'calibration_count': 0
        },
        'prothetic3': {
            'usage_hours': 7.8,
            'movement_count': 620,
            'response_time_ms': 58.9,
            'battery_cycles': 1,
            'calibration_count': 1
        }
    }
    
    context['ti'].xcom_push(key='telemetry_data', value=telemetry)
    logger.info(f"Extracted telemetry for {len(telemetry)} users")


def transform_and_load(**context):
    """
    Трансформация и загрузка данных в витрину reports_mart.
    """
    execution_date = context['execution_date']
    report_date = execution_date.strftime('%Y-%m-%d')
    
    customers = context['ti'].xcom_pull(key='crm_customers')
    telemetry = context['ti'].xcom_pull(key='telemetry_data')
    
    # Удаление данных за текущую дату (идемпотентность)
    execute_clickhouse_query(f"ALTER TABLE reports_mart DELETE WHERE report_date = '{report_date}'")
    
    # Вставка данных
    for customer in customers:
        user_id = customer['user_id']
        tel = telemetry.get(user_id, {})
        
        query = f"""
            INSERT INTO reports_mart (
                user_id, username, email, first_name, last_name, prosthetic_model,
                report_date, total_usage_hours, movement_count, avg_response_time_ms,
                battery_cycles, calibration_count, last_sync_at
            ) VALUES (
                '{customer['user_id']}',
                '{customer['username']}',
                '{customer['email']}',
                '{customer['first_name']}',
                '{customer['last_name']}',
                '{customer['prosthetic_model']}',
                '{report_date}',
                {tel.get('usage_hours', 0)},
                {tel.get('movement_count', 0)},
                {tel.get('response_time_ms', 0)},
                {tel.get('battery_cycles', 0)},
                {tel.get('calibration_count', 0)},
                now()
            )
        """
        execute_clickhouse_query(query)
        logger.info(f"Inserted data for user {user_id}")
    
    logger.info(f"Loaded {len(customers)} records to reports_mart for {report_date}")


def validate_data(**context):
    """Валидация загруженных данных."""
    execution_date = context['execution_date']
    report_date = execution_date.strftime('%Y-%m-%d')
    
    result = execute_clickhouse_query(f"""
        SELECT count(*) FROM reports_mart WHERE report_date = '{report_date}'
    """)
    
    count = int(result.strip())
    
    if count == 0:
        raise ValueError(f"No records loaded for {report_date}")
    
    logger.info(f"Validation passed: {count} records for {report_date}")
    return count


# Определение DAG
with DAG(
    dag_id='etl_reports_bionicpro',
    default_args=default_args,
    description='ETL процесс для формирования витрины отчётов BionicPRO',
    schedule_interval='0 2 * * *',  # Ежедневно в 02:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bionicpro', 'etl', 'reports'],
) as dag:
    
    # Задача 1: Создание таблиц
    task_create_tables = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables,
    )
    
    # Задача 2: Извлечение данных из CRM
    task_extract_crm = PythonOperator(
        task_id='extract_crm_data',
        python_callable=extract_crm_data,
    )
    
    # Задача 3: Извлечение телеметрии
    task_extract_telemetry = PythonOperator(
        task_id='extract_telemetry',
        python_callable=extract_telemetry,
    )
    
    # Задача 4: Трансформация и загрузка
    task_transform_load = PythonOperator(
        task_id='transform_and_load',
        python_callable=transform_and_load,
    )
    
    # Задача 5: Валидация
    task_validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )
    
    # Определение зависимостей
    task_create_tables >> [task_extract_crm, task_extract_telemetry]
    [task_extract_crm, task_extract_telemetry] >> task_transform_load >> task_validate
