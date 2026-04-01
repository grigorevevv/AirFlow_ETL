from __future__ import annotations

from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG

POSTGRES_CONN_ID = "postgres_training"

EXPECTED_ORDERS_SCHEMA = [
    ("order_id", "bigint"),
    ("order_ts", "timestamp without time zone"),
    ("customer_id", "bigint"),
    ("amount", "numeric"),
    ("status", "character varying"),
]


def _get_conn():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()


def _check_table_exists():
    import logging
    
    """Проверяет наличие таблицы public.orders."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM pg_catalog.pg_tables
                WHERE schemaname = 'public' AND tablename = 'orders'
            """)
            if cur.fetchone() is None:
                raise ValueError("Таблица public.orders не найдена")
        logging.info("Таблица public.orders существует")
    finally:
        conn.close()


def _check_schema():
    import logging
    
    """Проверяет соответствие схемы таблицы public.orders ожидаемой."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'orders'
                ORDER BY ordinal_position
            """)
            actual = cur.fetchall()
        if actual != EXPECTED_ORDERS_SCHEMA:
            raise ValueError(
                f"Схема не совпадает. Ожидалось: {EXPECTED_ORDERS_SCHEMA}, "
                f"получено: {actual}"
            )
        logging.info("Схема таблицы public.orders соответствует ожидаемой")
    finally:
        conn.close()


def _check_has_rows():
    import logging
    
    """Проверяет, что в таблице public.orders есть данные."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.orders")
            count = cur.fetchone()[0]
        if count == 0:
            raise ValueError("Таблица public.orders пуста")
        logging.info("Таблица public.orders содержит %s строк", count)
    finally:
        conn.close()


def _check_no_duplicates():
    import logging
    
    """Проверяет отсутствие дубликатов order_id в public.orders."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT order_id, COUNT(*) AS cnt
                FROM public.orders
                GROUP BY order_id
                HAVING COUNT(*) > 1
            """)
            duplicates = cur.fetchall()
        if duplicates:
            raise ValueError(f"Обнаружены дубликаты order_id: {duplicates}")
        logging.info("Дубликатов order_id не обнаружено")
    finally:
        conn.close()

def _check_amount():
    import logging
    
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT amount
                FROM public.orders
                WHERE amount < 0.01
            """) 
            amount_zero = cur.fetchall()   
        if amount_zero:
            raise ValueError(f"Обнаружены значения amount равные нулю: {amount_zero}")  
        logging.info("Значения amount все правильны")            
    finally:
        conn.close()    

def _log_dq_summary(**context):
    import logging

    """Логирует итоговую сводку по качеству данных."""
    # Достаем информацию о текущем запуске DAG'а
    dag_run = context['dag_run']
    
    # Получаем список всех задач в этом запуске
    task_instances = dag_run.get_task_instances()
    
    task_err = True
    
    for ti in task_instances:
        logging.info(f"Задача: {ti.task_id} | Статус: {ti.state}")
        
        if ti.state == 'failed':
            logging.error(f"Упала задача: {ti.task_id}")
            task_err = False
    
    if task_err:
        logging.info("Все проверки качества данных пройдены успешно!")
        logging.info("Качество данных в таблице orders соответствует требованиям.")
    else:
        logging.info("ОШИБКИ! проверь задачи с ошибками")


default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(seconds=10)}

with DAG(
    dag_id="csv_to_postgres_dq",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["demo", "postgres", "quality", "csv", "dq"],
    description="Проверки качества данных после CSV -> public.orders в Postgres",
) as dag:
    check_exists = PythonOperator(
        task_id="check_orders_table_exists",
        python_callable=_check_table_exists,
    )

    check_schema = PythonOperator(
        task_id="check_orders_schema",
        python_callable=_check_schema,
    )

    check_has_rows = PythonOperator(
        task_id="check_orders_has_rows",
        python_callable=_check_has_rows,
    )

    check_no_duplicates = PythonOperator(
        task_id="check_order_duplicates",
        python_callable=_check_no_duplicates,
    )
    
    check_amount = PythonOperator(
        task_id="check_amount",
        python_callable=_check_amount,
    )

    dq_summary = PythonOperator(
        task_id="data_quality_summary",
        python_callable=_log_dq_summary,
        trigger_rule="all_done",
    )

check_exists >> check_schema >> check_has_rows >> [check_no_duplicates, check_amount] >> dq_summary
