from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

# Определение DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'branching_dag',
    default_args=default_args,
    description='DAG для изучения условного выполнения задач в Airflow',
    schedule_interval=None,
    catchup=False,
    tags=['educational', 'branching', 'advanced']
)

def validate_customers(df: pandas.DataFrame):
    import logging

    # Валидация
    if len(df) < 1:
        logging.info("Файл пустой")
        return False
    
    #assert df['customer_id'].notna().all(), "Найдены пустые значения в столбце customer_id"
    #assert df['name'].notna().all(), "Найдены пустые значения в столбце name"

    logging.info(f"✅ Файл валидирован: {len(df)} записей.")
    return True

def validate_orders(df: pandas.DataFrame):
    import logging

    # Валидация
    if len(df) < 1:
        logging.info("Файл пустой")
        return False
    
    if (df['amount'] <= 1).all():
        logging.info("Найдены заказы с суммой меньше 1")
        return False
    
    # Можно дописать другую валидацию
    #assert df['order_id'].notna().all(), "Найдены пустые значения в столбце order_id"
    #assert df['customer_id'].notna().all(), "Найдены пустые значения в столбце customer_id"
    #assert df['product_id'].notna().all(), "Найдены пустые значения в столбце product_id"

    logging.info(f"✅ Файл валидирован: {len(df)} записей.")
    return True    

def check_data_quality():
    import pandas as pd
    """Проверка качества данных - случайным образом определяет, какие данные использовать"""
    # В реальном сценарии здесь будет проверка качества данных
    # Для учебных целей просто случайное решение
    df_customers = pd.read_csv('/opt/airflow/data/input/customers.csv')
    df_orders = pd.read_csv('/opt/airflow/data/input/orders.csv')

    branches_to_run = []
    
    if validate_customers(df_customers):
        branches_to_run.append('process_customers_branch')
    
    if validate_orders(df_orders):
        branches_to_run.append('process_orders_branch')

    if not branches_to_run:
        return 'process_end_branch' 
            
    return branches_to_run
        

def process_customers():
    """Обработка CSV данных"""
    print("Обработка CSV файла...")
    # Здесь будет логика обработки CSV файла
    return "CSV данные обработаны"

def process_orders():
    """Обработка JSON данных"""
    print("Обработка JSON файла...")
    # Здесь будет логика обработки JSON файла
    return "JSON данные обработаны"

def process_end():
    """Ничего не обработалось, просто завершаем"""
    print("Завершаем...")
    # Здесь будет логика обработки JSON файла
    return "Завершаем процесс"

def merge_results():
    """Объединение результатов из разных веток"""
    print("Объединение результатов из разных веток...")
    return "Результаты объединены"

# Определение задач
start_task = DummyOperator(
    task_id='start_task',
    dag=dag
)

check_quality_task = BranchPythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag
)

process_customers_task = PythonOperator(
    task_id='process_customers_branch',
    python_callable=process_customers,
    dag=dag
)

process_orders_task = PythonOperator(
    task_id='process_orders_branch',
    python_callable=process_orders,
    dag=dag
)

process_end_task = PythonOperator(
    task_id='process_end_branch',
    python_callable=process_end,
    dag=dag
)

merge_task = PythonOperator(
    task_id='merge_results',
    python_callable=merge_results,
    trigger_rule='none_failed_or_skipped',  # Выполняется, когда одна из веток завершена
    dag=dag
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag
)

# Установка зависимостей
# BranchPythonOperator автоматически пропускает (skips) задачи в невыбранной ветке
# Например, если check_data_quality возвращает 'process_csv_branch',
# то задача process_json_branch будет пропущена (статус skipped)
start_task >> check_quality_task
check_quality_task >> [process_customers_task, process_orders_task, process_end_task]
[process_customers_task, process_orders_task, process_end_task] >> merge_task >> end_task