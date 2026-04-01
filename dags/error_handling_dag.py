"""
DAG для демонстрации обработки ошибок в Airflow
Уровень: Продвинутый
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import chain
import random

# Определение DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,  # Отключаем email уведомления для простоты
    'email_on_retry': False,
    'retries': 3,  # Количество попыток при ошибке
    'retry_delay': timedelta(seconds=10)  # Задержка между попытками
}

dag = DAG(
    'error_handling_dag',
    default_args=default_args,
    description='DAG для изучения обработки ошибок в Airflow',
    schedule_interval=None,
    catchup=False,
    tags=['educational', 'error_handling', 'advanced']
)

def unreliable_task():
    """Задача, которая может завершиться с ошибкой"""
    # В реальном сценарии это может быть задача, зависящая от внешних факторов
    # Для учебных целей случайным образом генерируем ошибку
    if random.random() < 0.3:  # 30% вероятность ошибки
        print("Ошибка: задача не выполнена успешно!")
        raise Exception("Случайная ошибка в задаче")

    print("Задача выполнена успешно!")
    return "Задача выполнена"

def retry_task():
    """Задача с механизмом повторных попыток"""
    # Имитируем задачу, которая может завершиться с ошибкой, но со временем исправляется
    import time
    time.sleep(2)  # Имитация работы

    # С вероятностью 50% задача завершится с ошибкой
    if random.random() < 0.9:
        print("Ошибка в retry_task!")
        raise Exception("Ошибка в задаче с повторными попытками")

    print("retry_task выполнена успешно!")
    return "retry_task завершена"

def success_handler():
    """Обработчик успешного выполнения"""
    print("Поздравляем! Все задачи выполнены успешно!")
    return "Успешно завершено"

def failure_handler():
    """Обработчик ошибок"""
    print("Одна или несколько задач завершились с ошибкой!")
    print("Проверьте логи для получения дополнительной информации")
    return "Ошибка обработана"

# Определение задач
start_task = DummyOperator(
    task_id='start_task',
    dag=dag
)

unreliable_task = PythonOperator(
    task_id='unreliable_task',
    python_callable=unreliable_task,
    dag=dag
)

retry_task = PythonOperator(
    task_id='retry_task',
    python_callable=retry_task,
    dag=dag
)

success_handler_task = PythonOperator(
    task_id='success_handler',
    python_callable=success_handler,
    trigger_rule='all_success',  # Выполняется только если все предыдущие задачи успешны
    dag=dag
)

failure_handler_task = PythonOperator(
    task_id='failure_handler',
    python_callable=failure_handler,
    trigger_rule='one_failed',  # Выполняется если хотя бы одна предыдущая задача завершилась с ошибкой
    dag=dag
)

end_task = DummyOperator(
    task_id='end_task',
    trigger_rule="all_done",
    dag=dag
)

# Установка зависимостей
# Используем chain, чтобы наглядно показать ученикам построение ветвящихся зависимостей без ручного перечисления операторов.
chain(start_task, [unreliable_task, retry_task], [success_handler_task, failure_handler_task], end_task)

