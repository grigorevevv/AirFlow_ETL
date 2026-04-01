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

def escalation_callback(context):
    """
    Проверяет, какая это попытка падения.
    Если последняя — эскалирует инцидент.
    """
    # Достаем объект TaskInstance
    #ti = context['ti']
    # Текущий номер попытки (начинается с 1)
    current_try = context['ti'].try_number
    
    # В Airflow max_tries равен параметру 'retries'. 
    # Общее число запусков = retries + 1 (первый запуск + перезапуски).
    # Поэтому порог окончательного падения — это max_tries + 1.
    #threshold = ti.max_tries + 1
    
    # Проверяем, достигли ли мы порога
    if current_try >= context['ti'].max_tries:
        logging.critical(
            f"🔥 CRITICAL: ЭСКАЛАЦИЯ! Задача '{context['ti'].task_id}' "
            f"окончательно упала после {current_try} попыток.\n"
            f"Причина: {context.get('exception')}"
        )
        # Здесь обычно добавляют код отправки сообщения в Telegram/Slack
    else:
        # Если это не последняя попытка, пишем обычный ворнинг
        logging.warning(
            f"⚠️ Внимание: Задача '{context['ti'].task_id}' упала (попытка {current_try} из {context['ti'].max_tries}). "
            f"Ждем следующего ретрая."
        ) 

# Определение DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,  # Отключаем email уведомления для простоты
    'email_on_retry': False,
    'retries': 3,  # Количество попыток при ошибке
    'retry_delay': timedelta(seconds=10),  # Задержка между попытками
    'on_failure_callback': escalation_callback # Подключаем нашу функцию Для эскалации при окончательном падении
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
    if random.random() < 0.95:  # 30% вероятность ошибки
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
    if random.random() < 0.3:
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

import logging

   

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
#
