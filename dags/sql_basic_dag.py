"""
DAG для демонстрации работы с SQL в Airflow
Уровень: Начальный-Средний
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

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
    'sql_basic_dag',
    default_args=default_args,
    description='DAG для изучения SQL операций в Airflow',
    schedule_interval=None,
    catchup=False,
    tags=['educational', 'sql', 'beginner']
)

# SQL команды
create_table_sql = """
CREATE TABLE IF NOT EXISTS students_sample (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    age INTEGER,
    created_at TIMESTAMP DEFAULT NOW()   
);
"""

#alter_table_sql = """
#ALTER TABLE students_sample
#    ADD COLUMN email VARCHAR(100),
#    ADD COLUMN phone VARCHAR(15),
#    ADD COLUMN registration_date TIMESTAMP
#;    
#"""

insert_data_sql = """
INSERT INTO students_sample (name, age, email, phone, registration_date)
VALUES 
    ('Ivan Ivanov', 21, 'ivan.ivanov@example.com', '+79001112233', '2023-09-01 10:00:00'),
    ('Maria Petrova', 19, 'm.petrova_99@test.com', '89112223344', '2023-09-05 11:30:00'),
    ('Alex Smith', 22, 'alex_smith@global.com', '+15550102030', '2023-08-20 09:15:00'),
    ('Elena Sidorova', 20, 'elena.sid@work.com ', '79223334455', '2023-09-10 14:00:00'), 
    ('Dmitry Volkov', 23, 'volkov_d@tech.com', '+7(955)1234567', '2023-07-15 16:45:00'),
    ('Anna Kuznechova', 18, 'anna.k@university.com', '88005553535', '2023-09-12 08:00:00'),
    ('Sergey Orlov', 25, 'ORLOV.S@agency.com', '+79998887766', '2023-06-01 12:00:00'), -- Тут верхний регистр
    ('Olga Sokolova', 21, 'o.sokolova@startup.com', '74951234567', '2023-09-03 10:20:00'),
    ('Pavel Morozov', 24, 'pavel-morozov@data.com', '+12025550199', '2023-05-20 17:10:00'),
    ('Yana Belova', 20, 'yana_bel@cloud.com', '89004445566', '2023-09-15 09:00:00')
ON CONFLICT DO NOTHING;
"""   ##### Сгенерировано ИИ #####

query_data_sql = """
SELECT count(*) FROM students_sample;
"""

#truncate_table_sql = """
#TRUNCATE TABLE students_sample;
#"""

# Определение задач
create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_training',  # Это соединение нужно будет создать вручную в Airflow UI
    sql=create_table_sql,
    dag=dag
)

#alter_table_task = PostgresOperator(
#    task_id='alter_table',
#    postgres_conn_id='postgres_training',  
#    retries=0,
#    sql=alter_table_sql,
#    dag=dag
#)

insert_data_task = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id='postgres_training',
    sql=insert_data_sql,
    dag=dag
)

query_data_task = PostgresOperator(
    task_id='query_data',
    postgres_conn_id='postgres_training',
    sql=query_data_sql,
    dag=dag
)

#truncate_table_task = PostgresOperator(
#    task_id='drop_table',
#    postgres_conn_id='postgres_training',
#    sql=truncate_table_sql,
#    dag=dag
#)

# Установка зависимостей
create_table_task >> insert_data_task >> query_data_task 
