import datetime as dt
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


from sqlalchemy import create_engine


# Подключение к учебной базе PostgreSQL (postgres-training)
#DB_URL = "postgresql://student:student@postgres-training:5432/training"  Данные берутся из подключения созданное в UI

# Базовые параметры DAG
args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 12, 23),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

def create_dataset():
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook  #Добавляем подключение созданное в UI
    
    # 1. Создаем объект хука, передавая ID нашего подключения из UI
    hook = PostgresHook(postgres_conn_id='postgres_training_2')
    
    # 2. Хук сам достает логин, пароль, хост и делает нам готовый engine!
    engine = hook.get_sqlalchemy_engine()
    
    sql_query = """
        SELECT 'Количество студентов ' AS total, count(s.name) AS values
        FROM students_sample s
        UNION ALL
        SELECT 'Cредний возраст' AS total, ROUND(AVG(age), 2)
        FROM students_sample
    """

    students_df = pd.read_sql(sql_query, con=engine)
    
    students_df.to_csv('/opt/airflow/data/output/students_report2.csv', index=False, sep=';', encoding='utf-8')


dag = DAG(
    dag_id='students_report',
    schedule_interval=None,
    default_args=args,
)

# Начальная задача для логирования
start = BashOperator(
    task_id='start',
    bash_command='echo "Начинаем выполнение пайплайна!"',
    dag=dag,
)

create_students_dataset = PythonOperator(
    task_id='create_students_dataset',
    python_callable=create_dataset,
    dag=dag,
)

# Последовательность выполнения
start >> create_students_dataset