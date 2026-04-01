import datetime as dt
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from sqlalchemy import create_engine


# Подключение к учебной базе PostgreSQL (postgres-training)
DB_URL = "postgresql://student:student@postgres-training:5432/training"


# Базовые параметры DAG
args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 12, 23),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}


def download_titanic_dataset():
    import pandas as pd
    
    """Загрузка датасета Titanic и сохранение в базу"""
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)

    engine = create_engine(DB_URL)
    df.to_sql('titanic', engine, index=False, if_exists='replace', schema='public')


def pivot_dataset():
    import pandas as pd
    
    """Построение сводной таблицы и сохранение результата"""
    engine = create_engine(DB_URL)
    titanic_df = pd.read_sql('select * from public.titanic', con=engine)
    
    df = titanic_df.pivot_table(
        index=['Sex'],
        columns=['Pclass'],
        values='Name',
        aggfunc='count'
    ).reset_index()

    df.to_sql('titanic_pivot', engine, index=False, if_exists='replace', schema='public')


dag = DAG(
    dag_id='titanic_pivot',
    schedule_interval=None,
    default_args=args,
)

# Начальная задача для логирования
start = BashOperator(
    task_id='start',
    bash_command='echo "Начинаем выполнение пайплайна!"',
    dag=dag,
)

# Загрузка исходного датасета
create_titanic_dataset = PythonOperator(
    task_id='download_titanic_dataset',
    python_callable=download_titanic_dataset,
    dag=dag,
)

# Преобразование и сохранение сводной таблицы
pivot_titanic_dataset = PythonOperator(
    task_id='pivot_dataset',
    python_callable=pivot_dataset,
    dag=dag,
)

# Последовательность выполнения
start >> create_titanic_dataset >> pivot_titanic_dataset
