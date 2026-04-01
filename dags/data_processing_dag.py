"""
DAG для демонстрации ETL процессов в Airflow
Уровень: Средний-Продвинутый

Этот DAG реализует классический ETL-пайплайн:
1. Создание тестовых данных (клиенты + заказы) в CSV
2. Параллельное извлечение (Extract) клиентов и заказов
3. Трансформация (Transform) — объединение таблиц, вычисляемые поля
4. Загрузка (Load) — подготовка SQL для вставки в БД
5. Генерация текстового отчёта со статистикой

Результат: файлы в /opt/airflow/data/output/ (extracted_*, transformed_data.csv, report.txt).
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

def send_alert_on_failure(context):
    import logging
    """
    Эта функция сработает только если задача упадет.
    Airflow сам передаст сюда аргумент 'context'.
    """
    # Достаем полезную информацию из контекста
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    
    # Достаем сам текст ошибки, из-за которой всё сломалось!
    exception = context.get('exception') 
    log_url = task_instance.log_url

    # Формируем красивое сообщение
    error_message = (
        f"🚨 АЛЕРТ! Ошибка в пайплайне!\n"
        f"DAG: {dag_id}\n"
        f"Упала задача: {task_id}\n"
        f"Причина: {exception}\n"
        f"Ссылка на логи: {log_url}"
    )
    
    # В реальном проекте тут будет код отправки в Telegram/Slack
    logging.error(error_message)

# Определение DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=20),
    'on_failure_callback': send_alert_on_failure
}

dag = DAG(
    'data_processing_dag',
    default_args=default_args,
    description='DAG для изучения ETL процессов в Airflow',
    schedule_interval=None,
    catchup=False,
    tags=['educational', 'etl', 'intermediate']
)

def create_sample_data():
    """Создание примерных данных для ETL процесса"""
    import pandas as pd
    # Создаем файлы с данными клиентов и заказов
    customers_data = {
        'customer_id': [1, 2, 3, 4, 5],
        'name': ['Alice Johnson', 'Bob Smith', 'Charlie Brown', 'Diana Prince', 'Eve Wilson'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'diana@example.com', 'eve@example.com'],
        'join_date': ['2023-01-15', '2023-02-20', '2023-03-10', '2023-04-05', '2023-05-12']
    }
    
    orders_data = {
        'order_id': [101, 102, 103, 104, 105, 106, 107, 108],
        'customer_id': [1, 2, 1, 3, 4, 5, 2, 3],
        'product_id': [201, 202, 203, 204, 205, 206, 202, 204],
        'amount': [1200, 300, 80, 25, 400, 100, 305, 23],
        'order_date': ['2023-10-01', '2023-10-02', '2023-10-03', '2023-10-04', '2023-10-05', '2023-10-06', '2023-09-06', '2023-09-15']
    }

    products_data = {
        'product_id': [201, 202, 203, 204, 205, 206],
        'product_name': ['Laptop', 'Monitor', 'Keyboard', 'Mouse', 'Tablet', 'Headphones'],
        'category': ['Computers', 'Displays', 'Accessories', 'Accessories', 'Mobile Devices', 'Audio'],
        'base_price': [1200, 300, 80, 25, 400, 100]
    }
    
    # Сохраняем в CSV
    pd.DataFrame(customers_data).to_csv('/opt/airflow/data/input/customers.csv', index=False)
    pd.DataFrame(orders_data).to_csv('/opt/airflow/data/input/orders.csv', index=False)
    pd.DataFrame(products_data).to_csv('/opt/airflow/data/input/products.csv', index=False)
    
    print("Созданы файлы с данными клиентов и заказов")
    return "Sample data created"

def extract_customers():
    """Извлечение данных клиентов"""
    import pandas as pd
    df = pd.read_csv('/opt/airflow/data/input/customers.csv')
    print(f"Извлечено {len(df)} записей клиентов")
    
    # Сохраняем извлеченные данные
    df.to_csv('/opt/airflow/data/output/extracted_customers.csv', index=False)
    return f"Извлечено {len(df)} клиентов"

def extract_orders():
    """Извлечение данных заказов"""
    import pandas as pd
    df = pd.read_csv('/opt/airflow/data/input/orders.csv')
    print(f"Извлечено {len(df)} записей заказов")
    
    # Сохраняем извлеченные данные
    df.to_csv('/opt/airflow/data/output/extracted_orders.csv', index=False)
    return f"Извлечено {len(df)} заказов"


def validate_customers():
    """Чтение и валидация CSV файла"""
    import pandas as pd
    
    df = pd.read_csv('/opt/airflow/data/input/customers.csv')

    # Валидация
    assert len(df) > 0, "Файл пустой"
    assert df['customer_id'].notna().all(), "Найдены пустые значения в столбце customer_id"
    assert df['name'].notna().all(), "Найдены пустые значения в столбце name"

    return f"Файл валидирован: {len(df)} записей"

def validate_orders():
    """Чтение и валидация CSV файла"""
    import pandas as pd
    
    df = pd.read_csv('/opt/airflow/data/input/orders.csv')

    # Валидация
    assert len(df) > 0, "Файл пустой"
    assert df['order_id'].notna().all(), "Найдены пустые значения в столбце order_id"
    assert df['customer_id'].notna().all(), "Найдены пустые значения в столбце customer_id"
    assert df['product_id'].notna().all(), "Найдены пустые значения в столбце product_id"
    assert (df['amount'] >= 1).all(), "Найдены заказы с суммой меньше 1"

    return f"Файл валидирован: {len(df)} записей"  


def transform_data():
    """Преобразование данных - объединение клиентов и заказов"""
    import pandas as pd
    customers_df = pd.read_csv('/opt/airflow/data/input/customers.csv')
    orders_df = pd.read_csv('/opt/airflow/data/input/orders.csv')
    products_df = pd.read_csv('/opt/airflow/data/input/products.csv')
    
    # Объединяем данные
    merged_df_1 = pd.merge(orders_df, customers_df, on='customer_id', how='left')
    merged_df = pd.merge(merged_df_1, products_df, on='product_id', how='left')
    
    # Добавляем вычисляемые поля
    merged_df['total_spent'] = merged_df['amount']
    merged_df['order_month'] = pd.to_datetime(merged_df['order_date']).dt.to_period('M')
    
    # Сохраняем преобразованные данные
    merged_df.to_csv('/opt/airflow/data/output/transformed_data.csv', index=False)
    print(f"Преобразованы данные: {len(merged_df)} записей")
    
    return f"Преобразованы {len(merged_df)} записей"

def load_to_database():
    """Загрузка данных в базу данных (симуляция)"""
    import pandas as pd
    df = pd.read_csv('/opt/airflow/data/output/transformed_data.csv')
    
    # В реальном сценарии здесь был бы код для загрузки в базу данных
    # Для учебных целей просто логируем
    print(f"Загружено в базу данных: {len(df)} записей")
    
    # Создаем SQL для создания таблицы (в реальном сценарии)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS customer_orders (
        order_id INTEGER,
        customer_id INTEGER,
        product VARCHAR(100),
        amount DECIMAL(10,2),
        order_date DATE,
        name VARCHAR(100),
        email VARCHAR(100),
        join_date DATE,
        total_spent DECIMAL(10,2),
        order_month INTEGER
    );
    """
    
    print("SQL для создания таблицы:")
    print(create_table_sql)
    
    return f"Подготовлено к загрузке в базу: {len(df)} записей"

def generate_report():
    """Генерация отчетов"""
    import pandas as pd
    df = pd.read_csv('/opt/airflow/data/output/transformed_data.csv')
    
    # Создаем простой отчет
    report = {
        'total_orders': len(df),
        'total_revenue': df['amount'].sum(),
        'avg_order_value': df['amount'].mean(),
        'unique_customers': df['customer_id'].nunique(),
        'top_customer': df.groupby('name')['amount'].sum().idxmax(),
        'top_customer_spending': df.groupby('name')['amount'].sum().max(),
        # Добавляем товары
        'unique_products': df['product_id'].nunique(),
        'total_products': df['product_id'].count(),
        'top_products': df.groupby('product_name')['amount'].sum().idxmax(),
        'top_products_spending': df.groupby('product_name')['amount'].sum().max(),
        'top-3_max_amount': df.nlargest(3, 'amount')[['order_id', 'order_date', 'name', 'product_name', 'amount']],
        'monthly_orders': df.groupby('order_month')['order_id'].count().reset_index(),
        'mean_orders': df.groupby('name')['amount'].mean().reset_index()
    }
    
    # Сохраняем отчет в файл
    with open('/opt/airflow/data/output/report.txt', 'w') as f:
        f.write("Отчет по заказам клиентов\n")
        f.write("=" * 30 + "\n")
        f.write(f"Всего заказов: {report['total_orders']}\n")
        f.write(f"Общая выручка: ${report['total_revenue']}\n")
        f.write(f"Средний чек: ${report['avg_order_value']:.2f}\n")
        f.write(f"Уникальных клиентов: {report['unique_customers']}\n")
        f.write(f"Лучший клиент: {report['top_customer']} (${report['top_customer_spending']})\n")
        f.write("\nОтчет по товарам\n")
        f.write("=" * 30 + "\n")
        f.write(f"Уникальных товаров: {report['unique_products']}\n")
        f.write(f"Всего товаров продана: {report['total_products']}\n")
        f.write(f"Самый дорогой товар: {report['top_products']} (${report['top_products_spending']})\n")
    
    with open('/opt/airflow/data/output/detailed_report.txt', 'w') as f:
        f.write("Детальный отчет по заказам клиентов\n")
        f.write("=" * 30 + "\n")
        f.write(f"топ-3 самых дорогих заказа:\n{report['top-3_max_amount']}\n\n")
        f.write(f"распределение заказов по месяцам:\n{report['monthly_orders']}\n\n")
        f.write(f"среднюю сумму заказа по каждому клиенту:\n{report['mean_orders']}\n\n")
    
    print("Созданы отчеты по заказам")
    return "Отчеты созданы"

# Определение задач
create_data_task = PythonOperator(
    task_id='create_sample_data',
    python_callable=create_sample_data,
    dag=dag
)

extract_customers_task = PythonOperator(
    task_id='extract_customers',
    python_callable=extract_customers,
    dag=dag
)

extract_orders_task = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders,
    dag=dag
)

validate_orders_task = PythonOperator(
    task_id='validate_orders',
    python_callable=validate_orders,
    dag=dag
)

validate_customers_task = PythonOperator(
    task_id='validate_customres',
    python_callable=validate_customers,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag
)

# Установка зависимостей
create_data_task >> [extract_customers_task, extract_orders_task] >> validate_customers_task >> validate_orders_task >> transform_task >> load_task >> report_task