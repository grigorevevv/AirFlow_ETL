from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Определение DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'file_operations_dag',
    default_args=default_args,
    description='DAG для изучения работы с файлами в Airflow',
    schedule_interval=None,
    catchup=False,
    tags=['educational', 'files', 'intermediate']
)

def generate_sample_data():
    """Создание CSV файла с примерными данными"""
    import pandas as pd
    import random
    from mimesis import Person, Address, Numeric
    from mimesis.enums import Gender
    from mimesis.locales import Locale

    # Создаем генераторы с русской локализацией
    person = Person(Locale.RU)
    address = Address(Locale.RU)

    # Генерируем данные
    names = [person.full_name(gender=Gender.FEMALE if i % 3 == 0 else Gender.MALE) for i in range(1, 101)]
    numeric = Numeric()
    ages = [numeric.integer_number(start=18, end=65) for _ in range(100)]  # Используем правильный метод для генерации возраста
    occupations = [person.occupation() for _ in range(100)]
    addres = [f'{address.city()} {address.address()}' for _ in range(100)]
    departments = ['ЗПэшечка','Капитаны счетов', 'Казна', 'Финансы', 'Торговцы', 'Продавцы', 'Поддержка']
    experience = [numeric.integer_number(start=1, end=(ages[i] - 17)) for i in range(100)]

    data = {
        'id': range(1, 101),
        'name': names,
        'age': ages,
        'salary': [max(30000, min(150000, ages[i] * 1200 + (hash(names[i]) % 15000) - 5000)) for i in range(100)],
        'occupations' : occupations,
        'address': addres,
        'education_level': [person.academic_degree() for _ in range(100)],
        'department': [random.choice(departments) for _ in range(100)],
        'experience_years': experience
    }

    df = pd.DataFrame(data)
    df.to_csv('/opt/airflow/data/input/sample_data.csv', index=False)
    print(f"Создан файл с {len(df)} записями")
    print(f"Пример данных:")
    print(f"    имя: {data['name'][0]},")
    print(f"    возраст: {data['age'][0]},")
    print(f"    зарплата: {data['salary'][0]},")
    print(f"    адрес: {data['address'][0]},")
    print(f"    образование: {data['education_level'][0]},")
    print(f"    отдел: {data['department'][0]}")
    print(f"    опыт работы: {data['experience_years'][0]}")
    print(df.info())
    return "sample_data.csv created"
    
def read_and_validate_data():
    """Чтение и валидация CSV файла"""
    import pandas as pd
    
    df = pd.read_csv('/opt/airflow/data/input/sample_data.csv')
    print(f"Прочитан файл: {len(df)} строк, {len(df.columns)} столбцов")
    print(f"Колонки: {list(df.columns)}")

    # Простая валидация
    assert len(df) > 0, "Файл пустой"
    assert 'name' in df.columns, "Отсутствует столбец name"
    assert 'address' in df.columns, "Отсутствует столбец address"
    
    # Дополнительная валидация для новых данных
    assert df['name'].notna().all(), "Найдены пустые значения в столбце name"
    assert df['address'].notna().all(), "Найдены пустые значения в столбце address"
    assert df['education_level'].notna().all(), "Найдены пустые значения в столбце education_level"
    assert df['department'].notna().all(), "Найдены пустые значения в столбце department"
    
    assert df['age'].between(16, 100).all(), "Некорректные значения возраста"
    assert df['salary'].between(20000, 150000).all(), "Некорректные значения зарплаты"
    assert df['experience_years'].between(0, 48).all(), "Некорректные значения опыта работы"
    
    return f"Файл валидирован: {len(df)} записей"    

def transform_data():
    """Преобразование данных"""
    #import os
    import pandas as pd
    
    df = pd.read_csv('/opt/airflow/data/input/sample_data.csv')
    
    # Простое преобразование - добавим столбец с категорией зарплаты
    df['salary_category'] = df['salary'].apply(
        lambda x: 'High' if x >= 70000 else 'Medium' if x >= 50000 else 'Low'
    )
    
    # Добавим дополнительные трансформации для новых данных
    # Извлечем город из адреса (первое слово в адресе часто является городом)
    df['city'] = df['address'].str.split().str[0]
    # Преобразуем возраст в возрастную категорию
    df['age_category'] = df['age'].apply(
        lambda x: 'Young' if x < 30 else 'Middle-aged' if x < 50 else 'Senior'
    )
    
    df['experienced_person'] = df['experience_years'].apply(
        lambda x: 'Junior' if x < 3  else 'Middle' if x < 7 else 'Senior'
    )
    
    # Создаем директорию, если она не существует
    #os.makedirs('/opt/airflow/data/output', exist_ok=True)
    
    # Сохраняем обработанные данные
    df.to_csv('/opt/airflow/data/output/processed_data.csv', index=False)
    print(f"Обработаны данные: {len(df)} записей")
    print(f"Добавлены новые столбцы: city, age_category")
    
    return f"Данные обработаны: {len(df)} записей"

def write_summary():
    import pandas as pd
    """Создание сводки по обработанным данным"""
    
    df = pd.read_csv('/opt/airflow/data/output/processed_data.csv')
    
    summary = {
        'total_records': len(df),
        'avg_salary': df['salary'].mean(),
        'avg_age': df['age'].mean(),
        'high_salary_count': len(df[df['salary_category'] == 'High']),
        'medium_salary_count': len(df[df['salary_category'] == 'Medium']),
        'low_salary_count': len(df[df['salary_category'] == 'Low']),
        'young_count': len(df[df['age_category'] == 'Young']),
        'middle_aged_count': len(df[df['age_category'] == 'Middle-aged']),
        'senior_count': len(df[df['age_category'] == 'Senior']),
        'unique_cities': df['city'].nunique(),
        'top_cities': df['city'].value_counts().head(5).to_dict()
    }
    
    # Сохраняем сводку в текстовый файл
    with open('/opt/airflow/data/output/summary.txt', 'w') as f:
        f.write("Сводка по обработанным данным:\n")
        f.write(f"Всего записей: {summary['total_records']}\n")
        f.write(f"Средняя зарплата: {summary['avg_salary']:.2f} рублей\n")
        f.write(f"Средний возраст: {summary['avg_age']:.2f} лет\n")
        f.write(f"Высокая зарплата: {summary['high_salary_count']} чел.\n")
        f.write(f"Средняя зарплата: {summary['medium_salary_count']} чел.\n")
        f.write(f"Низкая зарплата: {summary['low_salary_count']} чел.\n")
        f.write(f"Молодые (до 30): {summary['young_count']} чел.\n")
        f.write(f"Среднего возраста (30-49): {summary['middle_aged_count']} чел.\n")
        f.write(f"Пожилые (50+): {summary['senior_count']} чел.\n")
        f.write(f"Уникальных городов: {summary['unique_cities']}\n")
        f.write(f"Топ-5 городов: {summary['top_cities']}\n")
    
    print("Создана сводка по данным")
    return "Сводка создана"


# Определение задач
generate_task = PythonOperator(
    task_id='generate_sample_data',
    python_callable=generate_sample_data,
    dag=dag
)

read_task = PythonOperator(
    task_id='read_and_validate_data',
    python_callable=read_and_validate_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

summary_task = PythonOperator(
    task_id='write_summary',
    python_callable=write_summary,
    dag=dag
)

# Установка зависимостей
generate_task >> read_task >> transform_task >> summary_task