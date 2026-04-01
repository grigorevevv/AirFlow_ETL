# Educational Airflow Setup for Beginners

Простой учебный стенд Apache Airflow для начинающих, изучающих SQL и Python.

## 🎯 Цель проекта

Создать максимально простую среду для изучения Apache Airflow.
Для удобства все переменные окружения захардкожены в docker скрипты. На проде так делать не надо :)

## 📋 Предварительные требования

- Docker и Docker Compose
- Базовые знания Python и SQL
- Веб-браузер для доступа к интерфейсу Airflow

## 🚀 Быстрый старт

### 1. Запуск стенда

```bash
# Перейдите в директорию проекта
cd airflow-docker

# Запустите все сервисы
docker-compose up -d
```

### 2. Доступ к интерфейсам

- **Airflow UI**: http://localhost:8080
  - Логин: `admin`
  - Пароль: `admin`

- **PostgreSQL для тренировок**: `localhost:5432`
  - База данных: `training`
  - Пользователь: `student`
  - Пароль: `student`

- **PostgreSQL для метаданных Airflow**: `localhost:5434`
  - База данных: `airflow`
  - Пользователь: `airflow`
  - Пароль: `airflow`

## 🏗️ Архитектура стенда

```mermaid
graph LR

    %% Локальная файловая система
    subgraph local["Локальная файловая система"]
        dags["Каталог<br/>Educational DAGs"]
        fs["Local Filesystem"]
        dags --> fs
    end

    %% Docker-контейнеры
    subgraph docker["Контейнеры Docker"]
        subgraph airflow["Airflow"]
            ws["Airflow Webserver"]
            sch["Airflow Scheduler"]
            meta["PostgreSQL Metadata"]
            sch --> meta
            ws --> meta
        end

        subgraph training["Учебная БД"]
            train["PostgreSQL Training"]
        end
    end

    %% Клиенты
    subgraph clients["Клиенты"]
        user["Пользователь<br/>(браузер)"]
        sql["SQL клиент"]
    end

    %% Монтирование DAG'ов в контейнеры Airflow
    dags -. "том с DAG-файлами" .- ws
    dags -. "том с DAG-файлами" .- sch

    %% Airflow-задачи работают с учебной БД
    sch -- "задачи DAG" --> train

    %% Внешние подключения
    user -- "порт 8080" --> ws
    sql -- "порт 5432" --> train
```

## 📁 Структура проекта

```
airflow-docker/
├── docker-compose.yml          # Конфигурация Docker
├── .env                        # Файл не используется: переменные заданы в docker-compose.yml
├── dags/                       # DAG файлы для обучения
│   ├── hello_world_dag.py      # Базовый пример
│   ├── sql_basic_dag.py        # Работа с SQL
│   ├── file_operations_dag.py  # Обработка файлов
│   ├── csv_to_postgres.py      # Загрузка CSV в Postgres (ETL)
│   ├── csv_to_postgres_dq.py   # Проверки качества данных (DQ)
│   ├── data_processing_dag.py  # Сложный ETL пайплайн
│   ├── branching_dag.py        # Условная логика
│   └── error_handling_dag.py   # Обработка ошибок
├── data/                       # Данные и артефакты прогонов
│   ├── input/                  # Входные данные
│   └── output/                 # Сгенерированные CSV, отчеты и результаты обработки
├── logs/                       # Логи Airflow
├── README.md                   # Эта инструкция
└── educational-tasks.md        # Практические задания для студентов
```

## 🎓 Учебные материалы

### Основы Airflow

**Цели:**
- Понимание структуры DAG
- Создание простых задач
- Настройка зависимостей между задачами

**Примеры DAG:**
- `hello_world_dag.py` - базовые операторы Python
- `sql_basic_dag.py` - работа с базами данных

### Интеграция с данными

**Цели:**
- Подключение к PostgreSQL
- Выполнение SQL запросов
- Обработка файлов CSV

**Примеры DAG:**
- `file_operations_dag.py` - работа с файлами
- `csv_to_postgres.py` - загрузка данных из CSV в PostgreSQL
- `csv_to_postgres_dq.py` - автоматизированные проверки качества (Data Quality)
- `data_processing_dag.py` - ETL процессы

### Продвинутые возможности

**Цели:**
- Условное выполнение задач
- Обработка ошибок
- Параметризация workflows

**Примеры DAG:**
- `branching_dag.py` - условная логика
- `error_handling_dag.py` - обработка ошибок

##  🎯 Практические задания

Для закрепления знаний по каждому DAG мы подготовили практические задания разного уровня сложности:

### 📋 Учебные задания

В файле **[educational-tasks.md](educational-tasks.md)** вы найдете детальные задания для каждого DAG:

-  🌱 **Начальный уровень**: Базовые задачи по созданию и настройке DAG'ов
-  📚 **Средний уровень**: Работа с данными, файлами и SQL операциями
-  🚀 **Продвинутый уровень**: ETL процессы, ветвление, обработка ошибок

###  🎓 Рекомендуемый путь обучения

1. **Начните с `hello_world_dag.py`** - освоите основы Airflow
2. **Перейдите к `sql_basic_dag.py`** - изучите работу с базами данных
3. **Попрактикуйтесь на `file_operations_dag.py`** - работа с файлами
4. **Освойте ETL и DQ на `csv_to_postgres.py` и `csv_to_postgres_dq.py`** - загрузка и валидация данных
5. **Разберите сложный ETL на `data_processing_dag.py`** - обработка данных
6. **Изучите продвинутые темы** - ветвление и обработка ошибок

Каждое задание содержит:
- Цель и сложность выполнения
- Конкретные требования к модификации кода
- Ожидаемый результат
- Время на выполнение

## 🔧 Технические детали

### Переменные окружения

В этом проекте мы не используем `.env`: все значения заданы напрямую в `docker-compose.yml`.

Сервисы Airflow получают:
- `POSTGRES_TRAINING_HOST=postgres-training`
- `POSTGRES_TRAINING_PORT=5432`
- `POSTGRES_TRAINING_DB=training`
- `POSTGRES_TRAINING_USER=student`
- `POSTGRES_TRAINING_PASSWORD=student`
- `AIRFLOW_CONN_POSTGRES_TRAINING=postgresql://student:student@postgres-training:5432/training`

`docker-compose run --rm airflow-init` запускает `airflow connections add postgres_training`, поэтому соединение доступно сразу после инициализации. Проверить наличие можно через:

```bash
docker-compose exec airflow-webserver airflow connections get postgres_training
```

`csv_to_postgres.py` по умолчанию складывает сгенерированные CSV в `/opt/airflow/data/output`, то есть в локальный каталог `airflow-docker/data/output/`.


### Порты

- `8080` - Airflow Webserver
- `5432` - PostgreSQL для тренировок
- `5434` - PostgreSQL для метаданных Airflow

## 🛠️ Управление стендом

### Запуск сервисов
```bash
docker-compose up -d
```

### Остановка сервисов
```bash
docker-compose down
```

### Просмотр логов
```bash
# Логи Airflow
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler

# Логи PostgreSQL
docker-compose logs postgres-training
docker-compose logs postgres-metadata
```

### Перезапуск конкретного сервиса
```bash
docker-compose restart airflow-webserver
```

## 🐛 Решение проблем

### DAG не появляется в интерфейсе
- Проверьте, что файл находится в папке `dags/`
- Убедитесь в правильности синтаксиса Python
- Проверьте логи планировщика: `docker-compose logs airflow-scheduler`

### Ошибки подключения к базе данных
- Убедитесь, что PostgreSQL запущен: `docker-compose ps`
- Проверьте логи PostgreSQL: `docker-compose logs postgres-training`

### Задачи завершаются с ошибкой
- Проверьте логи задачи в интерфейсе Airflow
- Убедитесь в наличии необходимых Python пакетов

## 📚 Дополнительные ресурсы

- [Официальная документация Airflow](https://airflow.apache.org/docs/)
- [Учебные материалы в родительской папке](../)
- [Практические задания для студентов](educational-tasks.md)

---

##  🌟 Начало работы с заданиями

После запуска стенда Airflow, откройте файл [educational-tasks.md](educational-tasks.md) и выберите задание, соответствующее вашему уровню подготовки. Каждое задание содержит подробные инструкции и ожидаемые результаты.

**Быстрый старт с заданиями:**
```bash
# Запустите стенд
docker-compose up -d

# Откройте задания в браузере или редакторе
# И следуйте инструкциям из educational-tasks.md
```

Удачи в изучении Apache Airflow!

---

**Примечание**: Этот стенд предназначен исключительно для учебных целей. Для production использования требуется дополнительная настройка безопасности.
