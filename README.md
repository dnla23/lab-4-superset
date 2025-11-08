# Л+П №4. Разработка аналитического дашборда для бизнес-кейса 
Дисциплина: Анализ больших данных и рыночных тенденций

Направление: Бизнес-информатика (магистратура)

Вариант 25

ФИО студента: Шитов Данила Олегович

ФИО преподователя: Босенко Тимур Муртазович

**Бизнес-логика расчета**

```
inauguration_age = EXTRACT(YEAR FROM AGE(start_date, birth_date))
years_in_office = EXTRACT(YEAR FROM AGE(end_date, start_date))
```

**Ожидаемый результат.** Интерактивный дашборд с визуализацией ключевых метрик президентства.

## Подробная архитектура решения

### Общая схема системы

![Архитектура](./img/arch.jpg)


### ETL-процесс

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Kaggle    │───▶│   Airflow   │───▶│ PostgreSQL │──▶│  Superset   │
│   Dataset   │    │   Extract   │    │   Load      │    │ Visualize   │
│             │    │   Transform │    │   Transform │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

![Схема конвеера ETL](./img/conveer.jpg)

### Подключение Airflow к PostgreSQL

**Строка подключения**: `postgresql://airflow:airflow@postgres:5432/airflow`

**Параметры подключения**:
- **Host**: postgres
- **Port**: 5432
- **Database**: airflow
- **Username**: airflow
- **Password**: airflow

### Настройка коннектора analytics_postgres

**Настройки для коннектора analytics_postgres**

| Поле в Airflow | Значение | Откуда взято (из docker-compose.yml) |
|---|---|---|
| Connection Id | analytics_postgres | Имя, используемое в DAG |
| Connection Type | Postgres | Тип базы данных |
| Host | analytics_postgres | Имя сервиса в Docker Compose |
| Schema | analytics | Из environment: POSTGRES_DB=analytics |
| Login | analytics | Из environment: POSTGRES_USER=analytics |
| Password | analytics | Из environment: POSTGRES_PASSWORD=analytics |
| Port | 5432 | Внутренний порт контейнера |

**Пошаговая инструкция**:
1. Зайдите в Airflow UI: http://localhost:8080
2. Перейдите в Admin -> Connections
3. Нажмите синюю кнопку + ("Add a new record")
4. Заполните поля формы согласно таблице выше
5. Нажмите кнопку Test (должно появиться "Connection successfully tested")
6. Нажмите Save

## Технологический стек

### Основные компоненты

*   **Apache Airflow 2.5.0** — оркестрация ETL-процессов, управление задачами
*   **PostgreSQL 12** — хранение данных и аналитическая витрина
*   **Apache Superset 3.1.1** — интерактивная визуализация и дашборды (стабильная версия)
*   **Kaggle API** — извлечение данных с платформы Kaggle
*   **pgAdmin 4** — веб-интерфейс для администрирования PostgreSQL
*   **Redis 7** — кэширование и сессии для Superset
*   **Docker & Docker Compose** — контейнеризация и оркестрация сервисов

### Python библиотеки

*   **pandas** — обработка и анализ данных
*   **kaggle** — работа с Kaggle API
*   **kagglehub** — скачивание датасетов
*   **psycopg2-binary** — подключение к PostgreSQL
*   **apache-airflow-providers-postgres** — интеграция Airflow с PostgreSQL

### Инфраструктура

*   **Кастомный Dockerfile** — образ Airflow с установленным gcc для компиляции пакетов
*   **Docker Volumes** — постоянное хранение данных
*   **Docker Networks** — изолированная сеть для сервисов

## Быстрый старт

### 1. Настройка Kaggle API
```bash
chmod +x setup_kaggle.sh
./setup_kaggle.sh
```

### 2. Запуск проекта
```bash
    sudo docker compose up -d
```

### 3. Проверка статуса

```bash
sudo docker compose ps
```

### 4. Доступ к сервисам
- **Airflow**: http://localhost:8081 (admin/admin)
- **pgAdmin**: http://localhost:5050 (admin@admin.com/admin)
- **Superset**: http://localhost:8088 (admin/admin)

### 5. Работа с данными
1. В Airflow запустите DAG `us_presidents_analysis`
2. В Superset подключитесь к базе `analytics_postgres`
3. Создайте дашборд с данными из `stg_us_presidents`

## Настройка Apache Superset

**Примечание**. В проекте используется Apache Superset версии 3.1.1 (стабильная версия). Версия `latest` не рекомендуется для продакшена из-за возможных нестабильностей.

### Шаг 1: Подключение к базе данных

1. Откройте Superset: http://localhost:8088
2. Перейдите к подключениям: ⚙️ Settings -> Data
3. Добавьте базу данных: Нажмите + Create Dataset
4. Заполните форму "Connect a database":

![Подключение к базе данных](./img/create_conn_superset_sql_01.jpg)

| Поле | Значение | Объяснение |
|---|---|---|
| Host | analytics_postgres | Имя сервиса базы данных в docker-compose.yml |
| Port | 5432 | Внутренний порт контейнера PostgreSQL |
| Database name | analytics | Из переменной POSTGRES_DB=analytics |
| Username | analytics | Из переменной POSTGRES_USER=analytics |
| Password | analytics | Из переменной POSTGRES_PASSWORD=analytics |
| Display Name | Analytics DB | Удобное имя для подключения |

![Подключение к базе данных](./img/create_conn_superset_sql_02.jpg)

### Шаг 2. Создание датасета

1. В верхнем меню нажмите + и выберите Dataset
2. В появившемся окне выберите:
   - DATABASE. Ваше подключение analytics
   - SCHEMA. public
   - SEE TABLE/VIEW. stg_us_presidents
3. Нажмите ADD

### Шаг 3: Создание графиков

**График 1. Индикатор "Общее количество президентов" (Big Number)**
1. Перейдите в Charts и нажмите + CHART
2. Выберите датасет: stg_us_presidents
3. Выберите тип: Big Number
4. Нажмите CREATE NEW CHART
5. Настройте метрику:
   - В поле METRIC нажмите (+ Drop a column/metric here or click)
   - Выберите агрегацию COUNT(DISTINCT)
   - В поле COLUMN выберите president
   - Нажмите Save (метрика: COUNT(DISTINCT president))
6. Нажмите UPDATE CHART
7. Сохраните как "Всего президентов"

**График 2. Количество президентов по партиям (Круговая диаграмма)**
1. Перейдите в Charts и нажмите + CHART
2. Выберите датасет: stg_us_presidents
3. Выберите тип: Pie Chart
4. Нажмите CREATE NEW CHART
5. Настройте поля:
   - DIMENSIONS: party (перетащите из левой панели)
   - METRIC: COUNT(*) (перетащите из левой панели)
6. Нажмите UPDATE CHART
7. Сохраните как "Распределение по партиям"

**График 3. Президенты по десятилетиям (Гистограмма)**
1. Перейдите в Charts и нажмите + CHART
2. Выберите датасет: stg_us_presidents
3. Выберите тип: Bar Chart
4. Нажмите CREATE NEW CHART
5. Настройте поля:
   - X-AXIS: presidency_decade
   - METRIC: COUNT(*)
   - SORT BY: presidency_decade
   - SORT ASCENDING: ✓ (галочка)
6. Нажмите UPDATE CHART
7. Сохраните как "Президенты по десятилетиям"

**График 4. Топ-10 президентов по сроку правления (Горизонтальная гистограмма)**
1. Перейдите в Charts и нажмите + CHART
2. Выберите датасет: stg_us_presidents
3. Выберите тип: Bar Chart
4. Нажмите CREATE NEW CHART
5. Настройте поля:
   - METRICS: MAX(years_in_office) (создайте метрику MAX для поля years_in_office)
   - DIMENSIONS: president
   - ROW LIMIT: 10
   - SORT BY: MAX(years_in_office) (по убыванию)
6. Перейдите во вкладку CUSTOMIZE:
   - Orientation: Horizontal
7. Нажмите UPDATE CHART
8. Сохраните как "Топ-10 по сроку правления"

**График 5. Сводная таблица**
1. Перейдите в Charts и нажмите + CHART
2. Выберите датасет: stg_us_presidents
3. Выберите тип: Table
4. Нажмите CREATE NEW CHART
5. Настройте поля:
   - QUERY MODE: Raw Records
   - COLUMNS: president, party, start_year, years_in_office
6. Нажмите UPDATE CHART
7. Сохраните как "Сводная таблица"

### Шаг 4. Сборка дашборда

1. В верхнем меню перейдите в Dashboards
2. Нажмите + DASHBOARD
3. Дайте название дашборду: "Анализ президентов США"
4. Нажмите Save
5. Перетащите созданные графики с правой панели на рабочую область:
   - "Всего президентов" (Big Number) - в верхний левый угол
   - "Распределение по партиям" (Pie Chart) - рядом с индикатором
   - "Президенты по десятилетиям" (Bar Chart) - внизу слева
   - "Топ-10 по сроку правления" (Horizontal Bar Chart) - справа
   - "Сводная таблица" (Table) - внизу справа
6. Расположите и измените их размеры по своему вкусу
7. Нажмите Save для сохранения дашборда

![Сборка дашборда](./img/dashboard_01.jpg)

Отлично, вы на правильном пути! Судя по вашему скриншоту, вы почти создали фильтр. Superset подсвечивает красным единственное, чего не хватает — **имени фильтра**.

Вот скорректированная и полная инструкция на основе вашего изображения:

### Шаг 5. Добавление интерактивного фильтра по партии на дашборд

В окне **"Add and edit filters"**, выполните следующие шаги:

1.  **Заполните обязательное поле `FILTER NAME`**. Это название, которое пользователи увидят на дашборде.
    *   Введите понятное имя, например: **"Партия"** или **"Фильтр по партиям"**.

2.  **Проверьте остальные настройки:**
    *   **`FILTER TYPE`**: **Value** — это правильный тип для создания фильтра с выпадающим списком уникальных значений из колонки.
    *   **`DATASET`**: **stg_us_presidents** — вы правильно выбрали свой набор данных.
    *   **`COLUMN`**: **party** — вы правильно указали колонку, по которой будет происходить фильтрация.

3.  **(Опционально) Настройте поведение фильтра:**
    *   **`Sort filter values`**. Поставьте галочку, чтобы список партий в фильтре был отсортирован по алфавиту. Это удобно для пользователя.
    *   **`Can select multiple values`**. Эта галочка (уже стоит по умолчанию) позволяет пользователям выбирать несколько партий одновременно для сравнения.

4.  **Сохраните фильтр.**
    *   После того как вы введете имя, кнопка сохранения (обычно "Save" или "Add" внизу окна) станет активной. Нажмите ее.

Фильтр появится на вашем дашборде, и вы сможете использовать его для интерактивного анализа данных на всех графиках, построенных на основе датасета `stg_us_presidents`. Не забудьте нажать **"SAVE"** на самом дашборде, чтобы сохранить все изменения.

![Сборка дашборда](./img/dashboard_02_filtr.jpg)

### 6. Очистка окружения
```bash
chmod +x cleanup.sh
sudo ./cleanup.sh
```

## Структура проекта

```
lpw_04/
├── dags/
│   ├── us_presidents_dag.py     # DAG для варианта 30
│   └── datamart_variant_30.sql  # SQL для витрины данных
├── docker-compose.yml           # Конфигурация инфраструктуры
├── Dockerfile                   # Кастомный образ Airflow
├── requirements.txt             # Python зависимости
├── kaggle.json                  # Kaggle API ключ
├── setup_kaggle.sh              # Настройка Kaggle API
├── cleanup.sh                   # Очистка окружения
└── README.md                    # Документация
```

## Структура данных

### Таблица stg_us_presidents
Основная таблица с обогащенными данными о президентах США:

| Поле | Тип | Описание |
|---|---|---|
| president | TEXT | Имя президента |
| party | TEXT | Партийная принадлежность |
| start_year | INTEGER | Год начала президентства |
| end_year | INTEGER | Год окончания президентства |
| years_in_office | INTEGER | Количество лет в офисе |
| presidency_decade | TEXT | Десятилетие президентства (например, "1780s") |

### Представление us_presidents_datamart
VIEW на основе таблицы stg_us_presidents для аналитики.

## Устранение неполадок

### Проблемы с правами доступа
```bash
sudo usermod -aG docker $USER
newgrp docker
```

### Проблемы с Kaggle API
```bash
./setup_kaggle.sh
```

### Проблемы с компиляцией
```bash
sudo docker compose build
sudo docker compose up -d
```

## Git-репозиторий

Проект содержит:
- **DAG**: `dags/us_presidents_dag.py`
- **SQL**: `dags/datamart_variant_30.sql`
- **Docker**: `docker-compose.yml`, `Dockerfile`
- **Скрипты**: `setup_kaggle.sh`, `cleanup.sh`
- **Документация**: `README.md`

**Исключения**: `kaggle.json` (не включать в Git!)

## Форма отчета

Итогом работы является публичный Git-репозиторий с файлом `Readme.md`, оформленным по профессиональному стандарту. Структура `Readme.md` должна включать следующие разделы:

### Цель работы
Краткое описание цели в контексте вашего бизнес-кейса.

### Исследуемые метрики бизнес-процесса
Перечень и описание ключевых метрик, которые вы анализировали.

### Архитектура решения
Краткое описание архитектуры data-конвейера (Airflow -> Postgres -> Superset).

### Технологический стек
Перечень использованных технологий.

### ER-диаграмма базы данных
Схема таблиц в PostgreSQL, задействованных в проекте (можно в виде скриншота).

### Ссылки на исходный код
*   Python-файл вашего DAG (`.py`).
*   SQL-файл для создания витрины данных (`.sql`).
*   JSON-файл с экспортированным дашбордом из Apache Superset или публичная ссылка на дашборд.

### Описание проделанной работы (с наглядными результатами)
*   Скриншот графа успешно выполненного DAG из интерфейса Airflow.
*   Текст вашего SQL-запроса для создания витрины данных (в блоке кода).
*   Скриншот итогового дашборда, на котором видны все 5 обязательных типов чартов и фильтры.

### Выводы по анализу
Краткие выводы и бизнес-инсайты, которые можно сделать на основе созданного дашборда.


# Задание и оценка работы

Варианты заданий для выполнения лабораторной работы доступны по ссылке:
**[Скачать варианты заданий](http://95.131.149.21/moodle/mod/assign/view.php?id=2108)**







