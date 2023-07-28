# Решение задания ПРО (не НОВОГО) по разделу ```Погружение в Airflow```

Запуск Aiflow из контейнера Docker [docker-compose.yaml](docker-compose.yaml), [dockerfile](dockerfile), [requirements.txt](requirements.txt)

Текст DAG [task_3.4_pro.py](./dags/task_3.4_pro.py), поместить в папку /dags директории проекта Airflow

Для корректной работы DAGа необходимо:

* создать подключение к базе данных PostgreSQL, [данные подключения](./pictures/connection_db.PNG)
* создать переменные Airflow, [данные переменных](./pictures/variables.PNG)
* создать подключение к файловой системе, [данные подключения](./pictures/connection_fs.PNG)

Исходный код использованных [SQL-скриптов](./sql_scrypts/)

Скрин-шоты Aiflow Graphs [ветвление с помощью BranchPythonOperator](./pictures/DAG_BranchPythonOperator.PNG), [ветвление с помощью PostgresOperator](./pictures/DAG_PostgresOperator.PNG)

Скрин-шоты витрин данных [витрина 1](./pictures/data_mart_1.PNG), [витрина 2](./pictures/data_mart_2.PNG)

Примечание: поскольку отсутствует техническая возможность скачивания файла-источника данных, необходимо в папку **raw_data/supermarket_**  директории проекта Airflow поместить [файл исходных данных](./raw_data/supermarket_/Sample-Superstore.csv)
