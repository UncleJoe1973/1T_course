import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowSensorTimeout
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta

import pandas as pd
from random import choice
from airflow.operators.python import get_current_context


raw_data_path = Variable.get("raw_data")
sql_scrypts = Variable.get("scrypts")


create_table_sql_query = """ 
        create table if not exists raw_store (
            row_ID integer,
            order_ID varchar(32),
            order_date date,
            ship_date date,
            ship_mode varchar(32),
            customer_ID varchar(32),
            customer_name varchar(64),
            segment varchar(32),
            country varchar(32),
            city varchar(32),
            state varchar(32),
            postal_code varchar(32),
            region varchar(32),
            product_ID varchar(32),
            category varchar(32),
            subcategory varchar(32),
            product_name varchar(200),
            sales real,
            quantity integer,
            discount real,
            profit real 
        );
        """

data_mart = """ 
create or replace view mart_store ("Категория", "Сумма") as
            select 
                category
                ,sum(sales)
            from public.raw_store
            where segment like 'Corporate'
            group by category; 
            """


#Вспомогательные функции
def insert_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='my_postgres')
    db_conn = pg_hook.get_conn()
    src_cursor = db_conn.cursor()

    df = pd.read_csv(f'{raw_data_path}/supermarket_/Sample-Superstore.csv')
    df.to_csv('temp.csv', sep='\t', header=False, index=False)
    
    cnt = 0
    with open('temp.csv', 'r') as f:
        src_cursor.execute('drop view if exists mart_store; drop table if exists raw_store')
        db_conn.commit() 
        src_cursor.execute(create_table_sql_query)
        db_conn.commit() 
        src_cursor.copy_from(f, 'raw_store', sep='\t')
        db_conn.commit() 
        src_cursor.execute('select count(*) from raw_store')
        cnt = src_cursor.fetchall()
    db_conn.close()
    return cnt[0][0]

def count_lines(**kwargs):
    ti = kwargs['ti']
    print('Total lines uploaded ', ti.xcom_pull(task_ids='insert_task'))


def _failure_callback(**kwargs):
    if isinstance(kwargs['exception'], AirflowSensorTimeout):
        print(kwargs)
        print("Sensor timed out")


def choose_data(**kwargs):
    with PostgresHook(postgres_conn_id='my_postgres').get_conn() as db_conn:
        src_cursor = db_conn.cursor()
        src_cursor.execute('select distinct category_name from core.categories;')
        data = cnt = src_cursor.fetchall()
    db_conn.close()

    random_choice = choice([x[0] for x in data])
    ti = kwargs['ti']
    ti.xcom_push(value=[x[0] for x in data], key='list')
    
    return random_choice


def choose_data_2(**kwargs):
    with PostgresHook(postgres_conn_id='my_postgres').get_conn() as db_conn:
        src_cursor = db_conn.cursor()
        src_cursor.execute('select distinct category_name from core.categories;')
        data = cnt = src_cursor.fetchall()
    db_conn.close()

    random_choice = choice([x[0] for x in data])

    ti = kwargs['ti']
    ti.xcom_push(value=[x[0] for x in data], key='list')
    
    return random_choice[0]


#Объявление DAG
args={'owner': 'airflow'}

default_args = {
    'owner': 'airflow',    
    'start_date': days_ago(2),
    # 'end_date': datetime(),
    'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

task_34_pro = DAG(
    dag_id = 'task_34_pro',
    default_args=args,
    # schedule_interval='0 0 * * *',
    schedule_interval='@daily',	
    dagrun_timeout=timedelta(minutes=60),
    description='task 3.2 decision',
    start_date = airflow.utils.dates.days_ago(1),
    template_searchpath = ['/opt/airflow/sql_scrypts/'],
)

start = DummyOperator(
    task_id='start',
    dag=task_34_pro
)

folder_creation = BashOperator(
    task_id='folder_creation',
    bash_command=f'mkdir -p {raw_data_path}/supermarket_',
    dag=task_34_pro
)

# Учитывая невозможность загрузки исходного файла с Яндекс-диск, привожу 2 возможных способа реализации загрузки, считаю, что
# данный таск отработал, файл в требуемой папке сохранен

#СПОСОБ 1: Загрузка с помощью  requests

#import requests
#from urllib.parse import urlencode

""" def download_file():
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
                https://cloud-api.yandex.net/v1/disk/resources/download?
    public_key = 'https://disk.yandex.com.am/d/wMKRLK7gNL09Dg'

    final_url = base_url + urlencode(dict(public_key=public_key))
    response = requests.get(final_url)
    download_url = response.json()['href']

    download_response = requests.get(download_url)
    with open('/opt/airflow/raw_data/supermarket_/Sample-Superstore.csv', 'w') as f:   # Здесь укажите нужный путь к файлу
        f.write(download_response.content)

download_file = PythonOperator(
    task_id='download_file',
    python_callable=download_file,
    dag=task_34_pro
    )

"""

# СПОСОБ 2: Загрузка файла с яндекс диска при помощи библиотеки wldhx.yadisk_direct. Библиотека монтируется в образ Airflow при его сборке из 
# файла Dockerfile (сам файл и requirements.txt прилагаются)

#import wldhx.yadisk_direct
""" download_file = BashOperator(
        task_id='download_file',
        bash_command='''
                    wget $(yadisk-direct https://disk.yandex.com.am/d/wMKRLK7gNL09Dg) -o /opt/airflow/raw_data/supermarket_/Sample-Superstore.csv
                ''',
        dag=task_34_pro) 
"""

# Для заглушки использую пустой оператор 
download_file = DummyOperator(
    task_id='download_file',
    dag=task_34_pro
)

#file sensor
wait_for_file = FileSensor(
    task_id='wait_for_file',
    poke_interval=60,
    timeout=60 * 30,
    mode="reschedule",
    on_failure_callback=_failure_callback,
    filepath='supermarket_/Sample-Superstore.csv',
    fs_conn_id = 'test_34_pro'
)

create_table = PostgresOperator(
    sql = create_table_sql_query,
    task_id = "create_table",
    postgres_conn_id = "my_postgres",
    dag = task_34_pro
    )

insert_task = PythonOperator(
    task_id='insert_task',
    python_callable=insert_data,
    provide_context=True,
    dag = task_34_pro
    )

#########################################################
# Реализация использования механизма Xcom двумя способами:

# PythonOperator
fetch_count_task_p = PythonOperator(
    task_id='fetch_count_task_p',
    python_callable=count_lines,
    dag = task_34_pro
    )

# BashOperator
fetch_count_task_b = BashOperator(
    task_id="fetch_count_task_b",
    bash_command='echo {{ti.xcom_pull("insert_task")}} lines uploaded',
    dag=task_34_pro
)

#########################################################
# Задание ПРО вопрос 2, п.1
# создание слоя core
create_core = PostgresOperator(
    sql = '01_core_init.sql',
    task_id = "create_core",
    postgres_conn_id = "my_postgres",
    dag = task_34_pro
    )

# наполнение данными слоя core
fill_core = PostgresOperator(
    sql = '02_data_init.sql',
    task_id = "fill_core",
    postgres_conn_id = "my_postgres",
    dag = task_34_pro
    )

# Задание ПРО вопрос 2, п.2, а/b - поскольку в процессе загрузки данные сразу конвертировались в тип date, показываю,
# что умею работать с функциями обработки строк в PostgreSQL. Результат - таблица public.date_formated в БД
 
date_formating = PostgresOperator(
    sql = '03_date_formated.sql',
    task_id = "date_formating",
    postgres_conn_id = "my_postgres",
    dag = task_34_pro
    )

# Задание ПРО вопрос 2, п.2, c/d
core_store = PostgresOperator(
    sql = '04_core_store.sql',
    task_id = "core_store",
    postgres_conn_id = "my_postgres",
    dag = task_34_pro
    )

# витрина из задания 3.3 (не НОВОГО)
create_data_mart = PostgresOperator(
    sql = data_mart,
    task_id = "create_data_mart",
    postgres_conn_id = "my_postgres",
    dag = task_34_pro
    )

# Задание вопрос 3, Витрина 1
data_mart_1 = PostgresOperator(
    sql = '05_data_mart_01.sql',
    task_id = "data_mart_1",
    postgres_conn_id = "my_postgres",
    dag = task_34_pro
    )

# Задание вопрос 3, Витрина 2
data_choose = PythonOperator(
    task_id='data_choose',
    python_callable=choose_data,
    provide_context=True,
    dag = task_34_pro
    )

# Закомментировать для проверки работы BranchPythonOperator.
# Решение при помощи PostgresOperator. Результат сохраняется в таблице БД question_2
data_mart_2 = PostgresOperator(
    task_id='data_mart_2',
    postgres_conn_id="my_postgres",
    sql='06_data_mart_02.sql',
    params={"year": "2015", 
            },
    dag = task_34_pro
)

# Раскомментировать для проверки работы BranchPythonOperator. Результат сохраняется в таблице БД question_3
""" 
# Решение при помощи BranchPythonOperator
branch_op = BranchPythonOperator(
        task_id="branch_op",
        python_callable=choose_data_2,
        retries=1,
        retry_delay=timedelta(minutes=1)
    )

technology = PostgresOperator(
    sql = '07_data_mart_03.sql',
    task_id = "T",
    postgres_conn_id = "my_postgres",
    params={"year": "2015", 
            'category':'Technology'},
    dag = task_34_pro
    )

office_suppliers = PostgresOperator(
    sql = '07_data_mart_03.sql',
    task_id = "O",
    postgres_conn_id = "my_postgres",
    params={"year": "2015", 
            'category':'Office suppliers'},
    dag = task_34_pro
    )

furniture = PostgresOperator(
    sql = '07_data_mart_03.sql',
    task_id = "F",
    postgres_conn_id = "my_postgres",
    params={"year": "2015", 
            'category':'Furniture'},
    dag = task_34_pro
    )
 """

end = DummyOperator(
    task_id='end',
    trigger_rule='none_failed_or_skipped',
    dag=task_34_pro
)

# Закомментировать для проверки работы BranchPythonOperator.
start >> folder_creation >> download_file >> wait_for_file >> create_table >> insert_task >> \
    [fetch_count_task_p, fetch_count_task_b] >> create_core >> fill_core >> date_formating >> \
    core_store >> create_data_mart >> data_mart_1 >> data_choose >> data_mart_2 >> end

# Раскомментировать для проверки работы BranchPythonOperator
""" start >> folder_creation >> download_file >> wait_for_file >> create_table >> insert_task >> \
    [fetch_count_task_p, fetch_count_task_b] >> create_core >> fill_core >> date_formating >> \
    core_store >> create_data_mart >> data_mart_1 >> branch_op >> [technology, furniture, office_suppliers] >> end """