import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta

import pandas as pd


raw_data_path = Variable.get("raw_data")

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
def insert_data():
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


#Объявление DAG
args={'owner': 'airflow'}

default_args = {
    'owner': 'airflow',    
    'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

task_33 = DAG(
    dag_id = 'task_33',
    default_args=args,
    # schedule_interval='0 0 * * *',
    schedule_interval='@daily',	
    dagrun_timeout=timedelta(minutes=60),
    description='task 3.2 decision',
    start_date = airflow.utils.dates.days_ago(1)
)

start = DummyOperator(
    task_id='start',
    dag=task_33
)

folder_creation = BashOperator(
    task_id='folder_creation',
    bash_command=f'mkdir -p {raw_data_path}/supermarket_1',
    dag=task_33
)

# Учитывая невозможность загрузки исходного файла с Яндекс-диск, привожу 2 возможных способа реализации загрузки, считаю, что
# данный таск отработал, файл в требуемой папке сохранен

#СПОСОБ 1: Загрузка с помощью  requests

#import requests
#from urllib.parse import urlencode

""" def download_file():
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?' # или https://cloud-api.yandex.net/v1/disk/resources/download?
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
    dag=task_33
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
        dag=task_32) 
"""

# Для заглушки использую пустой оператор 
download_file = DummyOperator(
    task_id='download_file',
    dag=task_33
)

create_table = PostgresOperator(
    sql = create_table_sql_query,
    task_id = "create_table",
    postgres_conn_id = "my_postgres",
    dag = task_33
    )

insert_task = PythonOperator(
    task_id='insert_task',
    python_callable=insert_data,
    provide_context=True,
    dag = task_33
    )

# Реализация использования механизма Xcom двумя способами:

# PythonOperator
fetch_count_task_p = PythonOperator(
    task_id='fetch_count_task_p',
    python_callable=count_lines,
    dag = task_33
    )

# BashOperator
fetch_count_task_b = BashOperator(
    task_id="fetch_count_task_b",
    bash_command='echo {{ti.xcom_pull("insert_task")}} lines uploaded',
    dag=task_33
)

create_data_mart = PostgresOperator(
    sql = data_mart,
    task_id = "create_data_mart",
    postgres_conn_id = "my_postgres",
    dag = task_33
    )

end = DummyOperator(
    task_id='end',
    dag=task_33
)

start >> folder_creation >> download_file >> create_table >> insert_task >> [fetch_count_task_p, fetch_count_task_b] \
        >> create_data_mart >> end

