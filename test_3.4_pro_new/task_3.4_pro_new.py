from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.sensors.python import PythonSensor
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from datetime import timedelta

import psycopg2
import requests
import datetime

# объявление переменных
variables = Variable.set(key='task_3.4_vars', 
                         value = 
                                    {'url':'https://api.exchangerate.host/latest?',
                                     'resp_key':'rates',
                                     'RUR_key':'RUB',
                                     'BTC_key':'BTC',
                                     'conn_name':'task_3.4',                           
                                     }, 
                         serialize_json=True)

dag_vars = Variable.get('task_3.4_vars', deserialize_json=True)

# get airflow db connection string
def get_conn(conn_name):
    conn = BaseHook.get_connection(conn_name)

    db_name = conn.schema
    db_user = conn.login
    db_pass = conn.password
    db_host = conn.host
    db_port = conn.port

    return 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)


# rates table
sql_string = '''
            create table if not exists rates
            (
	            ticker varchar(32),
                rate_date varchar(32),
	            rate varchar(32)
            );'''

# rates table rows count
rows_count = '''
            create or replace function count_row() returns integer as $body$
                declare cnt integer;
                begin
                if exists (select from pg_tables where tablename = 'rates' and schemaname = 'public') then 
                        select into cnt count(*)  from rates;
                end if;
                
                return cnt;
                end;
            $body$ language plpgsql;

            select count_row();
            '''

# calculated table
calculation_strirg = '''
            create table if not exists calc (
                            ticker varchar(32),
                            insert_date varchar(32),
                            max_rate numeric(20, 4),
                            min_rate numeric(20, 4),
                            avg_rate numeric(20, 4)
                        );

            insert into calc (ticker, insert_date, max_rate, min_rate, avg_rate)
            select 
                ticker,
                now(),
                max(rate::numeric),
                min(rate::numeric),
                avg(rate::numeric)
            from rates
            group by ticker, now();
            '''

# вспомогательные функции
def query_and_push(sql, **context):

    pg_hook = PostgresHook(postgres_conn_id=dag_vars.get('conn_name'))
    records = pg_hook.get_records(sql=sql)

    Variable.set(key='old_rows', value=records[0][0])


def check_rows(**context):
    old_count = Variable.get('old_rows', default_var=None)

    pg_hook = PostgresHook(postgres_conn_id=dag_vars.get('conn_name'))
    records = pg_hook.get_records(sql=rows_count)
    new_count=records[0][0]
    print(old_count, new_count) 

    if int(old_count) < new_count:
        Variable.set(key='old_rows', value=new_count)
        return True
    else: return False 


def create_table():    
    with psycopg2.connect(get_conn(dag_vars.get('conn_name'))) as conn:
        cursor = conn.cursor()
        cursor.execute(sql_string)
        conn.commit()
    conn.close()   


def rate_get():
    url = dag_vars['url']
    response = requests.get(url)
    data = response.json()[dag_vars['resp_key']]
    rate = data[dag_vars['RUR_key']] / data[dag_vars['BTC_key']]

    dt = datetime.datetime.now()
    
    return ('BTC', dt.strftime('%H:%M - %m.%d.%Y'), rate)


def data_insert(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='get_rate')

    with psycopg2.connect(get_conn(dag_vars.get('conn_name'))) as conn:
        cursor = conn.cursor()
        cursor.execute(
                f"INSERT INTO rates ({', '.join(['ticker', 'rate_date', 'rate'])}) VALUES ({', '.join(['%s'] * 3)})",
                data
            )
        conn.commit()
    conn.close()   

#Объявление DAG
default_args = {
    'owner': 'airflow',    
    'start_date': days_ago(2),
    # 'end_date': datetime(),
    'depends_on_past': False,
    'catchup':False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id = 'task_34_pro_new',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    #schedule_interval='@daily',	
    dagrun_timeout=timedelta(minutes=60),
    description='task 3.2 decision',
    start_date = days_ago(1),
    catchup=False
) as task_34_pro_new: 

    print_line = BashOperator(
        task_id="print_line",
        bash_command='echo Good morning my diggers!',
        dag=task_34_pro_new
    )

    count_rows = PythonOperator(
        task_id='count_rows',
        python_callable=query_and_push,
        op_kwargs={
            'sql': f'{rows_count};'
        },
        dag=task_34_pro_new
    )

    create_task = PythonOperator(
        task_id='create_task',
        python_callable=create_table,
        provide_context=True,
        dag = task_34_pro_new
        )

    get_rate = PythonOperator(
        task_id='get_rate',
        python_callable=rate_get,
        provide_context=True,
        dag = task_34_pro_new
        )

    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=data_insert,
        provide_context=True,
        dag = task_34_pro_new
        )

    #сенсор, определяющий изменение количества строк в таблице
    sensor = PythonSensor( 
        task_id='sensor',
        python_callable=check_rows,
        soft_fail=True,
        poke_interval=20,
        mode="poke",
        timeout=5,    
    )

    create_calc_table = PostgresOperator(
        sql = calculation_strirg,
        task_id = "create_data_mart",
        postgres_conn_id = dag_vars.get('conn_name'),
        dag = task_34_pro_new
        )

    print_line >> count_rows >> create_task >> get_rate >> insert_data >> sensor >> create_calc_table