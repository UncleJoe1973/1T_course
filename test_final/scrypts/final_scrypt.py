#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns

db_name = 'testdb'
db_user = 'postgres'
db_pass = 'postgres'
db_host = 'postgres_exam'
db_port = '5432'

db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)

# чтение витрины по анализу поездок по группам пассажиров (данные по чистой стоимости поездки) для записи в parquet
with psycopg2.connect(db_string) as conn:
    cursor = conn.cursor()
    cursor.execute('''SELECT * FROM exam.mart_1f''')
    p = cursor.fetchall()
conn.close()

df = pd.DataFrame(p, columns=['date', 
                              'percentage_zero', 
                              'percentage_1p', 
                              'percentage_2p',
                              'percentage_3p',
                              'percentage_4p_plus',
                              'fare_min',
                              'fare_max'
                             ])
df.to_parquet(r'/var/tmp/result/mart_1f.parquet', index=None)  

# чтение витрины по анализу поездок по группам пассажиров (данные по полной стоимости поездки) для записи в parquet
with psycopg2.connect(db_string) as conn:
    cursor = conn.cursor()
    cursor.execute('''SELECT * FROM exam.mart_1t''')
    p = cursor.fetchall()
conn.close()

df = pd.DataFrame(p, columns=['date', 
                              'percentage_zero', 
                              'percentage_1p', 
                              'percentage_2p',
                              'percentage_3p',
                              'percentage_4p_plus',
                              'total_min',
                              'total_max'
                             ])
df.to_parquet(r'/var/tmp/result/mart_1t.parquet', index=None) 

# чтение витрины по анализу зависимости чаевых от количества пассажиров и длины поездки для посторения визуализации
with psycopg2.connect(db_string) as conn:
    cursor = conn.cursor()
    cursor.execute('''SELECT * FROM exam.mart_2''')
    p = cursor.fetchall()
conn.close()

# подготовка данных для визуализации
df = pd.DataFrame(p, columns=['psg_cnt', 'trip_grp', 'trip_thr', 'avg_tip'])
df_m = pd.DataFrame(df, columns=['psg_cnt', 'trip_grp', 'avg_tip'])
df_m = df_m.pivot(index='trip_grp', values='avg_tip', columns='psg_cnt')

plt.figure(figsize=(10,8))
sns.heatmap(df_m, linewidths=.5, annot=True, cmap='coolwarm', fmt = '.3g') 
plt.xlabel('Количество пассажиров, чел', size=12)
plt.ylabel('Дальность поездки, ед', size=12)
plt.title('Средние чаевые в зависимости от\nчисла пассажиров и дальности поездки, ден. ед\n', size=14)

plt.savefig(r'/var/tmp/result/saved_figure.png')


