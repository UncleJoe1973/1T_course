# Решение задания PRO по разделу ```Введение в колоночные СУБД. Clickhouse```

Контейнер с решением поднимается запуском файла [docker-compose.yml](docker-compose.yml), требуемая витрина данных сохраняется в виде таблицы БД **_Clickhouse_** **ch_task_25_pro**.

  * База данных **_Postgres_** инициализируется скриптом [01_task_2.5_pro.sql](init/01_task_2.5_pro.sql), исходные данные находятся в папке [**init/data**](./init/data).
  * Витрина в БД **_Postgres_** формируется скриптом [11_task_2.5_pro.sql](init/11_task_2.5_pro.sql).
  * Миграция витрины в БД **_Clickhouse_** выполняется скриптом [clickhouse/01_init_db_2.8_pro.sql](clickhouse/01_init_db_2.8_pro.sql).
