# Запуск контейнера и проверка работоспособности

**Команда - docker compose up (в папке размещения файла docker-compose.yml)**

# Копирование загруженных файлов в контейнер

**Команда - docker cp init/ 748b87287a6c:/home/books, где 748b87287a6c - ID контейнера Docker в текущем сеансе**

_Вывод в терминале - Successfully copied 3.05MB to 748b87287a6c:/home/books_

# Вход в контейнер, проверка наличия скопированных файлов

**Команды:**

**docker exec -it 748b87287a6c bin/bash**

**cd home/books**

**ls**

_Вывод в терминале - voyna-i-mir-tom-1.txt  voyna-i-mir-tom-2.txt  voyna-i-mir-tom-3.txt  voyna-i-mir-tom-4.txt_

# Объединение файлов в один сводный файл

**Команды:**

**cat books/*** **> books/war_n_peace.txt**

**ls**

_Вывод в терминале - voyna-i-mir-tom-1.txt  voyna-i-mir-tom-2.txt  voyna-i-mir-tom-3.txt  voyna-i-mir-tom-4.txt  war_n_peace.txt_

# Размещение сводного файла в HDFS

**Команда - hdfs dfs -put books/war_n_peace.txt /user/dmitry/war_n_peace.txt**

# Вывод содержимого личной папки

**Команда - hdfs dfs -ls /user/dmitry**

_Вывод в терминале -_

_Found 1 items_

_-rw-r--r--   3 root dmitry    3048008 2023-08-09 06:48 /user/dmitry/war_n_peace.txt_

# Изменение прав доступа

**Команда - hdfs dfs -chmod 755 /user/dmitry/war_n_peace.txt**

# Вывод содержимого личной папки после изменения прав доступа

**Команда - hdfs dfs -ls /user/dmitry**

_Вывод в терминале -_

_Found 1 items_

_-rwxr-xr-x   3 root dmitry    3048008 2023-08-09 06:48 /user/dmitry/war_n_peace.txt_

# Вывод информации о файле в терминал

**Команда - hdfs dfs -du -h /user/dmitry**

_Вывод в терминале - 2.9 M  /user/dmitry/war_n_peace.txt_

# Вывод информации о файле в терминал с фактором репликации

**Команда - hdfs dfs -ls -h /user/dmitry/war_n_peace.txt**

_Вывод в терминале - -rwxr-xr-x   3 root dmitry      2.9 M 2023-08-09 06:48 /user/dmitry/war_n_peace.txt_

# Изменение фактора репликации

**Команда - hdfs dfs -setrep 2 /user/dmitry/war_n_peace.txt**

_Вывод в терминале - Replication 2 set: /user/dmitry/war_n_peace.txt_

# Повторный вывод информации о файле в терминал с фактором репликации

**Команда - hdfs dfs -ls -h /user/dmitry/war_n_peace.txt**

_Вывод в терминале - -rwxr-xr-x   2 root dmitry      2.9 M 2023-08-09 06:48 /user/dmitry/war_n_peace.txt_

# Определение количества строк в сводном файле

**Команда - hdfs dfs -cat /user/dmitry/war_n_peace.txt | wc -l**

_Вывод в терминале - 10272_

# Скрин-шот личной папки в HUE после выполнения всех команд

![изображение](https://github.com/UncleJoe1973/1T_course/assets/29273924/d1f4dd19-c487-4b55-a1b1-dad3bfcb6b66)
