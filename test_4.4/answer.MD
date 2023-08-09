*Запуск контейнера и проверка работоспособности*

PS D:\DE_Course\Hadoop> docker cp init/ 748b87287a6c:/home/books
Successfully copied 3.05MB to 748b87287a6c:/home/books

PS D:\DE_Course\Hadoop> docker exec -it 748b87287a6c bin/bash

cd home/books
ls
voyna-i-mir-tom-1.txt  voyna-i-mir-tom-2.txt  voyna-i-mir-tom-3.txt  voyna-i-mir-tom-4.txt

cat books/* > books/war_n_peace.txt
ls

voyna-i-mir-tom-1.txt  voyna-i-mir-tom-2.txt  voyna-i-mir-tom-3.txt  voyna-i-mir-tom-4.txt  war_n_peace.txt  wnp_utf8.txt

hdfs dfs -put books/war_n_peace.txt /user/dmitry/war_n_peace.txt
hdfs dfs -ls /user/dmitry
Found 1 items
-rw-r--r--   3 root dmitry    3048008 2023-08-09 06:48 /user/dmitry/war_n_peace.txt

hdfs dfs -chmod 755 /user/dmitry/war_n_peace.txt
hdfs dfs -ls /user/dmitry
Found 1 items
-rwxr-xr-x   3 root dmitry    3048008 2023-08-09 06:48 /user/dmitry/war_n_peace.txt
root@748b87287a6c:/home# hdfs du -ls -h /user/dmitry

hdfs dfs -du -h /user/dmitry
2.9 M  /user/dmitry/war_n_peace.txt

hdfs dfs -ls -h /user/dmitry/war_n_peace.txt
-rwxr-xr-x   3 root dmitry      2.9 M 2023-08-09 06:48 /user/dmitry/war_n_peace.txt

hdfs dfs -setrep 2 /user/dmitry/war_n_peace.txt
Replication 2 set: /user/dmitry/war_n_peace.txt

hdfs dfs -ls -h /user/dmitry/war_n_peace.txt
-rwxr-xr-x   2 root dmitry      2.9 M 2023-08-09 06:48 /user/dmitry/war_n_peace.txt

hdfs dfs -cat /user/dmitry/war_n_peace.txt | wc -l
10272
