*Запуск контейнера и проверка работоспособности*


PS D:\DE_Course\Hadoop> docker ps
CONTAINER ID   IMAGE                                                    COMMAND                  CREATED        STATUS                        PORTS                                          NAMES
6b3e62e800b8   gethue/hue:4.6.0                                         "./startup.sh"           20 hours ago   Up About a minute             0.0.0.0:8888->8888/tcp                         hadoop-hue-1
7f54c1683e6f   bde2020/hadoop-namenode:2.0.0-hadoop2.7.4-java8          "/entrypoint.sh /run…"   20 hours ago   Up About a minute (healthy)   0.0.0.0:50070->50070/tcp                       hadoop-namenode-1
7564913ac0d7   bde2020/hive:2.3.2-postgresql-metastore                  "entrypoint.sh /bin/…"   20 hours ago   Up About a minute             0.0.0.0:10000->10000/tcp, 10002/tcp            hadoop-hive-server-1
9e5f48f99130   bde2020/hive-metastore-postgresql:2.3.0                  "/docker-entrypoint.…"   20 hours ago   Up About a minute             0.0.0.0:5432->5432/tcp                         hadoop-hive-metastore-postgresql-1
c09141f383f8   bde2020/hadoop-resourcemanager:2.0.0-hadoop2.7.4-java8   "/entrypoint.sh /run…"   20 hours ago   Up About a minute (healthy)   8088/tcp                                       hadoop-resourcemanager-1
6e4789e37a87   postgres:12.1-alpine                                     "docker-entrypoint.s…"   20 hours ago   Up About a minute             0.0.0.0:6685->5432/tcp                         hadoop-huedb-1
416005d14e14   bde2020/hive:2.3.2-postgresql-metastore                  "entrypoint.sh /opt/…"   20 hours ago   Up About a minute             10000/tcp, 0.0.0.0:9083->9083/tcp, 10002/tcp   hadoop-hive-metastore-1
748b87287a6c   bde2020/hadoop-datanode:2.0.0-hadoop2.7.4-java8          "/entrypoint.sh /run…"   20 hours ago   Up About a minute (healthy)   0.0.0.0:50075->50075/tcp                       hadoop-datanode-1

PS D:\DE_Course\Hadoop> docker cp init/ 748b87287a6c:/home/books
Successfully copied 3.05MB to 748b87287a6c:/home/books

PS D:\DE_Course\Hadoop> docker exec -it 748b87287a6c bin/bash
root@748b87287a6c:/# cd home/books
root@748b87287a6c:/home/books# ls
voyna-i-mir-tom-1.txt  voyna-i-mir-tom-2.txt  voyna-i-mir-tom-3.txt  voyna-i-mir-tom-4.txt

root@748b87287a6c:/home# cat books/* > books/war_n_peace.txt
root@748b87287a6c:/home# cd /home/books
root@748b87287a6c:/home/books# ls
voyna-i-mir-tom-1.txt  voyna-i-mir-tom-2.txt  voyna-i-mir-tom-3.txt  voyna-i-mir-tom-4.txt  war_n_peace.txt  wnp_utf8.txt

root@748b87287a6c:/home# hdfs dfs -put books/war_n_peace.txt /user/dmitry/war_n_peace.txt
root@748b87287a6c:/home# hdfs dfs -ls /user/dmitry
Found 1 items
-rw-r--r--   3 root dmitry    3048008 2023-08-09 06:48 /user/dmitry/war_n_peace.txt

root@748b87287a6c:/home# hdfs dfs -chmod 755 /user/dmitry/war_n_peace.txt
root@748b87287a6c:/home# hdfs dfs -ls /user/dmitry
Found 1 items
-rwxr-xr-x   3 root dmitry    3048008 2023-08-09 06:48 /user/dmitry/war_n_peace.txt
root@748b87287a6c:/home# hdfs du -ls -h /user/dmitry

root@748b87287a6c:/home# hdfs dfs -du -h /user/dmitry
2.9 M  /user/dmitry/war_n_peace.txt

root@748b87287a6c:/home# hdfs dfs -ls -h /user/dmitry/war_n_peace.txt
-rwxr-xr-x   3 root dmitry      2.9 M 2023-08-09 06:48 /user/dmitry/war_n_peace.txt

root@748b87287a6c:/home# hdfs dfs -setrep 2 /user/dmitry/war_n_peace.txt
Replication 2 set: /user/dmitry/war_n_peace.txt

root@748b87287a6c:/home# hdfs dfs -ls -h /user/dmitry/war_n_peace.txt
-rwxr-xr-x   2 root dmitry      2.9 M 2023-08-09 06:48 /user/dmitry/war_n_peace.txt

root@748b87287a6c:/home# hdfs dfs -cat /user/dmitry/war_n_peace.txt | wc -l
10272
