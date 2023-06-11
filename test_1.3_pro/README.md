# Выполнение скрипта Python на основе данных.

1. В терминале сборка и запуск образа запускается командой в папке <b>1T_course/test_1.3_pro<b>: 
    ```docker
    docker-compose up -d
    ```
    
2. Результат работы можно посмотреть в логе Docker Desktop или в терминале:
    ```docker
    docker ps -a #определить ID контейнера, выполняющего скрипт - [CONTAINER ID]
    docker logs [CONTAINER ID]
    ```
