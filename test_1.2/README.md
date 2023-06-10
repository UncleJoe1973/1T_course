1. Создаем образ:   

    ```docker
    docker build -t q1:01 .
    ```
    Создаем контейнер:

    ```docker
    docker run -d -p 5433:5432 --name test q1:01
    ```

2. Запускаем psql в контейнере:

    ```docker
    docker exec -it test psql -U postgres 
    ```

3. Запускаем контейнер с монтированием volume для сохранения изменений в контейнере (например, папка на ЛОС `/data` -> папка в контейнере `/var/lib/postgresql/data`):

    ```docker
    docker run --rm -d -p 5432:5432 --name test_volume -v /data:/var/lib/postgresql/data q1:01
    ```
