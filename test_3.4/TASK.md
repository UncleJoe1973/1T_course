**Задание**

У вас получился прекрасно работающий dag, однако его еще можно улучшить! Для этого необходимо сделать следующие шаги:

* Вынести параметры для вызова API в airflow variables.

Зачем? Подобные конфигурационные параметры должны иметь возможность изменяться без погружения в сам код.

Создайте новую переменную в интерфейсе Airflow, перенесите в нее значение url [Exchangerate.host](https://exchangerate.host/#/) и названия валют, а затем импортируйте в код вашего dag’a.

* Вынести подключение к БД в airflow connections.

Зачем? Данные для подключения небезопасно хранить в коде, и они также должны быть доступны всем разработчикам, работающим с airflow.

![изображение](https://github.com/UncleJoe1973/1T_course/assets/29273924/22dc4e01-2816-4174-aa73-fda66c91b4b1)


Также через интерфейс airflow создайте новое подключение к PostgresDB и перенесите туда все данные:


Обратите внимание на host — данный host соответствует внутреннему адресу контейнера с базой внутри docker-compose.

Протестируйте ваше соединение в рамках интерфейса airflow перед использованием. Как только соединение будет успешно установлено, импортируйте подключение в ваш airflow dag.

 

По итогу выполнения всех пунктов задания вам необходимо прислать ссылку на ваш репозиторий.