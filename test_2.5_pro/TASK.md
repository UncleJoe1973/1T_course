**Задание ПРО**

![изображение](https://github.com/UncleJoe1973/1T_course/assets/29273924/ec173905-7ad2-4659-84f9-6c330cf0efd8)

Для успешного выполнения задания ПРО, требуется выполнить базовую часть задания.

Задание:

При работе по сбору витрины для анализа эффективности плана продаж, к вам обратился ваш руководитель и сообщил, что надо обязательно к схеме данных добавить таблицу о промо-акциях на те или иные товары, иначе финальная витрина будет недостаточно информативной.
Таким образом, в вашей схеме появляется новая таблица - promo, которая содержит информацию о том: в каком магазине, в какой день, на какой продукт и какого размера была скидка.
Вам также нужно к итоговой витрине которую вы получили при выполнении базового задания, добавить несколько новых атрибутов:

  **avg(sales/date)** - среднее количество продаж в день,
  
  **max_sales** - максимальное количество продаж за один день,
  
  **date_max_sales** - день, в который произошло максимальное количество продаж,
  
  **date_max_sales_is_promo** - факт того, действовала ли скидка в тот день, когда произошло максимальное количество продаж,
  
  **avg(sales/date) / max_sales** - отношение среднего количества продаж к максимальному,
  
  **promo_len** - количество дней месяце когда на товар действовала скидка,
  
  **promo_sales_cnt** - количество товаров проданных в дни скидок,
  
  **promo_sales_cnt/fact_sales** - отношение количества товаров проданных в дни скидки к общему количеству проданных товаров за месяц,
  
  **promo_income** - доход с продаж в дни акций,
  
  **promo_income/fact_income** - отношение дохода с продаж в дни акций к общему доходу с продаж за месяц

Результат выполнения задания необходимо выложить в github/gitlab и указать ссылку на Ваш репозиторий (не забудьте: репозиторий должен быть публичным).
