create table sales
(
	order_id varchar(64),
	order_dt date,
	goods_id varchar(64),
	order_amt integer
)
distributed by (order_id)
partition by range (order_dt) ( START (date '2023-01-01') INCLUSIVE END (date '2024-01-01') EXCLUSIVE EVERY (INTERVAL '1 month') );

COPY sales
FROM '/code/mine/data/core_sales.csv'
DELIMITER ','
CSV HEADER;

create table products
(
	goods_id varchar(64),
	goods_name varchar(200),
	price integer
)
distributed by (goods_id);

COPY products
FROM '/code/mine/data/core_goods.csv'
DELIMITER ','
CSV HEADER;

select 
	to_char(s.order_dt::timestamp, 'YYYY-Mon') as "Sales month",
	sum(s.order_amt * p.price) as "Sales sum"
from sales s 
	join products p using(goods_id)
group by to_char(s.order_dt::timestamp, 'YYYY-Mon')
order by 1;
