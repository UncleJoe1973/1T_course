-- структура таблиц raw данных

create table if not exists raw_promos
(
	id_promo varchar(10),
	goods_name varchar(100),
	start_dt bigint,
	end_dt bigint,
	discount numeric(7, 2)
);

create table if not exists raw_customers
(
	pk_id varchar(10),
	cookies varchar(32),
	cust_name varchar(100),
	sex varchar(2),
	birth_dt varchar(32),
	email varchar(100),
	phone varchar(100)
);

create table if not exists raw_sales
(
	order_id varchar(10),
	order_dt bigint,
	goods_name varchar(100),
	price numeric(7, 2),
	brand_name varchar(100),
	model_name varchar(100),
	category varchar(100),
	cookies varchar(32),
	order_sum numeric(7, 2)
);

create table if not exists raw_age_thr
(
	category varchar(32),
	low_threshold integer,
	high_threshold integer
);

-- загрузка raw данных

COPY raw_promos
FROM '/docker-entrypoint-initdb.d/data/promo.csv'
DELIMITER ';'
ENCODING 'UTF8'
CSV HEADER;

COPY raw_customers
FROM '/docker-entrypoint-initdb.d/data/customers.csv'
DELIMITER ';'
ENCODING 'UTF8'
CSV HEADER;

COPY raw_sales
FROM '/docker-entrypoint-initdb.d/data/sales.csv'
DELIMITER ';'
ENCODING 'UTF8'
CSV HEADER;

COPY raw_age_thr
FROM '/docker-entrypoint-initdb.d/data/age_thr.csv'
DELIMITER ';'
ENCODING 'UTF8'
CSV HEADER;
