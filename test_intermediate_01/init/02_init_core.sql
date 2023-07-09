create table if not exists core_category
(
	category_id varchar(32) PRIMARY KEY,
	category_name varchar(100)
);

insert into core_category (category_id, category_name)
select distinct
	md5(category),
	category
from raw_sales;

create table if not exists core_models
(
	model_id varchar(32) PRIMARY KEY,
	model_name varchar(100)
);

insert into core_models (model_id, model_name)
select distinct
	md5(model_name),
	model_name
from raw_sales;

create table if not exists core_brands
(
	brand_id varchar(32) PRIMARY KEY,
	brand_name varchar(100)
);

insert into core_brands (brand_id, brand_name)
select distinct
	md5(brand_name),
	brand_name
from raw_sales;

create table if not exists core_promos
(
	promo_id varchar(32) PRIMARY KEY,
	promo_name varchar(10), 
	start_dt timestamp,
	end_dt timestamp,
	discount numeric(7, 2)
);

insert into core_promos (promo_id, promo_name, start_dt, end_dt, discount)
select distinct
	md5(id_promo),
	id_promo,
	date_trunc('day', to_timestamp(start_dt)),
	date_trunc('day', to_timestamp(end_dt)),
	discount
from raw_promos;

create table if not exists core_goods
(
	goods_id varchar(32) PRIMARY KEY,
	goods_name varchar(100),
	model_id varchar(32) references core_models (model_id) on delete cascade on update cascade,
	brand_id varchar(32) references core_brands (brand_id) on delete cascade on update cascade,
	category_id varchar(32) references core_category (category_id) on delete cascade on update cascade,
	price numeric(7, 2)
);

insert into core_goods (goods_id, goods_name, model_id, brand_id, category_id, price)
select distinct
	md5(goods_name),
	goods_name,
	md5(model_name),
	md5(brand_name),
	md5(category),
	price
from raw_sales;

create table if not exists core_goods_promos
(
	goods_promos_id varchar(32) PRIMARY KEY,
	promo_id varchar(32) references core_promos (promo_id) on delete cascade on update cascade,
	goods_id varchar(32) references core_goods (goods_id) on delete cascade on update cascade
);

insert into core_goods_promos (goods_promos_id, promo_id, goods_id)
select distinct
	md5(id_promo || goods_name),
	md5(id_promo),
	md5(goods_name)
from raw_promos;

create table if not exists core_age_thr
(
	age_thr_id varchar(32) PRIMARY KEY,
	age_thr_name varchar(100),
	low_threshold integer,
	high_threshold integer
);

insert into core_age_thr (age_thr_id, age_thr_name, low_threshold, high_threshold)
select distinct
	md5(category),
	category,
	low_threshold,
	high_threshold
from raw_age_thr;

create table if not exists core_customers
(
	pk_id varchar(10),
	cookies varchar(32) PRIMARY KEY,
	cust_name varchar(100),
	sex varchar(2),
	birth_dt timestamp,
	email varchar(100),
	phone varchar(100),
	age_thr_id varchar(32) references core_age_thr (age_thr_id) on delete cascade on update cascade
);

insert into core_customers (pk_id, cookies, cust_name, sex, birth_dt, email, phone)
select distinct
	pk_id,
	cookies,
	cust_name,
	sex,
	date_trunc('day', to_timestamp(birth_dt, 'DD.MM.YYYY')),
	email,
	phone
from raw_customers;

update core_customers c
set age_thr_id = a.age_thr_id
from core_age_thr a
where extract('year' from age(c.birth_dt)) between a.low_threshold and a.high_threshold;

create table if not exists core_sales
(
	order_id varchar(32) PRIMARY KEY,
	order_mark varchar(10),
	order_line integer,
	order_dt timestamp,
	cookies varchar(32) references core_customers (cookies) on delete cascade on update cascade,
	goods_id varchar(32) references core_goods (goods_id) on delete cascade on update cascade,
	order_amt integer,
	sales_hour smallint
);

insert into core_sales (order_id, order_mark, order_line, order_dt, cookies, goods_id, order_amt, sales_hour)
select 
	md5(order_id),
	order_id,
	row_number() over(partition by order_id order by order_id),
	date_trunc('day', to_timestamp(order_dt)),
	cookies,
	md5(goods_name),
	(order_sum / price),
	extract(hour from to_timestamp(order_dt))
from raw_sales;

create table if not exists core_calendar
(
	date_id timestamp PRIMARY KEY,
	date_str varchar(12),
	weekday_name varchar(12),
	weekday_number varchar(12),
	week_number varchar(12),
	month_name varchar(12),
	month_number varchar(12),
	year_number varchar(12),
	sort_number varchar(12)
);

insert into core_calendar
with tmp (mx_dt, mn_dt) as (
select max(dt), min(dt) 
from (
	select birth_dt as dt from public.core_customers 
	union all 
	select order_dt from public.core_sales 
	union all 
	select end_dt from public.core_promos) tm
),

dts (dt) as (
	select * from generate_series((select mn_dt from tmp), (select mx_dt from tmp), '1 day')
)

select
	dt,	
	to_char(dt, 'YYYY-MM-DD'),
	to_char(dt, 'dy'),
	to_char(dt, 'ID'),
	to_char(dt, 'IW'),
	to_char(dt, 'month'),
	to_char(dt, 'MM'),
	to_char(dt, 'YYYY'),
	to_char(dt, 'YYYY') || '-' || to_char(dt, 'MM')
from dts;

