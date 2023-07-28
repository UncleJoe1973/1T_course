-- очистка таблиц от имеющихся данных
truncate table core.orders;
truncate table core.ship_mode cascade;
truncate table core.order_ids cascade;
truncate table core.customer_ids cascade;
truncate table core.customer_names cascade;
truncate table core.customer_segments cascade;
truncate table core.countries cascade;
truncate table core.cities cascade;
truncate table core.states cascade;
truncate table core.regions cascade;
truncate table core.product_ids cascade;
truncate table core.categories cascade;
truncate table core.subcategories cascade;
truncate table core.product_names cascade;

-- наполнение таблиц данными
insert into core.ship_mode (ship_mode_id, ship_mode_name)
select distinct 
	md5(ship_mode),
	ship_mode
from public.raw_store;

insert into core.order_ids (order_id_id, order_id_name)
select distinct 
	md5(order_id),
	order_id
from public.raw_store;

insert into core.customer_ids(customer_id_id, customer_id_name)
select distinct 
	md5(customer_id),
	customer_id
from public.raw_store;

insert into core.customer_names(customer_name_id, customer_name)
select distinct 
	md5(customer_name),
	customer_name
from public.raw_store;

insert into core.customer_segments(segment_id, segment_name)
select distinct 
	md5(segment),
	segment
from public.raw_store;

insert into core.countries(country_id, country_name)
select distinct 
	md5(country),
	country
from public.raw_store;

insert into core.cities(city_id, city_name)
select distinct 
	md5(city),
	city
from public.raw_store;

insert into core.states(state_id, state_name)
select distinct 
	md5(state),
	state
from public.raw_store;

insert into core.regions(region_id, region_name)
select distinct 
	md5(region),
	region
from public.raw_store;

insert into core.product_ids (product_id_id, product_id_name)
select distinct 
	md5(product_id),
	product_id
from public.raw_store;

insert into core.categories (category_id, category_name)
select distinct 
	md5(category),
	category
from public.raw_store;

insert into core.subcategories (subcategory_id, subcategory_name)
select distinct 
	md5(subcategory),
	subcategory
from public.raw_store;

insert into core.product_names (product_name_id, product_name)
select distinct 
	md5(product_name),
	product_name
from public.raw_store;

insert into core.orders (row_id, order_id_id, order_dt, ship_dt, ship_mode_id, customer_id_id, customer_name_id, segment_id, country_id, city_id,
								state_id, postal_code, region_id, product_id_id, category_id, subcategory_id, product_name_id, sales, quantity, discount, profit)
select 
		row_id,
		md5(order_id),
		order_date,
		ship_date,
		md5(ship_mode),
		md5(customer_id),
		md5(customer_name),
		md5(segment),
		md5(country),
		md5(city),
		md5(state),
		postal_code,
		md5(region),
		md5(product_id),
		md5(category),
		md5(subcategory),
		md5(product_name),
		sales,
		quantity,
		discount,
		profit
from public.raw_store;
