create table if not exists core.core_store as
select 
	row_id,
	order_id_id,
	order_dt,
	ship_dt,
	ship_mode_id,
	customer_id_id,
	customer_name_id,
	segment_id,
	country_id,
	city_id,
	state_id,
	postal_code,
	region_id,
	product_id_id,
	category_id,
	subcategory_id,
	product_name_id,
	sales,
	quantity,
	discount,
	profit
from core.orders
	left join core.customer_segments s using (segment_id)
where s.segment_name like 'Corporate'