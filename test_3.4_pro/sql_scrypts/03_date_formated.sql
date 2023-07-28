create table if not exists public.date_formated as
select distinct
		order_dt, 
		ship_dt,
		to_char(order_dt, 'MM/DD/YYYY') as od_init,
		to_char(ship_dt, 'MM/DD/YYYY') as sd_init,
		substring(to_char(order_dt, 'MM/DD/YYYY'), 7, 4) || '-' || substring(to_char(order_dt, 'MM/DD/YYYY'), 1, 2) || '-' || substring(to_char(order_dt, 'MM/DD/YYYY'), 4, 2) as od_formated,
		substring(to_char(ship_dt, 'MM/DD/YYYY'), 7, 4) || '-' || substring(to_char(ship_dt, 'MM/DD/YYYY'), 1, 2) || '-' || substring(to_char(ship_dt, 'MM/DD/YYYY'), 4, 2) as sd_formated
from core.orders