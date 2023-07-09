create or replace view data_mart ("личный кабинет", "пол", "возрастная категория", "e-mail", "телефон", "самый покупаемый за месяц товар", 
									"самый часто покупаемый бренд", "категория самых частых покупок", "сумма, потраченная за месяц", "количество товаров со скидкой") as

with tmp_g (cookies, rnk, goods_name) as (
select 
	c.cookies,
	rank() over (partition by c.cookies order by sum(s.order_amt) desc),
	g.goods_name
from core_customers c
	left join core_sales s on c.cookies = s.cookies 
	left join core_goods g on s.goods_id = g.goods_id 
	left join core_calendar cal on s.order_dt = cal.date_id
group by c.cookies, g.goods_name, cal.sort_number 
having cal.sort_number >= all(select max(sort_number) from core_calendar) 
),

goods_r (cookies, goods) as (
select distinct
	t.cookies
	,array_to_string(ARRAY(SELECT t1.goods_name FROM tmp_g t1 WHERE t.cookies = t1.cookies and t1.rnk = 1 ORDER BY t.cookies), ', '::text)
from tmp_g t
where t.rnk = 1
),

tmp_b (cookies, rnk, brand_name) as (
select 
	s.cookies,
	rank() over (partition by s.cookies order by count(cg.goods_id) desc),
	cb.brand_name
from core_goods cg
	left join core_sales s on cg.goods_id  = s.goods_id 
	left join core_brands cb  on cg.brand_id  = cb.brand_id 
group by s.cookies, cb.brand_name  
),

brand_r (cookies, brand) as (
select distinct
	t.cookies
	,array_to_string(ARRAY(SELECT t1.brand_name FROM tmp_b t1 WHERE t.cookies = t1.cookies and t1.rnk = 1 ORDER BY t.cookies), ', '::text)
from tmp_b t
where t.rnk = 1
),

tmp_cat (cookies, rnk, category_name) as (
select 
	s.cookies,
	rank() over (partition by s.cookies order by count(cg.goods_id) desc),
	cc.category_name
from core_goods cg
	left join core_sales s on cg.goods_id  = s.goods_id 
	left join core_category cc on cg.category_id = cc.category_id  
group by s.cookies, cc.category_name 
),

category_r (cookies, category) as (
select distinct
	t.cookies
	,array_to_string(ARRAY(SELECT t1.category_name FROM tmp_cat t1 WHERE t.cookies = t1.cookies and t1.rnk = 1 ORDER BY t.cookies), ', '::text) AS date_max_sales
from tmp_cat t
where t.rnk = 1
),

disc_amt (cookies, disc_amt) as (
select 
	cs.cookies,
	sum(cs.order_amt)
from core_goods g
	join core_goods_promos gp on g.goods_id  = gp.goods_id
	join core_promos p on gp.promo_id  = p.promo_id
	left join core_sales cs on g.goods_id = cs.goods_id
where cs.order_dt between p.start_dt and p.end_dt
group by cs.cookies
)

select
	c.pk_id,
	c.sex,
	a.age_thr_name,
	c.email,
	c.phone,
	g.goods,
	b.brand,
	ca.category,
	sum(cs.order_amt * cg.price),
	coalesce(da.disc_amt, 0)
from core_customers c
	left join core_age_thr a on c.age_thr_id = a.age_thr_id
	left join core_sales cs on c.cookies = cs.cookies 
	left join core_goods cg on cg.goods_id = cs.goods_id 
	left join goods_r g on g.cookies = c.cookies 
	left join brand_r b on b.cookies = c.cookies 
	left join category_r ca on ca.cookies = c.cookies
	left join disc_amt da on da.cookies = c.cookies
	left join core_calendar cal on cs.order_dt = cal.date_id
group by c.pk_id,
	c.sex,
	a.age_thr_name,
	c.email,
	c.phone,
	g.goods,
	b.brand,
	ca.category,
	da.disc_amt,
	cal.sort_number
having cal.sort_number >= all(select max(sort_number) from core_calendar) 

