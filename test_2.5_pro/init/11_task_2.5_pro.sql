-- таблицы реализации по магазинам с учетом промо-скидок

CREATE TABLE IF NOT EXISTS new_sales_d (shop_id, date, product_id, sales_cnt, new_price) as 
		SELECT distinct s.shop_id,
            s.date,
            s.product_id,
            s.sales_cnt,
            p.price
		FROM shop_dns s
		JOIN products p on s.product_id = p.product_id;


UPDATE new_sales_d 
SET new_price =  new_price * (1 - p.discount)
FROM promo p 
WHERE new_sales_d.shop_id = p.shop_id and new_sales_d.product_id = p.product_id and new_sales_d.date = p.promo_date;

CREATE TABLE IF NOT EXISTS new_sales_m (shop_id, date, product_id, sales_cnt, new_price) as 
		SELECT distinct s.shop_id,
            s.date,
            s.product_id,
            s.sales_cnt,
            p.price
		FROM shop_mvideo s
		JOIN products p on s.product_id = p.product_id;


UPDATE new_sales_m 
SET new_price =  new_price * (1 - p.discount)
FROM promo p 
WHERE new_sales_m.shop_id = p.shop_id and new_sales_m.product_id = p.product_id and new_sales_m.date = p.promo_date;

CREATE TABLE IF NOT EXISTS new_sales_s (shop_id, date, product_id, sales_cnt, new_price) as 
		SELECT distinct s.shop_id,
            s.date,
            s.product_id,
            s.sales_cnt,
            p.price
		FROM shop_sitilink s
		JOIN products p on s.product_id = p.product_id;


UPDATE new_sales_s 
SET new_price =  new_price * (1 - p.discount)
FROM promo p 
WHERE new_sales_s.shop_id = p.shop_id and new_sales_s.product_id = p.product_id and new_sales_s.date = p.promo_date;

-- Формирование витрины
CREATE OR REPLACE VIEW public."task_2.5_pro"
AS WITH fct_tbl AS (
         SELECT d.shop_id,
            d.date,
            d.product_id,
            d.sales_cnt,
            d.new_price
           FROM new_sales_d d
        UNION ALL
         SELECT m.shop_id,
            m.date,
            m.product_id,
            m.sales_cnt,
            m.new_price
           FROM new_sales_m m
        UNION ALL
         SELECT s_1.shop_id,
            s_1.date,
            s_1.product_id,
            s_1.sales_cnt,
            s_1.new_price
           FROM new_sales_s s_1
        ), 
        
 --- Показатели, рассчитанные в Задаче 2.5      

fct_tbl_agg AS (
         SELECT fct_tbl.shop_id,
            (date_trunc('MONTH'::text, fct_tbl.date::timestamp with time zone) + '1 mon'::interval - '1 day'::interval)::date AS last_day,
            fct_tbl.product_id,
            sum(fct_tbl.sales_cnt) AS fct_cnt,
            sum(fct_tbl.sales_cnt::numeric * fct_tbl.new_price) AS fct_inc
           FROM fct_tbl
          GROUP BY fct_tbl.shop_id, ((date_trunc('MONTH'::text, fct_tbl.date::timestamp with time zone) + '1 mon'::interval - '1 day'::interval)::date), fct_tbl.product_id
        ), 
 
--- свод 1        
tmp_1(shop_id, product_id, last_day, sales_fact, sales_plan, "sales_fact/sales_plan", income_fact, income_plan, "income_fact/income_plan") AS (
         SELECT sh.shop_id,
            pr.product_id,
            f.last_day,
            f.fct_cnt,
            p_1.plan_cnt,
            round(f.fct_cnt::numeric / p_1.plan_cnt::numeric, 2) AS round,
            f.fct_inc,
            p_1.plan_cnt::numeric * pr.price AS "?column?",
            round(f.fct_inc / (p_1.plan_cnt::numeric * pr.price), 2) AS round
           FROM plan p_1
             LEFT JOIN fct_tbl_agg f ON p_1.shop_id = f.shop_id AND p_1.plan_date = f.last_day AND p_1.product_id = f.product_id
             LEFT JOIN products pr ON p_1.product_id = pr.product_id
             LEFT JOIN shops sh ON p_1.shop_id = sh.shop_id
        ), 
        
  --- Новые показатели, рассчитанные для Задачи 2.5 PRO 

maxx_d(shop_id, product_id, maxx) AS (
         SELECT b.shop_id,
            b.product_id,
            max(b.sales_cnt) AS max
           FROM fct_tbl b
          GROUP BY b.shop_id, b.product_id
        ), 
        
days_d(shop_id, product_id, m_day) AS (  --даты максимальных продаж
         SELECT b.shop_id,
            b.product_id,
            b.date
           FROM fct_tbl b
             LEFT JOIN maxx_d m ON b.shop_id = m.shop_id AND b.product_id = m.product_id
          WHERE b.sales_cnt = m.maxx
        ), 
        
pr_d(shop_id, product_id, promo_date, date_max_sales_is_promo) AS ( --были ли максимальные продажи в день промо по дням
         SELECT DISTINCT dd.shop_id,
            dd.product_id,
            p_1.promo_date,
                CASE
                    WHEN p_1.promo_date = dd.m_day THEN 1
                    ELSE 0
                END AS "case"
           FROM promo p_1,
            days_d dd
          WHERE p_1.product_id = dd.product_id AND p_1.shop_id = dd.shop_id
        ), 
        
pr_d_fin(shop_id, product_id, last_day, date_max_sales_is_promo) AS (  --были ли максимальные продажи в день промо итог
         SELECT p_1.shop_id,
            p_1.product_id,
            (date_trunc('MONTH'::text, p_1.promo_date::timestamp with time zone) + '1 mon'::interval - '1 day'::interval)::date AS date,
            max(p_1.date_max_sales_is_promo) AS max
           FROM pr_d p_1
          GROUP BY p_1.shop_id, p_1.product_id, ((date_trunc('MONTH'::text, p_1.promo_date::timestamp with time zone) + '1 mon'::interval - '1 day'::interval)::date)
        ), 
        
cnt_pr_d(shop_id, product_id, last_day, promo_sales_cnt) AS (  --продажи за месяц в дни промо в штуках
         SELECT d.shop_id,
            d.product_id,
            (date_trunc('MONTH'::text, d.date::timestamp with time zone) + '1 mon'::interval - '1 day'::interval)::date AS date,
            sum(d.sales_cnt) AS sum
           FROM fct_tbl d
             LEFT JOIN promo p_1 ON d.product_id = p_1.product_id AND d.shop_id = p_1.shop_id
          WHERE d.date = p_1.promo_date
          GROUP BY ((date_trunc('MONTH'::text, d.date::timestamp with time zone) + '1 mon'::interval - '1 day'::interval)::date), d.shop_id, d.product_id
        ), 
        
cnt_pr_inc_d(shop_id, product_id, last_day, promo_income) AS ( --продажи за месяц в дни промо в деньгах
         SELECT d.shop_id,
            d.product_id,
            (date_trunc('MONTH'::text, d.date::timestamp with time zone) + '1 mon'::interval - '1 day'::interval)::date AS date,
            round(sum(d.sales_cnt::numeric * d.new_price), 2) AS round
           FROM fct_tbl d
             LEFT JOIN promo n ON d.product_id = n.product_id AND d.shop_id = n.shop_id
          WHERE d.date = n.promo_date
          GROUP BY ((date_trunc('MONTH'::text, d.date::timestamp with time zone) + '1 mon'::interval - '1 day'::interval)::date), d.shop_id, d.product_id
        ), 
 
--- свод 2
tmp_2(shop_id, product_id, last_day, "avg(sales/date)", max_sales, date_max_sales, date_max_sales_is_promo, "avg(sales/date) / max_sales", "promo_sales_cnt/fact_sales", promo_income, "promo_income/fact_income") AS (
         SELECT d.shop_id,
            d.product_id,
            (date_trunc('MONTH'::text, d.date::timestamp with time zone) + '1 mon'::interval - '1 day'::interval)::date AS last_day,
            sum(d.sales_cnt) / count(d.date) AS "avg(sales/date)",
            max(d.sales_cnt) AS max_sales,
            array_to_string(ARRAY( SELECT dd.m_day
                   FROM days_d dd
                  WHERE d.product_id = dd.product_id AND d.shop_id = dd.shop_id
                  ORDER BY dd.m_day), ', '::text) AS date_max_sales,
            f.date_max_sales_is_promo,
            round(sum(d.sales_cnt)::numeric / count(d.date)::numeric / max(d.sales_cnt)::numeric, 2) AS "avg(sales/date) / max_sales",
            c.promo_sales_cnt AS "promo_sales_cnt/fact_sales",
            i.promo_income,
            i.promo_income AS "promo_income/fact_income"
           FROM fct_tbl d
             LEFT JOIN promo p_1 ON d.product_id = p_1.product_id AND d.shop_id = p_1.shop_id
             LEFT JOIN cnt_pr_d c ON d.product_id = c.product_id AND d.shop_id = c.shop_id
             LEFT JOIN cnt_pr_inc_d i ON d.product_id = i.product_id AND d.shop_id = i.shop_id
             LEFT JOIN pr_d_fin f ON d.product_id = f.product_id AND d.shop_id = f.shop_id
          GROUP BY ((date_trunc('MONTH'::text, d.date::timestamp with time zone) + '1 mon'::interval - '1 day'::interval)::date), d.shop_id, d.product_id, c.promo_sales_cnt, i.promo_income, f.date_max_sales_is_promo
        ), 
        
len_promo(shop_id, product_id, last_day, promo_len) AS (  -- длительность промо по всем товарам и магазинам
         SELECT p_1.shop_id,
            p_1.product_id,
            (date_trunc('MONTH'::text, p_1.promo_date::timestamp with time zone) + '1 mon'::interval - '1 day'::interval)::date AS date,
            count(p_1.promo_date) AS count
           FROM promo p_1
          GROUP BY p_1.shop_id, p_1.product_id, ((date_trunc('MONTH'::text, p_1.promo_date::timestamp with time zone) + '1 mon'::interval - '1 day'::interval)::date)
        )
        
-- финальная витрина
 SELECT s.shop_name AS "Магазин",
    p.product_name AS "Продукт",
    to_char(t.last_day::timestamp with time zone, 'Mon-YYYY'::text) AS "Месяц продаж",
    t.sales_fact,
    t.sales_plan,
    t."sales_fact/sales_plan",
    t.income_fact,
    t.income_plan,
    t."income_fact/income_plan",
    tt."avg(sales/date)",
    tt.max_sales,
    tt.date_max_sales,
    tt.date_max_sales_is_promo,
    tt."avg(sales/date) / max_sales",
    lp.promo_len,
    round(tt."promo_sales_cnt/fact_sales"::numeric / t.sales_fact::numeric, 2) AS "promo_sales_cnt/fact_sales",
    tt.promo_income,
    round(tt.promo_income / t.income_fact, 2) AS "promo_income/fact_income"
   FROM tmp_1 t
     LEFT JOIN tmp_2 tt ON t.shop_id = tt.shop_id AND t.product_id = tt.product_id AND t.last_day = tt.last_day
     LEFT JOIN len_promo lp ON t.shop_id = lp.shop_id AND t.product_id = lp.product_id AND t.last_day = lp.last_day
     LEFT JOIN cnt_pr_inc_d cm ON t.shop_id = cm.shop_id AND t.product_id = cm.product_id AND t.last_day = cm.last_day
     LEFT JOIN shops s ON t.shop_id = s.shop_id
     LEFT JOIN products p ON t.product_id = p.product_id
  ORDER BY t.shop_id, t.product_id;
