with tmp as (
    select 
        o.name as company,
        c.year_subs as year_subs,
        cast(months_between(c.Subscription_Date, p.Date_of_birth) as integer) div 12 as diff
    from customers c
        join people p on c.row_nbr = p.row_nbr
        join oranizations o on c.company = o.name
    ),

tmp_2 as (
    select
        tmp.company,
        tmp.year_subs,
        a.age_grp,
        count(a.age_grp) as cnt
    from tmp, age_grp a
    where tmp.diff between a.low_thr and a.low_thr
    group by tmp.company, tmp.year_subs, a.age_grp
    having  count(tmp.diff) > 0
), 

tmp_3 as (
select 
    company,
    year_subs,
    age_grp,
    max(cnt) over (partition by company, year_subs) as cnt
from tmp_2
)

select 
    company as Company,
    year_subs as Year,
    concat_ws(', ', collect_set(age_grp)) as Age_group
from tmp_3
group by company, year_subs
order by Company, Year;
