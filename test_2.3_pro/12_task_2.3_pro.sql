--# Вопрос 1 №--

create or replace view public.question_pro_1 as
select
	r.reader_name as "Читатель",
	count(i.issue_id) as "Потеряно книг"	
from issues i
	left join readers r on i.fk_reader_id = r.reader_id
where not i.returned and i.return_dt is not null
group by r.reader_name;

