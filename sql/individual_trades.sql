select count(*), date_trunc('day', timestamp) from trades group by 2 order by 2 desc;

select * from trades where date_trunc('day', timestamp) = '2023-12-24';


select count(*), date_trunc('day', timestamp), mint
from trades
where sol_volume > 0
group by 2, 3
having count(*) > 5
order by 2 desc, 1 desc
;