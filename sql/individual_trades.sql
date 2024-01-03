select * from sol_trades order by timestamp desc;

select count(*) from sol_trades;

select * from token_trades order by timestamp desc;

select count(*) from token_trades;


select count(*), date_trunc('day', timestamp) from trades group by 2 order by 2 desc;

select * from sol_trades where date_trunc('day', timestamp) = '2023-12-24';


select count(*), date_trunc('day', timestamp), mint
from trades
where sol_volume > 0
group by 2, 3
having count(*) > 5
order by 2 desc, 1 desc
;