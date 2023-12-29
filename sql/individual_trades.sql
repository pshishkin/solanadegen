select count(*), date_trunc('day', timestamp) from trades group by 2 order by 2 desc;

select * from trades where date_trunc('day', timestamp) = '2023-12-24';