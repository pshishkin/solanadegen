select * from daily_trades order by (usd_volume + sol_volume * 50) desc;

select count(*), sum(trades), sum(usd_volume + sol_volume * 50) from daily_trades;


with
t1 as (select mint from daily_trades group by mint)
select count(*) from t1;