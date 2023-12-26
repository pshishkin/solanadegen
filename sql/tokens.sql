with

t1 as (select mint, day, sum(trades) trades from daily_trades group by mint, day),

t2 as (select count(*) c, sum(trades) trades, mint from t1 group by mint order by trades desc)

select * from t2 where c < 2;

