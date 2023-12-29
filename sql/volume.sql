select * from daily_trades order by (usd_volume + sol_volume * 50) desc;

select count(*), sum(trades), sum(usd_volume + sol_volume * 50) from daily_trades;

select day, count(*), sum(trades), sum(usd_volume + sol_volume * 50) from daily_trades group by day order by day;


with
t1 as (select mint from daily_trades group by mint)
select count(*) from t1;

select count(*) from transactions;

select * from transactions where timestamp < TIMESTAMP '2023-12-12' limit 10;

SELECT signature, timestamp, bucket FROM transactions
    WHERE processed = FALSE AND bucket < 200
    ORDER BY timestamp DESC limit 100;