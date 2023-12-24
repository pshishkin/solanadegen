select * from daily_trades order by (usd_volume + sol_volume * 50) desc;

select count(*), sum(trades) from daily_trades;