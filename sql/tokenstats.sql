select day, trades, (usd_volume + 70 * sol_volume) / token_volume * 1000 as priceper1000, (usd_volume + 70 * sol_volume) as usdvolume from daily_trades
         where mint = 'F47vvwFYuLioQsqEVAjqdY6Yihc8wVRiUcfHGcBR9XUs'
         order by day desc;

select * from trades;