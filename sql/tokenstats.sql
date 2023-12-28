select day, trades, (usd_volume + 70 * sol_volume) / token_volume * 1000 as priceper1000,
       (usd_volume + 70 * sol_volume) as usdvolume from daily_trades
         where mint = 'H7ed7UgcLp3ax4X1CQ5WuWDn6d1pprfMMYiv5ejwLWWU'
         order by day desc;

select * from trades
where mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263';