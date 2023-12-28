select * from subscribers;

select * from broadcasted_tokens;

SELECT dt.mint, min(dt.day) as first_trade_day
FROM daily_trades dt
WHERE dt.mint NOT IN (SELECT mint FROM broadcasted_tokens)
GROUP BY dt.mint
HAVING SUM(dt.trades) * 325 > 30000
order by first_trade_day desc
LIMIT 5;

SELECT dt.mint, min(dt.day) as first_trade_day
FROM daily_trades dt
GROUP BY dt.mint
HAVING SUM(dt.trades) * 325 > 30000
order by first_trade_day desc
LIMIT 5;
