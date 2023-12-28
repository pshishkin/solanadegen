select * from subscribers;

select * from broadcasted_tokens;

SELECT dt.mint, min(dt.day) as day
FROM daily_trades dt
WHERE dt.mint NOT IN (SELECT mint FROM broadcasted_tokens)
GROUP BY dt.mint
HAVING SUM(dt.trades) * 325 > 20000
order by day desc;