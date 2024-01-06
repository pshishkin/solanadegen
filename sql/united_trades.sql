drop view if exists united_trades;

CREATE OR REPLACE VIEW united_trades AS

with
    st1 as (
        select
            'sol' as source,
            signature,
            program_ids,
            timestamp,
            mint,
            token_delta,
            sol_delta,
            trader
        from sol_trades
        ),

    tt2 as (
    select
        'token' as source,
        signature,
        program_ids,
        timestamp,
        mint_spent as mint,
        -amount_spent as token_delta,
        amount_got * t2.sol_price as sol_delta,
        trader
    from token_trades t1
    join hourly_prices_final_view t2 on t1.mint_got = t2.mint and date_trunc('hour', t1.timestamp) = t2.hour_p1
    where sol_delta = 0
    ),

    tt3 as (
        select
            'token' as source,
            signature,
            program_ids,
            timestamp,
            mint_got as mint,
            amount_got as token_delta,
            -amount_spent * t2.sol_price as sol_delta,
            trader
    from token_trades t1
    join hourly_prices_final_view t2 on t1.mint_spent = t2.mint and date_trunc('hour', t1.timestamp) = t2.hour_p1
    where sol_delta = 0
    ),

    unioned as (
        select * from st1
        union all
        select * from tt2
        union all
        select * from tt3
        order by timestamp
    )

select * from unioned;

select count(*), sum(abs(sol_delta)) as vol, source from united_trades group by source;

select *, -sol_delta / token_delta * 10000 from united_trades where mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
and timestamp > timestamp '2024-01-03 20:00:00' and timestamp < timestamp '2024-01-03 20:10:00'
order by timestamp limit 100;

select *, -sol_delta / token_delta * 10000 from united_trades where mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
and timestamp > timestamp '2024-01-04 08:00:00' and timestamp < timestamp '2024-01-04 08:10:00'
order by timestamp limit 100;

select * , -sol_delta / token_delta * 10000 as price from united_trades
where mint = '14JnYcbAooDAZVb72DgmFGVZGDon5Ko4SN6ELrpU6ood'
order by timestamp;