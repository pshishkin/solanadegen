CREATE OR REPLACE VIEW hourly_prices_final_view AS

with hourly_raw_volumes as (
    select
        date_trunc('hour', timestamp) as hour,
        mint,
        sum(abs(token_delta)) as token_delta,
        sum(abs(sol_delta)) as sol_delta,
        count(*) as num_trades
    from
        sol_trades
    where
        abs(sol_delta) > 0.01
        and array_length(program_ids, 1) = 1
        and program_ids[1] = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'

    group by 1, 2
    order by 1 desc, 2
),

hourly_semi_raw_prices as (
    select *, sol_delta / token_delta as sol_price from hourly_raw_volumes
),

raw_tokens_detailed as (
select count(*) as hours, mint,
       min(sol_price),
       max(sol_price),
       max(sol_price) / min(sol_price) as price_range
       from hourly_semi_raw_prices
where
    sol_delta > 10
    and num_trades > 5
group by mint
order by hours desc
),

filtered_tokens_detailed as (
select *
    from raw_tokens_detailed
    where price_range < 5
    and hours > 20
),

filtered_tokens as (
select mint from filtered_tokens_detailed
),

hourly_prices_final as (
    select mint, hour as hour_calc, hour + INTERVAL '1 hour' as hour_p1, sol_price
    from hourly_semi_raw_prices
    where mint in (select mint from filtered_tokens)
)

select * from hourly_prices_final;

select * from hourly_prices_final_view;

select distinct mint from hourly_prices_final_view;

select mint, count(*) from hourly_prices_final_view group by mint;

-- select count(*), sum(sol_price * token_trades.amount_spent) from token_trades
-- join hourly_prices_final on
--     hourly_prices_final.mint = token_trades.mint_spent
--     and hourly_prices_final.hour = date_trunc('hour', token_trades.timestamp);

-- select sum(abs(sol_delta)) from sol_trades
--where mint in (select mint from filtered_tokens);

show database;