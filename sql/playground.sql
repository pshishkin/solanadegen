select * from sol_trades order by timestamp desc;

select count(*) from sol_trades;

-- select where array size of program_ids > 1
select * from sol_trades
         where program_ids[2] is not null
         order by timestamp desc;

select * from token_trades
         where signature = '5bx43SGzxvF8nKKrn7MVQFAEan7yg9ZJCtRXvQq3FvVR8HUrKa9pTELk5xBpvmEMQoq7eUikPmohbcWnoio4isnv';

select * from token_trades order by timestamp desc;

select count(*) from token_trades;

-- select where array size of program_ids > 1
select * from token_trades
         where program_ids[2] is not null
         order by timestamp desc;

select count(*), date_trunc('day', timestamp) from trades group by 2 order by 2 desc;

select * from sol_trades where date_trunc('day', timestamp) = '2023-12-24';


select count(*), date_trunc('day', timestamp), mint
from trades
where sol_volume > 0
group by 2, 3
having count(*) > 5
order by 2 desc, 1 desc
;

with m1 as (
    select count(*) as sol_tr, mint as mint
    from sol_trades
    group by mint
),
m2 as (
    select count(*) as token_tr_sp, mint_spent as mint
    from token_trades
    group by mint_spent
),
m3 as (
    select count(*) as token_tr_rec, mint_got as mint
    from token_trades
    group by mint_got
)

select sol_tr + token_tr_rec + token_tr_sp, *
from m1 join m2 using (mint) join m3 using (mint) order by 1 desc;