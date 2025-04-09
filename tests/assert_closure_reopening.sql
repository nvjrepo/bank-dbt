{# /* The assertion is to ensure an account cannot be closed and reopened at the same time */ #}

with closure as (
    select * from {{ ref('stg_backend__account_closure') }}
),

reopening as (
    select * from {{ ref('stg_backend__account_reopening') }}
),

checked as (
    select
        closure.account_id,
        closure.account_closed_at,
        reopening.account_reopened_at
    from closure
    inner join reopening
        on closure.account_id = reopening.account_id
    where date_diff(reopening.account_reopened_at, closure.account_closed_at, second) = 0
    
)

select * from checked
