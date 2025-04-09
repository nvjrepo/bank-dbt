{# /* The assertion is to ensure an account cannot have any transaction incurred during closing period,
      except for the start date of closing period
 */ #}

with daily_accounts as (
    select * from {{ ref('fct_daily_accounts') }}
),

accounts as (
    select * from {{ ref('dim_accounts') }}
),

checked as (
    select daily_accounts.* from daily_accounts
    inner join accounts
        on daily_accounts.account_id = accounts.account_id
    where
        true
        and accounts.reopened_at is not null
        and daily_accounts.account_status = 'closed'
        and daily_accounts.number_of_transactions != 0
        and daily_accounts.date_day 
        != cast(daily_accounts.last_closed_before_reopened_at as date)
)

select * from checked
