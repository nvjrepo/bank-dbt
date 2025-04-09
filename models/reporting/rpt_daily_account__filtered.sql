{{ 
    config(
        materialized='ephemeral'
    ) 
}}

with filtered_accounts as (
    select * from {{ ref('fct_daily_accounts') }}
    where date_day between '{{ var("7_period_reporting_end_date") }}' - 6 and '{{ var("7_period_reporting_end_date") }}'

),

users as (
    select distinct user_id from filtered_accounts
    where account_status = 'active'

),

active_users as (
    select distinct user_id from filtered_accounts
    where number_of_transactions > 0

)

select
    users.user_id,
    active_users.user_id is not null as is_active_user
 
from users
left join active_users
    on users.user_id = active_users.user_id
