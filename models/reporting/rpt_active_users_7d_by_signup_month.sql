{%- set reporting_date = '2020-06-08' -%}

with users as (
    select distinct user_id
    from {{ ref('fct_daily_accounts') }}
    where
        account_status = 'active'
        and date_day =  '{{ reporting_date }}'
),

active_users as (
    select distinct user_id
    from {{ ref('fct_daily_accounts') }}
    where
        number_of_transactions is not null
        and date_day between '{{ reporting_date }}' - 6 and '{{ reporting_date }}'
),

dim as (
    select * from {{ ref('dim_users') }}
)

select 
    cast(date_trunc(dim.first_account_created_at, month) as date) as first_account_signup_month,
    count(*) as total_7days_users,
    count(active_users.user_id) as total_7days_active_users,
    round(
        safe_divide(
            count(active_users.user_id),
            count(*)
        ),
        4
    ) as active_users_7days_percentage

from users
left join active_users
    on users.user_id = active_users.user_id
inner join dim
    on users.user_id = dim.user_id
{{ dbt_utils.group_by(1) }}
