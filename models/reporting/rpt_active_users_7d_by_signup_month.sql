{%- set reporting_date = '2020-06-08' -%}

with users as (
    select
        user_id,
        account_status = 'active' as is_total_account,
        account_status = 'active' 
            and number_of_transactions is not null
        as is_active_accounts

    from {{ ref('fct_daily_accounts') }}
    where date_day between '{{ reporting_date }}' - 6 and '{{ reporting_date }}'

),

dim as (
    select * from {{ ref('dim_users') }}
)

select 
    cast(date_trunc(dim.first_account_created_at, month) as date) as first_account_signup_month,
    count(distinct 
        case 
            when users.is_total_account then users.user_id 
        end
    ) as total_7days_users,
    count(distinct 
        case 
            when users.is_active_accounts then users.user_id 
        end
    ) as total_7days_active_users,
    round(
        safe_divide(
            count(distinct case when users.is_active_accounts then users.user_id end),
            count(distinct case when users.is_total_account then users.user_id end)
        ),
        4
    ) as active_users_7days_percentage

from users
inner join dim
    on users.user_id = dim.user_id
{{ dbt_utils.group_by(1) }}
