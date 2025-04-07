{%- set reporting_date = '2020-01-04' -%}

with users as (
    select
        user_id,
        account_status = 'active' as is_total_account,
        account_status = 'active'
            and number_of_transactions is not null
        as is_active_accounts
 
    from {{ ref('fct_daily_accounts') }}
    where date_day between '{{ reporting_date }}' - 6 and '{{ reporting_date }}'

)


select 
    count(distinct
        case
            when is_total_account then user_id
        end
    ) as total_7days_users,
    count(distinct
        case
            when is_active_accounts then user_id
        end
    ) as total_7days_active_users,
    round(
        safe_divide(
            count(distinct case when is_active_accounts then user_id end),
            count(distinct case when is_total_account then user_id end)
        ),
        4
    ) as active_users_7days_percentage

from users
