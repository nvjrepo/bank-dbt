with users as (
    select * from {{ ref('rpt_daily_account__filtered') }}

)

select 
    count(
        distinct
        case
            when is_total_account then user_id
        end
    ) as total_7days_users,
    count(
        distinct
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
