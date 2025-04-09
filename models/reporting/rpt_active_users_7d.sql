with users as (
    select * from {{ ref('rpt_daily_account__filtered') }}

)

select 
    count(*) as total_7days_users,
    count(case when is_active_user then user_id end) as total_7days_active_users,
    round(
        safe_divide(
            count(case when is_active_user then user_id end),
            count(*)
        ),
        4
    ) as active_users_7days_percentage

from users
