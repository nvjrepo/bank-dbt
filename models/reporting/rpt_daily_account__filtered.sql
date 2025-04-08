{{ 
    config(
        materialized='ephemeral'
    ) 
}}

select
    user_id,
    -- checking whether the account was active during that day
    account_status = 'active' as is_total_account,
    -- checking whether the filtered account have any transactions during that day
    account_status = 'active' 
    and number_of_transactions is not null
        as is_active_accounts

from {{ ref('fct_daily_accounts') }}
where date_day between '{{ var("7_period_reporting_end_date") }}' - 6 and '{{ var("7_period_reporting_end_date") }}'
