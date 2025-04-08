{{ 
    config(
        materialized='incremental',
        unique_key='event_id',
        on_schema_change='fail',
        partition_by={
            "field": "date_day",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by='account_id'
    )
}}

with dates as (
    select * from {{ ref('dim_dates') }}
    {% if is_incremental() %}
        where date_day >= (select cast(max(date_day) as date) from {{ this }}) - 3
    {% endif %}

),

created as (
    select
        *,
        cast(created_at as date) as account_started_date,
        case
            -- if the account is reopened after being last closed, it is still active
            when reopened_at > last_closed_at then current_date
            else coalesce(cast(last_closed_at as date), current_date)
        end as account_ended_date
    from {{ ref('dim_accounts') }}

),

closed as (
    select * from {{ ref('int_backend__account_closed__deduplicated') }}

),

transactions as (
    select * from {{ ref('stg_backend__account_transactions') }}

),

daily_accounts as (
    select
        dates.date_day,
        created.account_id
    
    from dates
    cross join created
    where
        dates.date_day between created.account_started_date
        and created.account_ended_date

),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['daily_accounts.date_day', 'daily_accounts.account_id']) }} as event_id,
        daily_accounts.account_id,
        created.user_id,
        daily_accounts.date_day,
        coalesce(transactions.number_of_transactions, 0) as number_of_transactions,
        case
            when date(created.reopened_at) = date(closed.account_closed_at)
                then
                    -- if reopening and closing events happen in the same date,
                    -- the account is "active" only when it was reopened AFTER being closed
                    case
                        when created.reopened_at > closed.account_closed_at then 'active'
                        else 'closed'
                    end
            when daily_accounts.date_day between cast(created.last_closed_before_reopened_at as date) and cast(created.reopened_at as date) - 1 then 'closed'            
            else 'active' 
        end as account_status

    from daily_accounts
    left join created
        on
            daily_accounts.account_id = created.account_id
    left join closed
        on
            daily_accounts.account_id = closed.account_id
            and daily_accounts.date_day = cast(closed.account_closed_at as date)            
    left join transactions
        on
            daily_accounts.account_id = transactions.account_id
            and daily_accounts.date_day = transactions.transaction_created_date

)

select * from final
