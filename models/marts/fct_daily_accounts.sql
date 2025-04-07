{{ 
    config(
        materialized='incremental',
        unique_key='event_id',
        partition_by={
            "field": "date_day",
            "data_type": "datetime",
            "granularity": "day"
        }
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
            when reopened_at > last_closed_at then current_date
            else coalesce(cast(last_closed_at as date), current_date)
        end as account_ended_date
    from {{ ref('dim_accounts') }}
),

closed as (
    select * from {{ ref('int_backend__account_closed__deduplicated') }}

),

reopened as (
    select * from {{ ref('stg_backend__account_reopened') }}

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

joined as (
    select
        daily_accounts.*,
        transactions.number_of_transactions,
        case
            when reopened.account_id is not null then 'active'
            when closed.account_id is not null then 'closed'
            else 'active' 
        end as account_status

    from daily_accounts
    left join reopened
        on
            daily_accounts.account_id = reopened.account_id
            and daily_accounts.date_day = cast(reopened.account_reopened_at as date)
    left join closed
        on
            daily_accounts.account_id = closed.account_id
            and daily_accounts.date_day = cast(closed.account_closed_at as date)            
    left join transactions
        on
            daily_accounts.account_id = transactions.account_id
            and daily_accounts.date_day = transactions.transaction_created_date

),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['joined.date_day', 'joined.account_id']) }} as event_id,
        joined.account_id,
        created.user_id,
        joined.date_day,
        joined.number_of_transactions,
        joined.account_status

    from joined
    inner join created
        on joined.account_id = created.account_id

)

select * from final
