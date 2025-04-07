with created as (
    select * from {{ ref('stg_backend__account_creation') }}
),

closed as (
    select
        account_id,
        count(*) as number_of_closed_attempts,
        min(account_closed_at) as first_closed_at,
        max(account_closed_at) as last_closed_at

    from {{ ref('int_backend__account_closed__deduplicated') }}
    {{ dbt_utils.group_by(1) }}

),

reopened as (
    select * from {{ ref('stg_backend__account_reopening') }}
),

transactions as (
    select * from {{ ref('stg_backend__account_transactions') }}
),

transactions_agg as (
    select
        transactions.account_id,
        sum(transactions.number_of_transactions) as number_of_transactions,
        min(transactions.transaction_created_date) as first_transaction_date,
        max(transactions.transaction_created_date) as last_transaction_date

    from transactions
    left join closed
        on transactions.account_id = closed.account_id
    where transactions.transaction_created_date <= cast(closed.last_closed_at as date)
    {{ dbt_utils.group_by(1) }}

)

select
    created.account_id,
    created.user_id,
    created.account_type,
    created.account_created_at as created_at,
    closed.number_of_closed_attempts,
    closed.first_closed_at,
    closed.last_closed_at,
    reopened.account_reopened_at as reopened_at,
    transactions_agg.number_of_transactions,
    transactions_agg.first_transaction_date,
    transactions_agg.last_transaction_date,

    case
        when reopened.account_id is not null then 'active'
        when closed.account_id is not null then 'closed'
        else 'active'
    end as account_status

from created
left join closed
    on created.account_id = closed.account_id
left join reopened
    on created.account_id = reopened.account_id
left join transactions_agg
    on created.account_id = transactions_agg.account_id
