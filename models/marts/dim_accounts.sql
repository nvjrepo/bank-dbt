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
    select
        account_id,
        sum(number_of_transactions) as number_of_transactions,
        max(transaction_created_date) as last_transaction_date,
        min(transaction_created_date) as first_transaction_date

    from {{ ref('stg_backend__account_transactions') }}
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
    transactions.number_of_transactions,
    transactions.first_transaction_date,
    transactions.last_transaction_date,

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
left join transactions
    on created.account_id = transactions.account_id
