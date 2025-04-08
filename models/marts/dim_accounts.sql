with created as (
    select * from {{ ref('stg_backend__account_creation') }}
),

closed as (
    select * from {{ ref('int_backend__account_closed__deduplicated') }}
),

closed_agg as (
    select
        account_id,
        count(*) as number_of_closed_attempts,
        min(account_closed_at) as first_closed_at,
        max(account_closed_at) as last_closed_at

    from closed
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
    left join closed_agg
        on transactions.account_id = closed_agg.account_id
    -- accounts should not have transactions after being closed
    where transactions.transaction_created_date <= cast(closed_agg.last_closed_at as date)
    {{ dbt_utils.group_by(1) }}

),

reopened_closed_joined as (
    select
        reopened.account_id,
        reopened.account_reopened_at,
        closed.account_closed_at as last_closed_before_reopened_at

    from closed
    inner join reopened
        on closed.account_id = reopened.account_id
    where reopened.account_reopened_at >= closed.account_closed_at
    qualify row_number() over (
        partition by closed.account_id 
        order by date_diff(reopened.account_reopened_at, closed.account_closed_at, millisecond)
    ) = 1 
)

select
    created.account_id,
    created.user_id,
    created.account_type,
    created.account_created_at as created_at,
    closed_agg.number_of_closed_attempts,
    closed_agg.first_closed_at,
    closed_agg.last_closed_at,
    reopened_closed_joined.last_closed_before_reopened_at,
    reopened_closed_joined.account_reopened_at as reopened_at,
    transactions_agg.number_of_transactions,
    transactions_agg.first_transaction_date,
    transactions_agg.last_transaction_date,

    case
        when reopened_closed_joined.account_id is not null then 'active'
        when closed_agg.account_id is not null then 'closed'
        else 'active'
    end as account_status

from created
left join closed_agg
    on created.account_id = closed_agg.account_id
left join reopened_closed_joined
    on created.account_id = reopened_closed_joined.account_id
left join transactions_agg
    on created.account_id = transactions_agg.account_id
