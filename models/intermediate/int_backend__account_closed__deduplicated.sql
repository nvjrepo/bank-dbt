{{ 
    config(
        materialized='table'
    )
}}

with source as (
    select
        *,
        lag(account_closed_at) over (partition by account_id order by account_closed_at) as next_closed_at

    from {{ ref('stg_backend__account_closure') }}

)

select *
from source
where
    date_diff(next_closed_at, account_closed_at, day) is null
    or date_diff(next_closed_at, account_closed_at, day) != 0
