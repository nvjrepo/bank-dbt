with accounts as (
    select * from {{ ref('dim_accounts') }}
),

users as (
    select
        user_id,
        min(created_at) as first_account_created_at
    
    from accounts
    {{ dbt_utils.group_by(1) }}

)

select * from final
