{{ 
    config(
        materialized='incremental',
        unique_key='account_date_id'
    )
}}

with source as (
    select * from {{ source('backend', 'account_transactions') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id_hashed']) }} as account_date_id,
        account_id_hashed as account_id,
        `date` as transaction_created_date,
        transactions_num as number_of_transactions

    from source

)

select * from renamed
