{{ 
    config(
        materialized='incremental',
        unique_key='account_id'
    )
}}

with source as (

    select * from {{ source('backend', 'account_created') }}

),

renamed as (

    select
        account_id_hashed as account_id,
        user_id_hashed as user_id,
        created_ts as account_created_at,
        account_type

    from source

)

select * from renamed
