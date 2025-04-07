with source as (
    select * from {{ source('backend', 'account_reopened') }}

),

renamed as (

    select 
        account_id_hashed as account_id,
        reopened_ts as account_reopened_at

    from source

)

select * from renamed
