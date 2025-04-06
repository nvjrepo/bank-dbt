with source as (
    {# /* some entries got duplicated with similar values of all rows
          for all columns. We use `distinct` to deduplicate those cases.
     */ #}
    select distinct * from {{ source('backend', 'account_closed') }}

),

renamed as (

    select 
        {{ dbt_utils.generate_surrogate_key(['account_id_hashed', 'closed_ts']) }} as account_closed_id,
        account_id_hashed as account_id,
        closed_ts as account_closed_at

    from source

)

select * from renamed
