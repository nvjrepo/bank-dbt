with base as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2017-08-08' as date)",
        end_date="current_date"
       )
    }}

)

select cast(date_day as date) as date_day from base
