{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2017-08-08' as date)",
    end_date="current_date"
   )
}}
