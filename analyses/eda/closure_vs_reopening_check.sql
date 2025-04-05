select * from `analytics-take-home-test`.`monzo_datawarehouse`.`account_transactions`
where account_id_hashed in (
  with a as (
    select
        account_id_hashed,
        min(closed_ts) as mi,
        max(closed_ts) as ma
    from `analytics-take-home-test`.`monzo_datawarehouse`.`account_closed`
    group by 1
    having count(*) > 1
  )
  
  select account_id_hashed from a
  where round(date_diff(ma,mi,day),0) != 0
)
