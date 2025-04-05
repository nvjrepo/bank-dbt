-- account_reopened
select * from analytics-take-home-test.monzo_datawarehouse.account_reopened a
left join analytics-take-home-test.monzo_datawarehouse.account_created b
    on a.account_id_hashed = b.account_id_hashed
where b.account_id_hashed is null

-- account_closed
select * from analytics-take-home-test.monzo_datawarehouse.account_closed a
left join analytics-take-home-test.monzo_datawarehouse.account_created b
    on a.account_id_hashed = b.account_id_hashed
where b.account_id_hashed is null

-- account_transactions
select * from analytics-take-home-test.monzo_datawarehouse.account_transactions a
left join analytics-take-home-test.monzo_datawarehouse.account_created b
    on a.account_id_hashed = b.account_id_hashed
where b.account_id_hashed is null