-- account_closed
select
    account_id_hashed,
    count(*)
from analytics-take-home-test.monzo_datawarehouse.account_closed
group by 1
having count(*) > 1;


--- sample 1 id of duplication
select * from analytics-take-home-test.monzo_datawarehouse.account_closed a
where account_id_hashed = 'hrwoeW4c1jDKy9aCshm3tPBDJQTHXCcn6OAyDsO/iAOaIvRFsLSxY9SJSHh51yPoT3CDdFJLrbct+u5HKVcEDg=='

-- account_reopened
select
    account_id_hashed,
    count(*)
from analytics-take-home-test.monzo_datawarehouse.account_reopened
group by 1
having count(*) > 1;

-- account_created
select
    account_id_hashed,
    count(*)
from analytics-take-home-test.monzo_datawarehouse.account_created
group by 1
having count(*) > 1

-- account_transactions
select
    account_id_hashed || `date`,
    count(*)
from analytics-take-home-test.monzo_datawarehouse.account_transactions
group by 1
having count(*) > 1