select
    'account_created' as table_source,
    count(*) as total_rows
from analytics-take-home-test.monzo_datawarehouse.account_created
union all
select
    'account_closed' as table_source,
    count(*) as total_rows
from analytics-take-home-test.monzo_datawarehouse.account_closed
union all
select
    'account_reopened' as table_source,
    count(*) as total_rows
from analytics-take-home-test.monzo_datawarehouse.account_reopened
union all
select
    'account_transactions' as table_source,
    count(*) as total_rows
from analytics-take-home-test.monzo_datawarehouse.account_transactions
