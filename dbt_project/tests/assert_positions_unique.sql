select ticker, trade_date, count(*) as n
from {{ ref('fct_positions_daily') }}
group by ticker, trade_date
having count(*) > 1