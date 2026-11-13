select
    rate_date,
    base_currency,
    quote_currency,
    rate
from {{ source('stocks_raw', 'fx_rates') }}
where rate > 0