select
    ticker,
    trade_date,
    close_price,
    volume,
    dividends,
    stock_splits
from {{ source('stocks_raw', 'stock_prices') }}
where close_price > 0