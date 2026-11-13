with prices as (

    select * from {{ ref('stg_stock_prices') }}

),

h_clean as (

    select
        upper(ticker)   as ticker,
        shares,
        avg_cost,
        upper(currency) as currency
    from {{ ref('holdings') }}

),

usd_inr as (

    select rate_date, rate
    from {{ ref('stg_fx_rates') }}
    where base_currency = 'USD' and quote_currency = 'INR'

),

joined as (

    select
        p.trade_date,
        p.ticker,
        h.currency,
        h.shares,
        h.avg_cost,
        p.close_price,
        case
            when h.currency = 'INR' then 1.0
            when h.currency = 'USD' then (
                select f.rate
                from usd_inr f
                where f.rate_date <= p.trade_date
                order by f.rate_date desc
                limit 1
            )
        end as fx_rate_to_inr,
        lag(p.close_price) over (
            partition by p.ticker order by p.trade_date
        ) as prev_close
    from prices p
    join h_clean h on h.ticker = p.ticker

)

select
    trade_date,
    ticker,
    currency,
    shares,
    close_price,
    fx_rate_to_inr,
    round(shares * close_price * fx_rate_to_inr, 2)           as market_value_inr,
    round((close_price / avg_cost - 1) * 100, 4)              as unrealized_pnl_pct,
    round((close_price / nullif(prev_close, 0) - 1) * 100, 4) as price_return_pct
from joined