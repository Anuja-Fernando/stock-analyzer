with positions as (

    select * from {{ ref('fct_positions_daily') }}

),

expected as (

    select count(*) as n_holdings from {{ ref('holdings') }}

),

daily as (

    select
        trade_date,
        sum(market_value_inr)   as portfolio_value_inr,
        count(market_value_inr) as n_positions
    from positions
    group by trade_date

),

complete_days as (

    select d.trade_date, d.portfolio_value_inr
    from daily d
    cross join expected e
    where d.n_positions = e.n_holdings

),

with_returns as (

    select
        trade_date,
        portfolio_value_inr,
        (portfolio_value_inr
            / lag(portfolio_value_inr) over (order by trade_date) - 1) * 100
            as daily_return_pct,
        max(portfolio_value_inr) over (
            order by trade_date rows between unbounded preceding and current row
        ) as running_peak_inr
    from complete_days

)

select
    trade_date,
    portfolio_value_inr,
    round(daily_return_pct, 4)                                   as daily_return_pct,
    round((portfolio_value_inr / running_peak_inr - 1) * 100, 4) as drawdown_pct,
    round(
        stddev_samp(daily_return_pct) over (
            order by trade_date rows between 29 preceding and current row
        ) * sqrt(252), 4
    ) as volatility_30d_annualized_pct
from with_returns