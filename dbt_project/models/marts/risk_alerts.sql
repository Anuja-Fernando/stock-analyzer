with latest_portfolio as (

    select *
    from {{ ref('fct_portfolio_daily') }}
    where trade_date = (select max(trade_date) from {{ ref('fct_portfolio_daily') }})

),

latest_positions as (

    select
        p.trade_date,
        p.ticker,
        coalesce(c.sector, 'Unknown')                          as sector,
        p.market_value_inr / lp.portfolio_value_inr * 100      as weight_pct
    from {{ ref('fct_positions_daily') }} p
    join latest_portfolio lp on lp.trade_date = p.trade_date
    left join {{ ref('stg_company_profiles') }} c on c.ticker = p.ticker

),

sector_weights as (

    select trade_date, sector, sum(weight_pct) as weight_pct
    from latest_positions
    group by trade_date, sector

)

select
    trade_date as alert_date,
    'drawdown' as alert_type,
    'portfolio' as subject,
    drawdown_pct as metric_value,
    -{{ var('max_drawdown_pct') }} as threshold
from latest_portfolio
where drawdown_pct <= -{{ var('max_drawdown_pct') }}

union all

select trade_date, 'daily_move', 'portfolio', daily_return_pct, {{ var('max_daily_move_pct') }}
from latest_portfolio
where abs(daily_return_pct) >= {{ var('max_daily_move_pct') }}

union all

select trade_date, 'high_volatility', 'portfolio', volatility_30d_annualized_pct,
       {{ var('max_volatility_pct') }}
from latest_portfolio
where volatility_30d_annualized_pct >= {{ var('max_volatility_pct') }}

union all

select trade_date, 'sector_concentration', sector, weight_pct, {{ var('max_sector_weight_pct') }}
from sector_weights
where weight_pct >= {{ var('max_sector_weight_pct') }}

union all

select trade_date, 'position_concentration', ticker, weight_pct, {{ var('max_position_weight_pct') }}
from latest_positions
where weight_pct >= {{ var('max_position_weight_pct') }}