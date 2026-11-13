-- Fails (returns a row) if the portfolio mart has no days at all, e.g. a bad ticker
-- in holdings.csv means no date has prices for every holding.
select 'fct_portfolio_daily is empty' as problem
from (select count(*) as n from {{ ref('fct_portfolio_daily') }}) t
where n = 0