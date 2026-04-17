-- we want to standardize all volumes to USD to make it easier to filter and analyze data across different currency pairs.
-- To do this, we will compute the FX rate to USD for each currency and date, and then multiply the volume by the FX rate to get the volume in USD. 

---- create table for daily first timestamps to use for FX rate extraction
CREATE TABLE daily_first_timestamps AS
WITH bounds AS (
  SELECT
    date_trunc('day', min(open_time))::date AS dmin,
    date_trunc('day', max(open_time))::date AS dmax
  FROM public.ohlc_data
),
days AS (
  SELECT generate_series(dmin, dmax, interval '1 day')::date AS trade_date
  FROM bounds
)
SELECT
  trade_date,
  (
    SELECT min(o.open_time)
    FROM public.ohlc_data o
    WHERE o.open_time >= trade_date
      AND o.open_time <  trade_date + interval '1 day'
  ) AS first_open_time
FROM days;



create table fx_rates as
WITH RECURSIVE
-- Get all available rates at each date's first timestamp
daily_rates AS (
    SELECT
        dft.trade_date,
        o.base_currency,
        o.quote_currency,
        o.close as rate,
        o.pair
    FROM daily_first_timestamps dft
    JOIN public.ohlc_data o
        ON o.open_time = dft.first_open_time
    WHERE o.close > 0  -- Ensure valid rates
),

-- Recursive CTE to find conversion paths to USDT
currency_paths AS (
    -- Base case: Direct USDT pairs
    SELECT
        trade_date,
        CAST(base_currency as TEXT) as currency,
        'USDT' as target_currency,
        rate,
        1 as path_length,
        CAST(pair as TEXT) as conversion_path,
        CAST(base_currency as TEXT) || '->' || CAST(quote_currency as TEXT) as path_description
    FROM daily_rates
    WHERE quote_currency = 'USDT'

    UNION ALL

    -- Also include reverse of USDT pairs (e.g., USDT/BTC means BTC rate is 1/rate)
    SELECT
        trade_date,
        quote_currency as currency,
        'USDT' as target_currency,
        1.0 / rate as rate,
        1 as path_length,
        CAST(pair as TEXT) as conversion_path,
        CAST(quote_currency || '->' || base_currency || '(inv)' as TEXT) as path_description
    FROM daily_rates
    WHERE base_currency = 'USDT' AND rate > 0

    UNION ALL

    -- Recursive case: Multi-hop conversions
    SELECT
        dr.trade_date,
        dr.base_currency as currency,
        cp.target_currency,
        dr.rate * cp.rate as rate,
        cp.path_length + 1 as path_length,
        dr.pair || '->' || cp.conversion_path as conversion_path,
        dr.base_currency || '->' || dr.quote_currency || '->' ||
            SUBSTRING(cp.path_description FROM POSITION('->' IN cp.path_description) + 2) as path_description
    FROM daily_rates dr
    JOIN currency_paths cp
        ON dr.quote_currency = cp.currency
        AND dr.trade_date = cp.trade_date
    WHERE cp.path_length < 3  -- Limit to 3 hops to avoid infinite loops
        AND dr.base_currency != 'USDT'  -- Don't create paths from USDT
),
-- Get the shortest (most direct) path for each currency to USDT per date
best_paths AS (
    SELECT DISTINCT ON (trade_date, currency)
        trade_date,
        currency,
        rate as usdt_rate,
        conversion_path,
        path_description,
        path_length
    FROM currency_paths
    WHERE target_currency = 'USDT'
    ORDER BY trade_date, currency, path_length ASC, rate DESC
)
select * from best_paths;

---- fx rates table created
-------=========================================================

--- create index on fx
create index idx_fx_rates_currency_trade_date
    on public.fx_rates (currency, trade_date);

------------------------------------------------------------
--- get missing fx
create temporary table fx_rates_missing as
with all_currency_dates as (select trade_date, base_currency as currency
                            from all_pairs_date
                            union
                            select trade_date, base_currency as currency
                            from all_pairs_date)
,missing_fx as (
    select acd.* from all_currency_dates acd
    left join fx_rates fx on acd.currency =fx.currency and acd.trade_date = fx.trade_date
    where fx.trade_date is null
    )
,results as (select *,
                    (select usdt_rate
                     from fx_rates fx
                     where fx.currency = mfx.currency
                       and fx.trade_date 
                           (select min(trade_date)
                            from fx_rates fx2
                            where fx2.currency = fx.currency
                              and fx2.trade_date >= mfx.trade_date)) as fill_rate_next,
                    (select usdt_rate
                     from fx_rates fx
                     where fx.currency = mfx.currency
                       and fx.trade_date = 
                           (select max(trade_date)
                            from fx_rates fx2
                            where fx2.currency = fx.currency
                              and fx2.trade_date < mfx.trade_date))  as fill_rate_prev
             from missing_fx mfx)
select trade_date,currency, coalesce(fill_rate_prev, fill_rate_next) as fx_rate,
       case when fill_rate_prev is not null then 'fill forward' else 'fill_back' end as conversion_path, null as path_description, 0 as path_length
       from results
       where currency != 'USDT'
union all
select trade_date, currency , 1 as fx_rate, 'self' as conversion_path, null as path_description, 0 as path_length
        from missing_fx where currency = 'USDT'
;


--- update fx_rates table with missing rates filled in
insert into fx_rates
select * from fx_rates_missing;
-----------------------------------------------------------------------

---- update USD volumes in batches to avoid long running transactions
UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.trade_date >= DATE '2017-01-01'
  AND od.trade_date <  DATE '2018-01-01';

UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.trade_date >= DATE '2018-01-01'
  AND od.trade_date <  DATE '2019-01-01';

UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.trade_date >= DATE '2019-01-01'
  AND od.trade_date <  DATE '2020-01-01';

UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.open_time >= DATE '2020-01-01'
  AND od.open_time <  DATE '2020-02-01';


UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.open_time >= DATE '2020-02-01'
  AND od.open_time <  DATE '2020-03-01';

UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.open_time >= DATE '2020-03-01'
  AND od.open_time <  DATE '2020-04-01';

UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.open_time >= DATE '2020-04-01'
  AND od.open_time <  DATE '2020-05-01';

UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.open_time >= DATE '2020-05-01'
  AND od.open_time <  DATE '2020-06-01';

UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.open_time >= DATE '2020-06-01'
  AND od.open_time <  DATE '2020-07-01';

UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.open_time >= DATE '2020-07-01'
  AND od.open_time <  DATE '2020-08-01';

UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.open_time >= DATE '2020-08-01'
  AND od.open_time <  DATE '2020-09-01';

UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.open_time >= DATE '2020-09-01'
  AND od.open_time <  DATE '2020-10-01';

UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.open_time >= DATE '2020-10-01'
  AND od.open_time <  DATE '2020-11-01';

UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.open_time >= DATE '2020-11-01'
  AND od.open_time <  DATE '2020-12-01';

UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.open_time >= DATE '2020-12-01'
  AND od.open_time <  DATE '2021-01-01';


UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.open_time >= DATE '2021-01-01'
  AND od.open_time <  DATE '2021-03-01';


UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.open_time >= DATE '2021-03-01'
  AND od.open_time <  DATE '2021-06-01';


UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.open_time >= DATE '2021-06-01'
  AND od.open_time <  DATE '2021-09-01';

UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.open_time >= DATE '2021-09-01'
  AND od.open_time <  DATE '2022-01-01';

UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.open_time >= DATE '2022-01-01'
  AND od.open_time <  DATE '2022-06-01';

UPDATE public.ohlc_data AS od
SET volume_usd = od.volume * fx.usdt_rate
FROM public.fx_rates AS fx
WHERE fx.currency = od.base_currency
  AND fx.trade_date = od.trade_date
  AND od.volume_usd IS NULL
  AND od.open_time >= DATE '2022-06-01'
  AND od.open_time <  DATE '2023-01-01';
-----------------------------------------------------------------------
