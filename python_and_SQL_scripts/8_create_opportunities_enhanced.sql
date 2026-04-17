-- -- This SQL script performs the following steps to create an enhanced version of the triangle_opportunities table with additional volatility and liquidity metrics:
-- 1. It aggregates the raw OHLC data into hourly buckets, calculating open, high, low, close, and total volume in USD for each hour and currency pair.
-- 2. It computes rolling volatility and liquidity metrics for each currency pair on an hourly basis, including 24h, 168h (1 week), and 720h (1 month) rolling windows.
-- 3. It normalizes the pair delimiters in the triangle_opportunities table to ensure consistent formatting for the JOIN operation.
-- 4. It builds an enriched triangle_opportunities_enriched table by joining the original triangle_opportunities with the computed hourly metrics for each leg of the triangle, 
-- and calculates additional features such as maximum volatility across legs, average volatility, maximum range, minimum liquidity, minimum coverage, and maximum volume coefficient of variation.


-- ============================================================
-- STEP 1: Hourly OHLC aggregation
-- Source: ohlc_data → ohlc_1h
-- ============================================================

CREATE TABLE public.ohlc_1h AS
WITH b AS (
  SELECT
    pair,
    date_trunc('hour', open_time) AS bucket_time,
    open_time,
    open, high, low, close,
    volume_usd,
    row_number() OVER (PARTITION BY pair, date_trunc('hour', open_time)
                       ORDER BY open_time) AS rn_first,
    row_number() OVER (PARTITION BY pair, date_trunc('hour', open_time)
                       ORDER BY open_time DESC) AS rn_last
  FROM public.ohlc_data
  WHERE trade_date >= DATE '2017-01-01'
    AND trade_date <  DATE '2018-01-01'
)
SELECT
  pair,
  bucket_time,
  max(CASE WHEN rn_first = 1 THEN open END) AS open,
  max(high)                                  AS high,
  min(low)                                   AS low,
  max(CASE WHEN rn_last  = 1 THEN close END) AS close,
  sum(volume_usd)                            AS volume_usd_1h,
  count(*)                                   AS obs_count_1h
FROM b
GROUP BY pair, bucket_time;

-- Repeat INSERT for each subsequent year (2018–2022)
WITH b AS (
  SELECT
    pair,
    date_trunc('hour', open_time) AS bucket_time,
    open_time,
    open, high, low, close,
    volume_usd,
    row_number() OVER (PARTITION BY pair, date_trunc('hour', open_time)
                       ORDER BY open_time) AS rn_first,
    row_number() OVER (PARTITION BY pair, date_trunc('hour', open_time)
                       ORDER BY open_time DESC) AS rn_last
  FROM public.ohlc_data
  WHERE trade_date >= DATE '2018-01-01'
    AND trade_date <  DATE '2019-01-01'
)
INSERT INTO public.ohlc_1h
SELECT pair, bucket_time,
  max(CASE WHEN rn_first = 1 THEN open END),
  max(high), min(low),
  max(CASE WHEN rn_last  = 1 THEN close END),
  sum(volume_usd), count(*)
FROM b GROUP BY pair, bucket_time;

WITH b AS (
  SELECT pair, date_trunc('hour', open_time) AS bucket_time, open_time,
    open, high, low, close, volume_usd,
    row_number() OVER (PARTITION BY pair, date_trunc('hour', open_time) ORDER BY open_time) AS rn_first,
    row_number() OVER (PARTITION BY pair, date_trunc('hour', open_time) ORDER BY open_time DESC) AS rn_last
  FROM public.ohlc_data
  WHERE trade_date >= DATE '2019-01-01' AND trade_date < DATE '2020-01-01'
)
INSERT INTO public.ohlc_1h
SELECT pair, bucket_time,
  max(CASE WHEN rn_first=1 THEN open END), max(high), min(low),
  max(CASE WHEN rn_last=1 THEN close END), sum(volume_usd), count(*)
FROM b GROUP BY pair, bucket_time;

WITH b AS (
  SELECT pair, date_trunc('hour', open_time) AS bucket_time, open_time,
    open, high, low, close, volume_usd,
    row_number() OVER (PARTITION BY pair, date_trunc('hour', open_time) ORDER BY open_time) AS rn_first,
    row_number() OVER (PARTITION BY pair, date_trunc('hour', open_time) ORDER BY open_time DESC) AS rn_last
  FROM public.ohlc_data
  WHERE trade_date >= DATE '2020-01-01' AND trade_date < DATE '2021-01-01'
)
INSERT INTO public.ohlc_1h
SELECT pair, bucket_time,
  max(CASE WHEN rn_first=1 THEN open END), max(high), min(low),
  max(CASE WHEN rn_last=1 THEN close END), sum(volume_usd), count(*)
FROM b GROUP BY pair, bucket_time;

WITH b AS (
  SELECT pair, date_trunc('hour', open_time) AS bucket_time, open_time,
    open, high, low, close, volume_usd,
    row_number() OVER (PARTITION BY pair, date_trunc('hour', open_time) ORDER BY open_time) AS rn_first,
    row_number() OVER (PARTITION BY pair, date_trunc('hour', open_time) ORDER BY open_time DESC) AS rn_last
  FROM public.ohlc_data
  WHERE trade_date >= DATE '2021-01-01' AND trade_date < DATE '2022-01-01'
)
INSERT INTO public.ohlc_1h
SELECT pair, bucket_time,
  max(CASE WHEN rn_first=1 THEN open END), max(high), min(low),
  max(CASE WHEN rn_last=1 THEN close END), sum(volume_usd), count(*)
FROM b GROUP BY pair, bucket_time;

WITH b AS (
  SELECT pair, date_trunc('hour', open_time) AS bucket_time, open_time,
    open, high, low, close, volume_usd,
    row_number() OVER (PARTITION BY pair, date_trunc('hour', open_time) ORDER BY open_time) AS rn_first,
    row_number() OVER (PARTITION BY pair, date_trunc('hour', open_time) ORDER BY open_time DESC) AS rn_last
  FROM public.ohlc_data
  WHERE trade_date >= DATE '2022-01-01' AND trade_date < DATE '2023-01-01'
)
INSERT INTO public.ohlc_1h
SELECT pair, bucket_time,
  max(CASE WHEN rn_first=1 THEN open END), max(high), min(low),
  max(CASE WHEN rn_last=1 THEN close END), sum(volume_usd), count(*)
FROM b GROUP BY pair, bucket_time;

CREATE INDEX IF NOT EXISTS idx_ohlc_1h_pair_time ON public.ohlc_1h(pair, bucket_time);
ANALYZE public.ohlc_1h;


-- ============================================================
-- STEP 2: Per-pair rolling volatility & liquidity metrics
-- Source: ohlc_1h → pair_vol_hourly
-- ============================================================

CREATE TABLE public.pair_vol_hourly AS
WITH base AS (
  SELECT
    pair,
    bucket_time,
    close::double precision    AS close_px,
    high::double precision     AS high_px,
    low::double precision      AS low_px,
    volume_usd_1h::double precision AS vol_usd_1h,
    obs_count_1h,
    lag(close::double precision) OVER (PARTITION BY pair ORDER BY bucket_time) AS prev_close,
    lag(bucket_time)             OVER (PARTITION BY pair ORDER BY bucket_time) AS prev_time
  FROM public.ohlc_1h
),
rets AS (
  SELECT
    *,
    CASE
      WHEN prev_time IS NOT NULL
       AND bucket_time - prev_time = interval '1 hour'
      THEN (close_px / NULLIF(prev_close, 0) - 1.0)
    END AS ret_1h,
    ((high_px - low_px) / NULLIF(close_px, 0)) AS hl_pct_1h
  FROM base
),
w AS (
  SELECT
    *,
    count(ret_1h)    OVER (PARTITION BY pair ORDER BY bucket_time ROWS BETWEEN 23  PRECEDING AND CURRENT ROW) AS nret_24h,
    count(ret_1h)    OVER (PARTITION BY pair ORDER BY bucket_time ROWS BETWEEN 167 PRECEDING AND CURRENT ROW) AS nret_168h,
    count(ret_1h)    OVER (PARTITION BY pair ORDER BY bucket_time ROWS BETWEEN 719 PRECEDING AND CURRENT ROW) AS nret_720h,
    (bucket_time - lag(bucket_time, 23)  OVER (PARTITION BY pair ORDER BY bucket_time)) AS span_24h,
    (bucket_time - lag(bucket_time, 167) OVER (PARTITION BY pair ORDER BY bucket_time)) AS span_168h,
    (bucket_time - lag(bucket_time, 719) OVER (PARTITION BY pair ORDER BY bucket_time)) AS span_720h,
    stddev_samp(ret_1h) OVER (PARTITION BY pair ORDER BY bucket_time ROWS BETWEEN 23  PRECEDING AND CURRENT ROW) AS rv_24h_raw,
    stddev_samp(ret_1h) OVER (PARTITION BY pair ORDER BY bucket_time ROWS BETWEEN 167 PRECEDING AND CURRENT ROW) AS rv_168h_raw,
    stddev_samp(ret_1h) OVER (PARTITION BY pair ORDER BY bucket_time ROWS BETWEEN 719 PRECEDING AND CURRENT ROW) AS rv_720h_raw,
    avg(hl_pct_1h)   OVER (PARTITION BY pair ORDER BY bucket_time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS range_24h_raw,
    sum(vol_usd_1h)  OVER (PARTITION BY pair ORDER BY bucket_time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS sum_vol_usd_24h,
    avg(vol_usd_1h)  OVER (PARTITION BY pair ORDER BY bucket_time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS avg_vol_usd_24h,
    stddev_samp(vol_usd_1h) OVER (PARTITION BY pair ORDER BY bucket_time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS std_vol_usd_24h,
    avg(obs_count_1h) OVER (PARTITION BY pair ORDER BY bucket_time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) / 60.0 AS coverage_24h
  FROM rets
)
SELECT
  pair,
  bucket_time,
  close_px AS price,
  CASE WHEN nret_24h  >= 18  AND span_24h  <= interval '26 hours'  THEN rv_24h_raw  END AS rv_24h,
  CASE WHEN nret_168h >= 120 AND span_168h <= interval '176 hours' THEN rv_168h_raw END AS rv_168h,
  CASE WHEN nret_720h >= 500 AND span_720h <= interval '760 hours' THEN rv_720h_raw END AS rv_720h,
  CASE WHEN nret_24h  >= 18  AND span_24h  <= interval '26 hours'  THEN range_24h_raw END AS range_24h,
  sum_vol_usd_24h,
  CASE WHEN avg_vol_usd_24h > 0 THEN std_vol_usd_24h / avg_vol_usd_24h END AS vol_usd_cv_24h,
  coverage_24h
FROM w;

CREATE INDEX IF NOT EXISTS idx_pair_vol_hourly_pair_time
  ON public.pair_vol_hourly(pair, bucket_time);
ANALYZE public.pair_vol_hourly;


-- ============================================================
-- STEP 3: Normalize pair delimiters in triangle_opportunities
-- (prerequisite for the JOIN in Step 4)
-- ============================================================

UPDATE public.triangle_opportunities
SET pair_ab = replace(pair_ab, '/', '-'),
    pair_bc = replace(pair_bc, '/', '-'),
    pair_ca = replace(pair_ca, '/', '-');


-- ============================================================
-- STEP 4: Build enriched triangle table
-- Source: triangle_opportunities × pair_vol_hourly (×3 legs)
--         → triangle_opportunities_enriched
-- ============================================================

CREATE TABLE public.triangle_opportunities_enriched AS
WITH t AS (
  SELECT
    *,
    date_trunc('hour', "timestamp") AS hour_time
  FROM public.triangle_opportunities
),
j AS (
  SELECT
    t.*,
    vab.rv_24h        AS rv24_ab,   vab.range_24h         AS range24_ab,  vab.sum_vol_usd_24h AS liq24_ab,  vab.coverage_24h AS cov24_ab,
    vbc.rv_24h        AS rv24_bc,   vbc.range_24h         AS range24_bc,  vbc.sum_vol_usd_24h AS liq24_bc,  vbc.coverage_24h AS cov24_bc,
    vca.rv_24h        AS rv24_ca,   vca.range_24h         AS range24_ca,  vca.sum_vol_usd_24h AS liq24_ca,  vca.coverage_24h AS cov24_ca,
    vab.rv_168h       AS rv168_ab,  vbc.rv_168h           AS rv168_bc,    vca.rv_168h         AS rv168_ca,
    vab.rv_720h       AS rv720_ab,  vbc.rv_720h           AS rv720_bc,    vca.rv_720h         AS rv720_ca,
    vab.vol_usd_cv_24h AS volcv_ab, vbc.vol_usd_cv_24h   AS volcv_bc,    vca.vol_usd_cv_24h  AS volcv_ca
  FROM t
  LEFT JOIN public.pair_vol_hourly vab ON vab.pair = t.pair_ab AND vab.bucket_time = t.hour_time
  LEFT JOIN public.pair_vol_hourly vbc ON vbc.pair = t.pair_bc AND vbc.bucket_time = t.hour_time
  LEFT JOIN public.pair_vol_hourly vca ON vca.pair = t.pair_ca AND vca.bucket_time = t.hour_time
)
SELECT
  j.*,
  GREATEST(rv24_ab,   rv24_bc,   rv24_ca)   AS tri_rv24h_max,
  (rv24_ab + rv24_bc + rv24_ca) / 3.0       AS tri_rv24h_mean,
  GREATEST(rv168_ab,  rv168_bc,  rv168_ca)  AS tri_rv168h_max,
  GREATEST(rv720_ab,  rv720_bc,  rv720_ca)  AS tri_rv720h_max,
  GREATEST(range24_ab, range24_bc, range24_ca) AS tri_range24h_max,
  LEAST(liq24_ab,    liq24_bc,   liq24_ca)  AS tri_liq24h_min,
  LEAST(cov24_ab,    cov24_bc,   cov24_ca)  AS tri_cov24h_min,
  GREATEST(volcv_ab, volcv_bc,   volcv_ca)  AS tri_volcv24h_max
FROM j;

CREATE INDEX IF NOT EXISTS idx_tri_enriched_time
  ON public.triangle_opportunities_enriched(hour_time);
CREATE INDEX IF NOT EXISTS idx_tri_enriched_triangle_key_time
  ON public.triangle_opportunities_enriched(triangle_key, hour_time);
ANALYZE public.triangle_opportunities_enriched;
