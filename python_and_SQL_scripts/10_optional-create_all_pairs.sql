create table all_pairs as
select distinct  pair, base_currency, quote_currency from ohlc_data;

create table all_pairs_date  as
select distinct date(open_time) as trade_date, pair, base_currency, quote_currency from ohlc_data;

create table all_open_times as
    select distinct open_time from ohlc_data;

