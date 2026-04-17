-- We will add two new columns to the ohlc_data table:
-- 1. volume_usd: This will store the total trading volume in USD for each record. 
-- We will calculate this by multiplying the volume column (which is in terms of the base currency) by
-- the close price (which is in terms of the quote currency). This will allow us to easily filter 
-- and analyze records based on trading volume in USD, 
-- which standardizes the volume across different currency pairs and makes it easier to compare and aggregate data.


ALTER TABLE public.ohlc_data
ADD COLUMN volume_usd numeric(30,10);

-- 2. trade_date: This will be a generated column that extracts the date from the open_time
-- timestamp. This will allow us to easily group and filter records by date without having to perform a function call on the open_time column each time, 
-- which can improve query performance when analyzing data on a daily basis.

ALTER TABLE public.ohlc_data
ADD COLUMN trade_date date
GENERATED ALWAYS AS (open_time::date) STORED;
