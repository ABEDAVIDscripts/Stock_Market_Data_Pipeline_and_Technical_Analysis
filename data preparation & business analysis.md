## Data Preparation and Business Analysis

<br>
<br>

### 1:  Create a Table 
- Create temporary staging table (matches CSV structure)
```sql
CREATE TABLE stock_data_temp (
    date DATE,
    close DECIMAL(10, 2),
    sma_7 DECIMAL(10, 2),
    sma_14 DECIMAL(10, 2),
    sma_30 DECIMAL(10, 2),
    rsi DECIMAL(10, 2),
    rsi_signal VARCHAR(20),
    macd DECIMAL(10, 4),
    macd_signal_line DECIMAL(10, 4),
    histogram DECIMAL(10, 4),
    macd_crossover VARCHAR(10),
    recommendation VARCHAR(20)
);
```

- Create FINAL table with (Symbol COLUMN)
```bash
CREATE TABLE Final_stock_data (
    symbol      VARCHAR(10),
    date        DATE,
    close       DECIMAL(10, 2),
    sma_7       DECIMAL(10, 2),
    sma_14      DECIMAL(10, 2),
    sma_30      DECIMAL(10, 2),
    rsi         DECIMAL(10, 2),
    rsi_signal  VARCHAR(20),
    macd        DECIMAL(10, 4),
    macd_signal_line    DECIMAL(10, 2),
    histogram   DECIMAL(10, 4),
    macd_crossover      VARCHAR(10),
    recommendation      VARCHAR(10)
);
```

<br>
<br>
<br>

### 2. Load Stock Data

#### 2:1. AAPL STOCK
- Create a Manifest File <br>
`aws s3 ls s3://fp-transformed-bucket/AAPL/ | grep "\.csv"`

<BR>

- In EC2 terminal, List AAPL CSV files
```bash
(fp_env) ubuntu@ip-172-31-3-244:~$ aws s3 ls s3://fp-transformed-bucket/AAPL/ | grep "\.csv"
```

- Create AAPL Manifest
``` bash
cat > aapl_manifest.json << 'EOF'
{
  "entries": [
    {"url":"s3://fp-transformed-bucket/AAPL/consolidated_AAPL_20251114_164346.csv", "mandatory":true},
    {"url":"s3://fp-transformed-bucket/AAPL/consolidated_AAPL_20251115_160456.csv", "mandatory":true},
    {"url":"s3://fp-transformed-bucket/AAPL/consolidated_AAPL_20251115_161830.csv", "mandatory":true},
    {"url":"s3://fp-transformed-bucket/AAPL/consolidated_AAPL_20251115_164738.csv", "mandatory":true},
    {"url":"s3://fp-transformed-bucket/AAPL/consolidated_AAPL_20251117_002653.csv", "mandatory":true}
  ]
}
EOF
```

<BR>

- Upload the Manifest to S3
`aws s3 cp manifest.json s3://fp-transformed-bucket/manifests/aapl_manifest.json`

<br>

- Load data using manifest
```SQL
COPY stock_data_temp 
FROM 's3://fp-transformed-bucket/manifests/aapl_manifest.json'
IAM_ROLE 'arn:aws:iam::093827727213:role/service-role/AmazonRedshift-CommandsAccessRole-20251115T111001'
FORMAT AS CSV
IGNOREHEADER 1
REGION 'eu-north-1'
MANIFEST;
```

<br>

- Verify the Loaded Data
```sql
SELECT COUNT(*) FROM stock_data_temp;
```

<BR>

- Move AAPL Data to Final Table
```sql
INSERT INTO Final_stock_data 
SELECT 
    'APPL' AS symbol,
    date,
    close,
    sma_7,
    sma_14,
    sma_30,
    rsi,
    rsi_signal,
    macd,
    macd_signal_line,
    histogram,
    macd_crossover,
    recommendation
FROM stock_data_temp;
```

<BR>

- Verify final table 
```sql
SELECT COUNT(*) 
FROM Final_stock_data 
WHERE symbol = 'AAPL';
```

- Clear temp table for next stock
```sql
TRUNCATE TABLE stock_data_temp;
```

<BR>
<BR>

#### 2.2: AMZN STOCK
- Create a Manifest File <br>
`aws s3 ls s3://fp-transformed-bucket/AMZN/ | grep "\.csv"`

<BR>

- In EC2 terminal, List AMZN CSV files
```bash
(fp_env) ubuntu@ip-172-31-3-244:~$ aws s3 ls s3://fp-transformed-bucket/AMZN/ | grep "\.csv"
```

<BR>

- Create AMZN Manifest
```bash
cat > amzn_manifest.json << 'EOF'
{
  "entries": [
    {"url":"s3://fp-transformed-bucket/AMZN/consolidated_AMZN_20251114_164347.csv", "mandatory":true},
    {"url":"s3://fp-transformed-bucket/AMZN/consolidated_AMZN_20251115_160456.csv", "mandatory":true},
    {"url":"s3://fp-transformed-bucket/AMZN/consolidated_AMZN_20251115_161831.csv", "mandatory":true},
    {"url":"s3://fp-transformed-bucket/AMZN/consolidated_AMZN_20251115_164739.csv", "mandatory":true},
    {"url":"s3://fp-transformed-bucket/AMZN/consolidated_AMZN_20251117_002653.csv", "mandatory":true}
  ]
}
EOF
```

<br>

- Upload to S3
`aws s3 cp amzn_manifest.json s3://fp-transformed-bucket/manifests/amzn_manifest.json`

<br>

- Load AMZN data
```sql
COPY stock_data_temp 
FROM 's3://fp-transformed-bucket/manifests/amzn_manifest.json'
IAM_ROLE 'arn:aws:iam::093827727213:role/service-role/AmazonRedshift-CommandsAccessRole-20251115T111001'
FORMAT AS CSV
IGNOREHEADER 1
REGION 'eu-north-1'
MANIFEST;
```

<br>

- VERIFY
```SQL
SELECT COUNT(*)
FROM stock_data_temp;
```

<br> 

- Move to final table
```sql
INSERT INTO Final_stock_data 
SELECT 
    'AMZN' AS symbol,
    date, close, sma_7, sma_14, sma_30, rsi, rsi_signal,
    macd, macd_signal_line, histogram, macd_crossover, recommendation
FROM stock_data_temp;
```

<br>

- Verify
```sql
SELECT COUNT(*)
FROM final_stock_data
WHERE symbol='AMZN';
```

<br>

- Clear temp table for next stock
```sql
TRUNCATE TABLE stock_data_temp;
```

<br>
<br>

#### 2.3: GOOGL STOCK
- Create a Manifest File <br>
`aws s3 ls s3://fp-transformed-bucket/GOOGL/ | grep "\.csv"`

<BR>

- List GOOGL CSV files
```bash
(fp_env) ubuntu@ip-172-31-3-244:~$ aws s3 ls s3://fp-transformed-bucket/GOOGL/ | grep "\.csv"
```

<BR>

- Create GOOGL manifest
```bash
cat > googl_manifest.json << 'EOF'
{
  "entries": [
    {"url":"s3://fp-transformed-bucket/GOOGL/consolidated_GOOGL_20251114_164347.csv", "mandatory":true},
    {"url":"s3://fp-transformed-bucket/GOOGL/consolidated_GOOGL_20251115_160456.csv", "mandatory":true},
    {"url":"s3://fp-transformed-bucket/GOOGL/consolidated_GOOGL_20251115_161830.csv", "mandatory":true},
    {"url":"s3://fp-transformed-bucket/GOOGL/consolidated_GOOGL_20251115_164739.csv", "mandatory":true},
    {"url":"s3://fp-transformed-bucket/GOOGL/consolidated_GOOGL_20251117_002653.csv", "mandatory":true}
  ]
}
EOF
```

<br>

- Upload to S3
`aws s3 cp googl_manifest.json s3://fp-transformed-bucket/manifests/googl_manifest.json`

<br>

- Load GGOGL data
```sql
COPY stock_data_temp 
FROM 's3://fp-transformed-bucket/manifests/googl_manifest.json'
IAM_ROLE 'arn:aws:iam::093827727213:role/service-role/AmazonRedshift-CommandsAccessRole-20251115T111001'
FORMAT AS CSV
IGNOREHEADER 1
REGION 'eu-north-1'
MANIFEST;
```

<br>

- Move to final table
```sql
INSERT INTO Final_stock_data 
SELECT 
    'AMZN' AS symbol,
    date, close, sma_7, sma_14, sma_30, rsi, rsi_signal,
    macd, macd_signal_line, histogram, macd_crossover, recommendation
FROM stock_data_temp;
```

<br>

- Clear temp table for next stock
```sql
TRUNCATE TABLE stock_data_temp;
```

<br>
<br>

#### 2.4: Same process for `MSFT` and `TSLA`

<br>
<br>


### 3. Data Preparation

1.  Check data record
```sql
SELECT
COUNT(DISTINCT symbol) AS no_of_symbols,
COUNT(*) AS no_of_records,
MAX(date) AS newest_date,
MIN(date) AS oldest_date,
MAX(date) - MIN(date) AS date_range_in_days
FROM final_stock_data;
```



- Record per Symbol
```sql
SELECT
symbol, 
COUNT(*) AS no_of_records,
MAX(date) AS newest_date,
MIN(date) AS oldest_date,
MAX(date) - MIN(date) AS date_range_in_days
FROM final_stock_data
GROUP BY symbol
ORDER BY 1 ;
```

<br>
<br>

2. Check for Duplicate
```sql
SELECT
    COUNT(*) AS duplicates, 
    symbol, date
FROM final_stock_data
GROUP BY symbol, date
HAVING COUNT(*) > 1;
```



- Text error in APPL, UPDATE to AAPL
```SQL
UPDATE stock_data_cleaned
SET symbol = 'AAPL'
WHERE symbol = 'APPL';
```



- Duplicate found! Remove Duplicates using a BACKUP TABLE
```sql
CREATE TABLE stock_data_cleaned AS
SELECT *
FROM (
        SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY macd DESC) as rn
        FROM final_stock_data
    )
WHERE rn = 1;
```



- verify
```sql
SELECT
    count(*) AS duplicates,
    symbol, date
FROM stocK_data_cleaned 
GROUP BY symbol, date
HAVING count(*) > 1;
```

- Count remaining data
```sql
SELECT
    count(*) AS datafile
FROM stocK_data_cleaned ;
```

<br>
<br>

- 3. Check for NULL Value 
```sql
SELECT * 
FROM stock_data_cleaned
WHERE 
    date is NULL OR
    close IS NULL OR
    sma_7 IS NULL OR
    sma_14 IS NULL OR
    sma_30 IS NULL OR
    rsi IS NULL OR 
    rsi_signal IS NULL OR
    macd IS NULL OR
    macd_signal_line IS NULL OR
    histogram IS NULL OR
    macd_crossover IS NULL OR 
    recommendation IS NULL
;
```

<br>
<br>

- 4. DATA VALIDITY CHECKS
``` sql
SELECT * FROM stock_data_cleaned;
```



- Check for Negative Prices
```sql
SELECT
    symbol, date, close, sma_7, sma_14, sma_30
FROM stock_data_cleaned 
WHERE 
    close < 0 OR
    sma_7 < 0  OR
    sma_14 < 0  OR
    sma_30 < 0 
ORDER BY symbol;    
```



- Check for Zero Prices
```sql
SELECT
    symbol, date, close, sma_7, sma_14, sma_30
FROM stock_data_cleaned 
WHERE 
    close = 0 OR
    sma_7 = 0  OR
    sma_14 = 0  OR
    sma_30 = 0 
ORDER BY symbol;
```



- Check RSI out of range = (-0 OR 100+)
```sql
SELECT
    symbol, date, rsi, rsi_signal
FROM stock_data_cleaned 
WHERE 
    rsi < 0 OR rsi > 100
ORDER BY symbol;
```



- Check for extreme price movements (>50% in one day = potential data error)
```sql
WITH pricetable AS
  (SELECT
    symbol, date, close,
    LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
    ROUND( ( (close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) /
    LAG(close) OVER (PARTITION BY symbol ORDER BY date) ) * 100, 2) AS close_per_change
  FROM stock_data_cleaned) 

 SELECT
    symbol, date, close, prev_close, close_per_change
  FROM pricetable
  WHERE ABS(close_per_change) > 50;
```



- Error found, Investigate
- See the AAPL price structure
```sql
SELECT
  date, close, 
  CASE 
    WHEN close > 400 THEN 'CORRUPTED'
    WHEN close BETWEEN 200 AND 300 THEN 'VALID'
    ELSE 'ERROR CHECK'
  END AS status
FROM stock_data_cleaned
WHERE symbol = 'AAPL'
ORDER BY date;
```



- AAPL price range
```sql
SELECT 
  symbol,
  MIN(close) as min_price,
  MAX(close) as max_price,
  AVG(close) as avg_price,
  STDDEV(close) as price_stddev
FROM stock_data_cleaned
GROUP BY symbol
HAVING symbol = 'AAPL';
```



- Check each symbol for close-price values that fall outside the valid range for 2025 (52-week period)
```sql
SELECT
  symbol,
  COUNT(*) AS total_records,
  ROUND(MIN(close), 2) AS min_price,
  ROUND(MAX(close), 2) AS max_price,
  ROUND(AVG(close), 2) AS avg_price,
  COUNT(*) AS total_record,
  -- AAPL valid range: $150-300
  COUNT(CASE WHEN symbol = 'AAPL' AND (close < 150 OR close > 300) THEN 1 END) AS Corrupt_AAPL,
  -- AMZN valid range: $180-300
  COUNT(CASE WHEN symbol = 'AMZN' AND (close < 180 OR close > 300) THEN 1 END) AS Corrupt_AMZN,
	-- GOOGL valid range: $140-328
	COUNT(CASE WHEN symbol = 'GOOGL' AND (close < 140 OR close > 330) THEN 1 END) AS Corrupt_GOOGL,
	-- MSFT valid range: $350-550
	COUNT(CASE WHEN symbol = 'MSFT' AND (close < 350 OR close > 550) THEN 1 END) AS Corrupt_MSFT,
	-- TSLA valid range: $250-550
	COUNT(CASE WHEN symbol = 'TSLA' AND (close < 250 OR close > 550) THEN 1 END) AS Corrupt_TSLA,	
FROM stock_data_cleaned
GROUP BY symbol
ORDER BY symbol;
```


- AAPL corrupt data = 7 and GOOGL corrupt data = 30
- Confirm the corrupt range for AAPL, to know the range to delete
```sql
SELECT symbol,
  COUNT(CASE WHEN close > 300 THEN 1 END) AS above_300,
  COUNT(CASE WHEN close < 150 THEN 1 END) AS below-150
FROM stock_data_cleaned
WHERE symbol = 'AAPL'
GROUP BY symbol;
```


- Confirm the corrupted range for GOOGL, to know what range to delete
```sql
SELECT symbol,
  COUNT(CASE WHEN close > 250 THEN 1 END) AS above_250,
  COUNT(CASE WHEN close < 140 THEN 1 END) AS below_140
FROM stock_data_cleaned
WHERE symbol = 'GOOGL'
GROUP BY symbol;
```


- Delete AAPL corrupt data above 300
```sql
DELETE FROM stock_data_cleaned
WHERE symbol = 'AAPL' AND close > 300;
```


- Verify Delete AAPL corrupt data
```sql
SELECT
  symbol, close
FROM stock_data_cleaned
WHERE 
  symbol = 'AAPL' AND
  close > 300;
```

<br>
<br>
<br>



## Business Question

- 1. which stock had the highest percentage gain/loss over the period?
```sql
-- (new value - old value)/ old value) * 100

WITH firstable AS
    (SELECT
        symbol, close, date,
        ROW_NUMBER () OVER (PARTITION BY symbol ORDER BY date ASC) AS row_acnd,
        ROW_NUMBER () OVER (PARTITION BY symbol ORDER BY date DESC) AS row_desc
    FROM stock_data_cleaned ),

sndtable AS 
    (SELECT 
        symbol,
        MAX(CASE WHEN row_acnd = 1 THEN close END) AS startprice,
        MAX(CASE WHEN row_desc = 1 THEN close END) AS endprice
    FROM firstable
    GROUP BY symbol)    

SELECT
    symbol, startprice, endprice,
    endprice - startprice AS price_change,
    ROUND((((endprice - startprice)/ startprice) * 100), 2) AS percent_change,
    CASE 
        WHEN endprice > startprice THEN 'Gain'
        ELSE 'Loss'
    END AS gain_or_loss
FROM sndtable
ORDER BY percent_change DESC;
```

<br>

2. How many buy vs sell signals were generated for each stock?
```sql
SELECT 
    symbol, 
    COUNT(macd_crossover) AS total_signal, 
    COUNT(CASE WHEN macd_crossover = 'BUY' THEN 1 END) AS buy_signal, 
    COUNT(CASE WHEN macd_crossover = 'SELL' THEN 1 END) AS sell_signal, 
    COUNT(CASE WHEN macd_crossover = 'HOLD' THEN 1 END) AS hold_signal 
FROM stock_data_cleaned 
GROUP BY symbol;
```

<br>

3. Which stocks frequently hit overbought (RSI > 70) or oversold (RSI < 30) conditions?
```sql
SELECT
    symbol,
    COUNT(*) AS total_condition,
    COUNT(CASE WHEN rsi > 70 THEN 1 END) AS overbought,
    COUNT(CASE WHEN rsi < 30 THEN 1 END) AS oversold,
    ROUND(((COUNT(CASE WHEN rsi > 70 THEN 1 END)) * 100.0) / COUNT(*), 1) AS percent_overbought,
    ROUND(((COUNT(CASE WHEN rsi < 30 THEN 1 END)) * 100.0) / COUNT(*), 1) AS percent_sold
FROM stock_data_cleaned
GROUP BY symbol
ORDER BY percent_overbought DESC, percent_sold DESC;
```

<br>


4. When did MACD crossovers occur and what was the subsequent price movement?
```sql
WITH crossovertable AS (
    SELECT 
        symbol,
        date,
        macd_crossover,
        close AS crossover_price
    FROM stock_data_cleaned
    WHERE macd_crossover IN ('BUY', 'SELL')
)
SELECT
    c.symbol,
    c.date AS crossover_date,
    c.macd_crossover,
    c.crossover_price,
    s.date AS next_date, 
    s.close AS next_price
FROM crossovertable c
JOIN stock_data_cleaned s
  ON c.symbol = s.symbol
 AND s.date = (
        SELECT MIN(date)
        FROM stock_data_cleaned
        WHERE symbol = c.symbol AND date > c.date
     )
ORDER BY c.symbol, crossover_date;
```

<br>


5. Is the stock in an uptrend, downtrend, or sideways movement?
```sql
SELECT
    symbol,
    date,
    close,
    sma_7,
    sma_14,
    sma_30,
    CASE
        WHEN sma_7 > sma_14 AND sma_14 > sma_30 THEN 'UPTREND'
        WHEN sma_7 < sma_14 AND sma_14 < sma_30 THEN 'DOWNTREND'
        ELSE 'SIDEWAYS'
    END AS trend_status
FROM stock_data_cleaned
ORDER BY symbol, date;
```

<br>

6. Which stock is most volatile?
```SQL
/* Volatility = Standard Deviation of Daily Returns
daily return = (current close- previous close)/previous close
*/

WITH returns AS (
    SELECT
        symbol,
        date,
        close,
        (close - LAG(close) OVER (PARTITION BY symbol ORDER BY date))
            / LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS daily_return
    FROM stock_data_cleaned
)
SELECT
    symbol,
    CAST(ROUND(STDDEV_SAMP(daily_return), 4) AS DECIMAL(4,10)) AS volatility
FROM returns
WHERE daily_return IS NOT NULL
GROUP BY symbol
ORDER BY volatility DESC;
```

<BR>


7. What's the current status of all stocks?
```SQL
WITH status_table AS (
    SELECT 
        symbol, date, close, macd_crossover, rsi_signal, recommendation,
        CASE
            WHEN sma_7 > sma_14 AND sma_14 > sma_30 THEN 'Uptrend'
            WHEN sma_7 < sma_14 AND sma_14 < sma_30 THEN 'Downtrend'
            ELSE 'Sideways'
        END AS trend_status,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rows
    FROM stock_data_cleaned
)
SELECT 
    symbol, date, close, macd_crossover, rsi_signal, recommendation, trend_status
FROM status_table 
WHERE rows = 1
ORDER BY symbol;
```


<BR>

8. Which days of the week have the most buy/sell signals?
```sql
SELECT
    TO_CHAR(date, 'Day') AS weekdays,
    COUNT(*) AS total,
    COUNT(CASE WHEN macd_crossover = 'BUY' THEN 1 END) AS buy_signals,
    COUNT(CASE WHEN macd_crossover = 'SELL' THEN 1 END) AS sell_signals,
    COUNT(CASE WHEN macd_crossover = 'HOLD' THEN 1 END) AS neutral_signals
FROM stock_data_cleaned
GROUP BY weekdays
ORDER BY buy_signals DESC;
```



