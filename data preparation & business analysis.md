## Data Preparation and Business Analysis


### 1.  Create a Table 
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

#### 2.5: Verify
- Check data record
```sql
SELECT
COUNT(DISTINCT symbol) AS no_of_symbols,
COUNT(*) AS no_of_records,
MAX(date) AS newest_date,
MIN(date) AS oldest_date,
MAX(date) - MIN(date) AS date_range_in_days
FROM final_stock_data;
```

<BR>

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

### 3. Data Preparation

1. Check for Duplicate

<div style="display: flex; justify-content: space-between; gap: 4%;">
  <img src="https://github.com/user-attachments/assets/913d81fb-eb99-4d14-85cd-1569d355df11"  aalt="check for duplicate" width="48%">
  <img src="https://github.com/user-attachments/assets/94af75e8-f72c-42bc-b471-edcdb34b49e7" alt="check for duplicate result" width="48%">
</div>

<BR>

```sql
SELECT
    COUNT(*) AS duplicates, 
    symbol, date
FROM final_stock_data
GROUP BY symbol, date
HAVING COUNT(*) > 1;
```

<br>

- Text error in APPL, UPDATE to AAPL
```SQL
UPDATE stock_data_cleaned
SET symbol = 'AAPL'
WHERE symbol = 'APPL';
```

<br>

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

<br>

- verify
```sql
SELECT
    count(*) AS duplicates,
    symbol, date
FROM stocK_data_cleaned 
GROUP BY symbol, date
HAVING count(*) > 1;
```

<br>

- Count remaining data
```sql
SELECT
    count(*) AS datafile
FROM stocK_data_cleaned ;
```

<br>
<br>

2. Check for NULL Value

<div style="display: flex; justify-content: space-between; gap: 4%;">
  <img src="https://github.com/user-attachments/assets/8627849e-2753-4307-996d-c3e076c0a274" width="48%">
  <img src="https://github.com/user-attachments/assets/ee4794a2-ea62-48d0-aaee-95fdcc67daba" width="48%">
</div>

<br>

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

3. DATA VALIDITY CHECKS

<img width="700" alt="Check for Negative Prices x Zero Prices" src="https://github.com/user-attachments/assets/b4ff1620-0ce5-4196-ae53-d43bb04562ec" />

<br>
<br>

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

<br>

Output: <br>

<img width="700" alt="Check for Negative Prices result" src="https://github.com/user-attachments/assets/bec3c1b6-6334-4a4a-a0f7-db463cf08dbd" />

<br>
<br>

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

<br>

Output: <br>
<img width="700" alt="Check for Zero Prices result" src="https://github.com/user-attachments/assets/f6b78faa-085d-448d-9459-14942a313c71" />

<br>

- Check RSI out of range, out of range = -0 OR 100+


```sql
SELECT
    symbol, date, rsi, rsi_signal
FROM stock_data_cleaned 
WHERE 
    rsi < 0 OR rsi > 100
ORDER BY symbol;
```

<br>

Output <br>
<img width="700"  alt="Check RSI out of range (0-100) result" src="https://github.com/user-attachments/assets/550835dd-c0d1-4f9e-808c-8d05c7ffdc6c" />

<br>


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

<br>

Output: <br>

<img width="700" alt="result Check for extreme price movements (above 50% in one day - potential data error)" src="https://github.com/user-attachments/assets/fb0f5922-619a-41e4-bf24-75ed7b2fa3ef" />

<br>
<br>


- Error found, Investigate
- See the AAPL price structure

<div style="display: flex; justify-content: space-between; gap: 4%;">
  <img src="https://github.com/user-attachments/assets/ab2dbfb6-6667-4555-ab39-d382991c8b7d" width="48%">
  <img src="https://github.com/user-attachments/assets/076ee2f2-227b-4774-99c1-2c02a167b705" width="48%">
</div>

<br>

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

<br>

- AAPL price range

<div style="display: flex; justify-content: space-between; gap: 4%;">
  <img src="https://github.com/user-attachments/assets/b34878a6-ff6c-4037-9ad3-d3c361f8882e" width="48%">
  <img src="https://github.com/user-attachments/assets/65677759-18ff-4298-850c-e08cecb57c78" width="48%">
</div>

<br>

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

<br>


- Check all symbol for possible extreme price movements based on Valid close-price range in 2025 (52-week period)

<div style="display: flex; justify-content: space-between; gap: 4%;">
  <img src="https://github.com/user-attachments/assets/02a43dfa-68d1-43b0-b515-e30d2b227279" width="48%">
  <img src="https://github.com/user-attachments/assets/b04f1f84-2a52-4228-af3f-5be79777fe2f" width="48%">
</div>

<br>

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

<br>
<br>

- AAPL corrupt data = 7 
- Confirm the corrupt range for AAPL, to know the range to delete

```sql
SELECT symbol,
  COUNT(CASE WHEN close > 300 THEN 1 END) AS above_300,
  COUNT(CASE WHEN close < 150 THEN 1 END) AS below-150
FROM stock_data_cleaned
WHERE symbol = 'AAPL'
GROUP BY symbol;
```

<br>


Output <br>
<img width="700" alt="corrupted range for AAPL" src="https://github.com/user-attachments/assets/297aab19-10f5-4d5d-970a-9b8bb0189de4" />


<br>
<br>


> The excess price range for GOOGL are valid in 2025, hence delete only AAPL corrupted price range

- Delete AAPL corrupt data above 300
```sql
DELETE FROM stock_data_cleaned
WHERE symbol = 'AAPL' AND close > 300;
```

<br>

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

Ouput: <br>

<img width="700" alt="verification result for AAPL delete above 300" src="https://github.com/user-attachments/assets/a3647686-75f8-4a53-8d0b-f13f74a8fa45" />



<br>
<br>
<br>
<br>



## Business Question

1. which stock had the highest percentage gain/loss over the period?

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

Output <br>
<img width="700"  alt="1  RESULT  Which stock had the highest percentage gain or loss over the period" src="https://github.com/user-attachments/assets/9cefca46-e782-4b97-a3a3-750efd78a855" />

<br>
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
<br>


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
<br>


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







