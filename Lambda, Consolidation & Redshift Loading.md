# Data Transformation with Lambda, Consolidation and Redshift Loading

<BR>

Purpose: 
- Create Lambda functions to process raw stock data
- Calculate technical indicators (SMA, RSI, MACD)
- Store transformed data in intermediate bucket
- Trigger Lambda automatically when new files arrive in S3

<br>

### 1. Create Lambda Execution Role
<img width="700" alt="permissions policies" src="https://github.com/user-attachments/assets/4a2c3448-1dfb-45dc-9e41-7449d47ca41d" />

Create IAM Role for Lambda:
- AWS Console → IAM → Roles → "Create Role"
- Trusted entity:
```
AWS Service
Use case: Lambda
```

- Add permissions:
```
AmazonS3FullAccess
CloudWatchLogsFullAccess
```
- Name the role: `fp_lambda_execution_role`
- Create Role

<br>
<br>

### 2. Create Lambda Layer for Dependencies
Lambda needs extra Python libraries (pandas, numpy). A "layer" is needed to package them. <br>

- On EC2 instance:
```bash
# Create directory for layer
mkdir -p ~/lambda-layers/python
cd ~/lambda-layers

# Install pandas and numpy
pip install pandas numpy -t python/

# Create ZIP file
zip -r pandas-numpy-layer.zip python/

# Check file size
ls -lh pandas-numpy-layer.zip
```

<br>

- Download `pandas-numpy-layer.zip`
  - S3 Console → Open fp-landing-zone-bucket
  - lambda-layer → pandas-numpy-layer.zip
  - Download


- Upload to AWS Lambda
  - AWS Console → Lambda → Layers → Create layer
  - Name: `pandas-numpy-layer` → Upload a file from Amazon S3
  - `s3://fp-landing-zone-bucket/lambda-layer/pandas-numpy-layer.zip`
  - Compatible runtimes: `Python 3.12`


<br>
<br>

### 3. Create Lambda Function 
These Lambda functions are automatically triggered when new files appear in the Landing Zone bucket, and they save results to the Intermediate bucket.

<br>
<img width="700" alt="calculate sma" src="https://github.com/user-attachments/assets/fc5bbb5e-c266-48c0-941b-63e24aeda065" />

#### 3.1: Create Lambda Function - Calculate SMA 

- Create the function:
  - Lambda Console → Functions → Create function
  - Configure:
    - Author from scratch
    - Function name: calculate_sma
    - Runtime: Python 3.12
    - Architecture: x86_64
    - Permissions: existing role → `fp_lambda_execution_role`
  - Create function

<br>

- Add the layer:
  - Layers section → Add a layer → 
  - Custom layers → pandas-numpy-layer
  - Add
  
<br>

- Configure memory and timeout:
  - Configuration
  - General configuration → Edit
  - Memory: 512 MB
  - Timeout: 3 minutes
  - Save

<br>

- Add script in the Code tab:

``` python
import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    """
    Calculate Simple Moving Average (SMA) for stock data
    Then trigger RSI and MACD calculations
    """
    
    try:
        # Get bucket and key from S3 event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        print(f"Processing file: s3://{bucket}/{key}")
        
        # Extract symbol from key (e.g., AAPL/stock_AAPL_20251106_123456.json)
        symbol = key.split('/')[0]
        filename = key.split('/')[-1]
        
        # Download JSON file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read().decode('utf-8')
        stock_data = json.loads(file_content)
        
        # Extract time series data
        if 'Time Series (Daily)' not in stock_data:
            print(f"No time series data found in {filename}")
            return {
                'statusCode': 400,
                'body': json.dumps('No time series data found')
            }
        
        time_series = stock_data['Time Series (Daily)']
        
        # Convert to DataFrame
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        
        # Convert string values to float
        df['close'] = df['4. close'].astype(float)
        
        # Calculate SMAs (7, 14, 30 day periods)
        df['sma_7'] = df['close'].rolling(window=7).mean()
        df['sma_14'] = df['close'].rolling(window=14).mean()
        df['sma_30'] = df['close'].rolling(window=30).mean()
        
        # Remove NaN values
        df = df.dropna()
        
        # Create output dictionary
        output_data = {
            'metadata': {
                'symbol': symbol,
                'indicator': 'SMA',
                'processed_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'source_file': filename
            },
            'data': []
        }
        
        # Add data points
        for date, row in df.iterrows():
            output_data['data'].append({
                'date': date.strftime('%Y-%m-%d'),
                'close': round(row['close'], 2),
                'sma_7': round(row['sma_7'], 2),
                'sma_14': round(row['sma_14'], 2),
                'sma_30': round(row['sma_30'], 2)
            })
        
        # Generate output filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_key = f"{symbol}/sma_{symbol}_{timestamp}.json"
        
        # Save to intermediate bucket
        s3_client.put_object(
            Bucket='fp-intermediate-bucket',
            Key=output_key,
            Body=json.dumps(output_data, indent=2),
            ContentType='application/json'
        )
        
        print(f"✅ SMA calculated for {symbol} - Saved to s3://fp-intermediate-bucket/{output_key}")
        
        # ===== INVOKE OTHER LAMBDA FUNCTIONS =====
        print(f"Triggering RSI and MACD calculations for {symbol}...")
        
        try:
            # Invoke RSI Lambda (asynchronous)
            lambda_client.invoke(
                FunctionName='calculate_rsi',
                InvocationType='Event',  # Async - don't wait for response
                Payload=json.dumps(event)
            )
            print(f"✅ RSI Lambda invoked for {symbol}")
        except Exception as e:
            print(f"⚠️ Failed to invoke RSI: {str(e)}")
        
        try:
            # Invoke MACD Lambda (asynchronous)
            lambda_client.invoke(
                FunctionName='calculate_macd',
                InvocationType='Event',  # Async - don't wait for response
                Payload=json.dumps(event)
            )
            print(f"✅ MACD Lambda invoked for {symbol}")
        except Exception as e:
            print(f"⚠️ Failed to invoke MACD: {str(e)}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully calculated SMA for {symbol} and triggered RSI/MACD',
                'output_file': output_key,
                'data_points': len(output_data['data'])
            })
        }
        
    except Exception as e:
        print(f"❌ Error processing file: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
```

- Deploy

<br>
<br>

#### 3.2: Update Lambda Permissions
- The SMA function needs permission to invoke other Lambda functions.
  - Configuration tab → Permissions 
  - Role name → Opens IAM 
  - Add permissions → Attach policies → `AWSLambdaRole`
  - Check AWSLambdaRole 
  - Add permissions


<br>

- Add S3 Trigger ONLY to calculate_sma (This configure Lambda to run automatically when files arrive):
  - Lambda function → Add trigger
  - Configure trigger:
  - Source: S3
  - Bucket: `fp-landing-zone-bucket`
  - Event type: All object create events
  - Suffix: .json
  - Acknowledge recursive invocation
  - Add


<br>

- Test the Lambda Function 
  - Manual test:
    - Test tab in Lambda
    - Create new event
    - Event name: test_sma
    - Test event code:
    ```python
      {
        "Records": [ { "s3": { "bucket": {"name": "fp-landing-zone-bucket"},
        "object": {"key": "AAPL/stock_AAPL_20251106_213430.json"} } } ]
      }
      Save → Test
    ```

<br>

<img width="700" alt="sma aapl" src="https://github.com/user-attachments/assets/6c7c2eaa-ac67-43c2-8ad2-d8ab3873892c" />

  - Check results in S3 intermediate bucket 

<br>
<br>

#### 3.3: : Create Lambda Function - Calculate RSI 
RSI (Relative Strength Index) measures momentum - values 0-100. <br>
RSI > 70 = Overbought (stock might drop) <br>
RSI < 30 = Oversold (stock might rise) <br>

<br>

- Create function:

  - Lambda → Create function
  - Function name: calculate_rsi
  - Runtime: Python 3.12
  - existing role: financial_lambda_execution_role
  - Create function


<br>

- Add layer and configure (same as SMA):

  - Add pandas-numpy-layer
  - Memory: 512 MB
  - Timeout: 3 minutes


<br>

- Add code:
```python
import json
import boto3
import pandas as pd
from datetime import datetime

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Calculate Relative Strength Index (RSI) for stock data
    RSI = 100 - (100 / (1 + RS))
    where RS = Average Gain / Average Loss over 14 periods
    """
    
    try:
        # Get bucket and key from S3 event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        print(f"Processing file: s3://{bucket}/{key}")
        
        # Extract symbol from key
        symbol = key.split('/')[0]
        filename = key.split('/')[-1]
        
        # Download JSON file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read().decode('utf-8')
        stock_data = json.loads(file_content)
        
        # Extract time series data
        if 'Time Series (Daily)' not in stock_data:
            print(f"No time series data found in {filename}")
            return {
                'statusCode': 400,
                'body': json.dumps('No time series data found')
            }
        
        time_series = stock_data['Time Series (Daily)']
        
        # Convert to DataFrame
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        
        # Convert string values to float
        df['close'] = df['4. close'].astype(float)
        
        # Calculate price changes
        df['price_change'] = df['close'].diff()
        
        # Separate gains and losses
        df['gain'] = df['price_change'].apply(lambda x: x if x > 0 else 0)
        df['loss'] = df['price_change'].apply(lambda x: abs(x) if x < 0 else 0)
        
        # Calculate average gain and loss (14-day period)
        period = 14
        df['avg_gain'] = df['gain'].rolling(window=period).mean()
        df['avg_loss'] = df['loss'].rolling(window=period).mean()
        
        # Calculate RS and RSI
        df['rs'] = df['avg_gain'] / df['avg_loss']
        df['rsi'] = 100 - (100 / (1 + df['rs']))
        
        # Remove NaN values
        df = df.dropna()
        
        # Create output dictionary
        output_data = {
            'metadata': {
                'symbol': symbol,
                'indicator': 'RSI',
                'period': period,
                'processed_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'source_file': filename,
                'interpretation': {
                    'overbought': 'RSI > 70 (stock may be overvalued)',
                    'oversold': 'RSI < 30 (stock may be undervalued)',
                    'neutral': '30 <= RSI <= 70'
                }
            },
            'data': []
        }
        
        # Add data points
        for date, row in df.iterrows():
            rsi_value = round(row['rsi'], 2)
            
            # Determine signal
            if rsi_value > 70:
                signal = 'OVERBOUGHT'
            elif rsi_value < 30:
                signal = 'OVERSOLD'
            else:
                signal = 'NEUTRAL'
            
            output_data['data'].append({
                'date': date.strftime('%Y-%m-%d'),
                'close': round(row['close'], 2),
                'rsi': rsi_value,
                'signal': signal
            })
        
        # Generate output filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_key = f"{symbol}/rsi_{symbol}_{timestamp}.json"
        
        # Save to intermediate bucket
        s3_client.put_object(
            Bucket='fp-intermediate-bucket',
            Key=output_key,
            Body=json.dumps(output_data, indent=2),
            ContentType='application/json'
        )
        
        print(f"Successfully processed {symbol} - Saved to s3://fp-intermediate-bucket/{output_key}")
        
        # Get latest signal
        latest = output_data['data'][-1]
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully calculated RSI for {symbol}',
                'output_file': output_key,
                'data_points': len(output_data['data']),
                'latest_rsi': latest['rsi'],
                'latest_signal': latest['signal']
            })
        }
        
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
```

- Deploy

<br>
<br>

#### 3.4: Create Lambda Function - Calculate MACD
MACD (Moving Average Convergence Divergence) shows trend changes. <br>
MACD line crosses above signal = Buy signal. <br>
MACD line crosses below signal = Sell signal. <br>

<br>

- Create function:
  - Lambda → Create function
  - Function name: calculate_macd
  - Runtime: Python 3.12
  - Use existing role: financial_lambda_execution_role


<br>

- Add layer and configure (same as RSI):
```
Add pandas-numpy-layer
Memory: 512 MB
Timeout: 3 minutes
```

<br>

- Add code
```python
import json
import boto3
import pandas as pd
from datetime import datetime

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Calculate MACD (Moving Average Convergence Divergence) for stock data
    MACD = 12-day EMA - 26-day EMA
    Signal Line = 9-day EMA of MACD
    Histogram = MACD - Signal Line
    """
    
    try:
        # Get bucket and key from S3 event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        print(f"Processing file: s3://{bucket}/{key}")
        
        # Extract symbol from key
        symbol = key.split('/')[0]
        filename = key.split('/')[-1]
        
        # Download JSON file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read().decode('utf-8')
        stock_data = json.loads(file_content)
        
        # Extract time series data
        if 'Time Series (Daily)' not in stock_data:
            print(f"No time series data found in {filename}")
            return {
                'statusCode': 400,
                'body': json.dumps('No time series data found')
            }
        
        time_series = stock_data['Time Series (Daily)']
        
        # Convert to DataFrame
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        
        # Convert string values to float
        df['close'] = df['4. close'].astype(float)
        
        # Calculate EMAs
        df['ema_12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema_26'] = df['close'].ewm(span=26, adjust=False).mean()
        
        # Calculate MACD line
        df['macd'] = df['ema_12'] - df['ema_26']
        
        # Calculate Signal line (9-day EMA of MACD)
        df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        
        # Calculate Histogram
        df['histogram'] = df['macd'] - df['signal']
        
        # Remove NaN values
        df = df.dropna()
        
        # Create output dictionary
        output_data = {
            'metadata': {
                'symbol': symbol,
                'indicator': 'MACD',
                'parameters': {
                    'fast_period': 12,
                    'slow_period': 26,
                    'signal_period': 9
                },
                'processed_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'source_file': filename,
                'interpretation': {
                    'bullish': 'MACD crosses above Signal line (Buy signal)',
                    'bearish': 'MACD crosses below Signal line (Sell signal)',
                    'histogram_positive': 'Histogram > 0 (Bullish momentum)',
                    'histogram_negative': 'Histogram < 0 (Bearish momentum)'
                }
            },
            'data': []
        }
        
        # Detect crossovers
        df['prev_macd'] = df['macd'].shift(1)
        df['prev_signal'] = df['signal'].shift(1)
        
        # Add data points
        for date, row in df.iterrows():
            # Determine crossover signal
            signal_type = 'HOLD'
            
            if pd.notna(row['prev_macd']) and pd.notna(row['prev_signal']):
                # Bullish crossover (MACD crosses above signal)
                if row['prev_macd'] <= row['prev_signal'] and row['macd'] > row['signal']:
                    signal_type = 'BUY'
                # Bearish crossover (MACD crosses below signal)
                elif row['prev_macd'] >= row['prev_signal'] and row['macd'] < row['signal']:
                    signal_type = 'SELL'
            
            output_data['data'].append({
                'date': date.strftime('%Y-%m-%d'),
                'close': round(row['close'], 2),
                'macd': round(row['macd'], 4),
                'signal': round(row['signal'], 4),
                'histogram': round(row['histogram'], 4),
                'crossover_signal': signal_type
            })
        
        # Generate output filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_key = f"{symbol}/macd_{symbol}_{timestamp}.json"
        
        # Save to intermediate bucket
        s3_client.put_object(
            Bucket='fp-intermediate-bucket',
            Key=output_key,
            Body=json.dumps(output_data, indent=2),
            ContentType='application/json'
        )
        
        print(f"Successfully processed {symbol} - Saved to s3://fp-intermediate-bucket/{output_key}")
        
        # Get latest signal
        latest = output_data['data'][-1]
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully calculated MACD for {symbol}',
                'output_file': output_key,
                'data_points': len(output_data['data']),
                'latest_macd': latest['macd'],
                'latest_signal': latest['signal'],
                'latest_crossover': latest['crossover_signal']
            })
        }
        
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
```

- Deploy

<br>
<br>

#### 3.5: Add CloudWatch and Trigger the UI Dag

- Add CloudWatch 
  - IAM Console → Roles → fp_ec2_access
  - Add permissions → Attach policies
  - Search: CloudWatchLogsReadOnlyAccess
  - Add permissions


<br>

- **Trigger DAG UI**

<br>

> When the DAG runs, the SMA Lambda automatically triggers RSI and MACD calculations 

<BR>

RSI Output <br>
<img width="700" alt="rsi aapl" src="https://github.com/user-attachments/assets/5d6bf1bf-7d45-4610-b8e7-5dff4b4037ce" />

<BR>
<BR>

MACD Output <br>
<img width="700" alt="macd aapl" src="https://github.com/user-attachments/assets/9300ef53-00c8-41eb-96fe-c8a7444922e9" />


<br>
<br>
<br>
<br>


# Data Consolidation & Redshift Loading

<BR>

### 1: Create Consolidation Lambda
This Lambda will combine all 3 indicators for each stock into one consolidated JSON file.  
<br>
 
- Create the Lambda Function: 
  - Lambda Console → Functions → Create function
  - Function name: consolidate_stock_data 
  - Runtime: Python 3.12  
  - Architecture: x86_64  
  - Permissions: Use existing role → `fp_lambda_execution_role`


<br> 

- Add the pandas layer: 
  - Layers → Add a layer
  - Custom layers → pandas-numpy-layer → Version 1  
  - Add


<br>

- Configure memory/timeout: 
  - Configuration tab → General configuration → Edit  
  - Memory: 512 MB  
  - Timeout: 5 minutes  
  - Save


<br>

- Add the consolidation code: 
```python
import json
import boto3
from datetime import datetime
import pandas as pd

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Consolidate SMA, RSI, and MACD data for all stocks
    Creates a single comprehensive report per stock
    """
    
    try:
        # Define stocks to process
        stocks = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
        
        # Process each stock
        for symbol in stocks:
            try:
                print(f"Processing {symbol}...")
                
                # Get all files for this symbol from intermediate bucket
                response = s3_client.list_objects_v2(
                    Bucket='fp-intermediate-bucket',
                    Prefix=f'{symbol}/'
                )
                
                if 'Contents' not in response:
                    print(f"No files found for {symbol}")
                    continue
                
                # Sort files by LastModified to get most recent
                files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
                
                # Find the most recent file for each indicator
                sma_file = None
                rsi_file = None
                macd_file = None
                
                for file in files:
                    key = file['Key']
                    if 'sma_' in key and not sma_file:
                        sma_file = key
                    elif 'rsi_' in key and not rsi_file:
                        rsi_file = key
                    elif 'macd_' in key and not macd_file:
                        macd_file = key
                    
                    # Break if we found all three
                    if sma_file and rsi_file and macd_file:
                        break
                
                if not (sma_file and rsi_file and macd_file):
                    print(f"Missing indicators for {symbol}")
                    print(f"  SMA: {sma_file}")
                    print(f"  RSI: {rsi_file}")
                    print(f"  MACD: {macd_file}")
                    continue
                
                # Load all three indicator files
                sma_data = load_json_from_s3('fp-intermediate-bucket', sma_file)
                rsi_data = load_json_from_s3('fp-intermediate-bucket', rsi_file)
                macd_data = load_json_from_s3('fp-intermediate-bucket', macd_file)
                
                # Consolidate the data
                consolidated = consolidate_data(symbol, sma_data, rsi_data, macd_data)
                
                # Save consolidated data
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_key = f"{symbol}/consolidated_{symbol}_{timestamp}.json"
                
                s3_client.put_object(
                    Bucket='fp-transformed-bucket',
                    Key=output_key,
                    Body=json.dumps(consolidated, indent=2),
                    ContentType='application/json'
                )
                
                print(f"✅ Consolidated {symbol} - Saved to s3://fp-transformed-bucket/{output_key}")
                
                # Also create a CSV version
                csv_data = create_csv_data(consolidated)
                csv_key = f"{symbol}/consolidated_{symbol}_{timestamp}.csv"
                
                s3_client.put_object(
                    Bucket='fp-transformed-bucket',
                    Key=csv_key,
                    Body=csv_data,
                    ContentType='text/csv'
                )
                
                print(f"✅ CSV created: s3://fp-transformed-bucket/{csv_key}")
                
            except Exception as e:
                print(f"❌ Error processing {symbol}: {str(e)}")
                continue
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully consolidated stock data',
                'stocks_processed': stocks
            })
        }
        
    except Exception as e:
        print(f"❌ Error in consolidation: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

def load_json_from_s3(bucket, key):
    """Load and parse JSON file from S3"""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    return json.loads(content)

def consolidate_data(symbol, sma_data, rsi_data, macd_data):
    """Combine all indicators into a single dataset"""
    
    # Create DataFrames
    sma_df = pd.DataFrame(sma_data['data'])
    rsi_df = pd.DataFrame(rsi_data['data'])
    macd_df = pd.DataFrame(macd_data['data'])
    
    # Merge on date
    merged = sma_df.merge(rsi_df[['date', 'rsi', 'signal']], on='date', how='inner', suffixes=('', '_rsi'))
    merged = merged.merge(
        macd_df[['date', 'macd', 'signal', 'histogram', 'crossover_signal']], 
        on='date', 
        how='inner',
        suffixes=('', '_macd')
    )
    
    # Rename columns for clarity
    merged.rename(columns={
        'signal': 'rsi_signal',
        'signal_macd': 'macd_signal_line',
        'crossover_signal': 'macd_crossover'
    }, inplace=True)
    
    # Generate trading recommendations
    merged['recommendation'] = merged.apply(generate_recommendation, axis=1)
    
    # Sort by date descending (most recent first)
    merged = merged.sort_values('date', ascending=False)
    
    # Create consolidated output
    consolidated = {
        'metadata': {
            'symbol': symbol,
            'report_type': 'Consolidated Technical Analysis',
            'generated_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'data_points': len(merged),
            'indicators': ['SMA', 'RSI', 'MACD'],
            'source_files': {
                'sma': sma_data['metadata']['source_file'],
                'rsi': rsi_data['metadata']['source_file'],
                'macd': macd_data['metadata']['source_file']
            }
        },
        'latest_analysis': merged.iloc[0].to_dict() if len(merged) > 0 else {},
        'historical_data': merged.to_dict('records')
    }
    
    return consolidated

def generate_recommendation(row):
    """Generate trading recommendation based on all indicators"""
    
    signals = []
    
    # SMA signal (price vs moving averages)
    if row['close'] > row['sma_30']:
        signals.append('bullish_trend')
    else:
        signals.append('bearish_trend')
    
    # RSI signal
    if row['rsi_signal'] == 'OVERSOLD':
        signals.append('buy_rsi')
    elif row['rsi_signal'] == 'OVERBOUGHT':
        signals.append('sell_rsi')
    else:
        signals.append('neutral_rsi')
    
    # MACD signal
    if row['macd_crossover'] == 'BUY':
        signals.append('buy_macd')
    elif row['macd_crossover'] == 'SELL':
        signals.append('sell_macd')
    else:
        signals.append('hold_macd')
    
    # Generate overall recommendation
    buy_signals = sum(1 for s in signals if 'buy' in s or 'bullish' in s)
    sell_signals = sum(1 for s in signals if 'sell' in s or 'bearish' in s)
    
    if buy_signals >= 2:
        return 'STRONG BUY' if buy_signals == 3 else 'BUY'
    elif sell_signals >= 2:
        return 'STRONG SELL' if sell_signals == 3 else 'SELL'
    else:
        return 'HOLD'

def create_csv_data(consolidated):
    """Convert consolidated JSON to CSV format"""
    
    # Create DataFrame from historical data
    df = pd.DataFrame(consolidated['historical_data'])
    
    # Select and order columns
    columns = [
        'date', 'close', 
        'sma_7', 'sma_14', 'sma_30',
        'rsi', 'rsi_signal',
        'macd', 'macd_signal_line', 'histogram', 'macd_crossover',
        'recommendation'
    ]
    
    df = df[columns]
    
    # Convert to CSV string
    return df.to_csv(index=False)
```

- Deploy


<br>
<br>

### 2. Test and Consolidation
#### 2.1: Test the Consolidation Lambda
- Test tab → new event
- Event name: `test_consolidate`
- Test code: 
```bash
{}
```
- Save → Test (success for all 5 stocks)
- Verify Consolidated Files in S3 transformed bucket

<br>


#### 2.2: Add Consolidation to Airflow DAG
Modify Airflow DAG to call the consolidation Lambda after all processing is done. <br>

- Stop airflow standalone
- Add consolidation task at the end of `previous_task = load_task` in `.py file` <br>
```python
    # This runs AFTER all stocks are processed
    consolidate_task = PythonOperator(
        task_id='consolidate_data',
        python_callable=trigger_consolidation,
        trigger_rule='all_success'  # Only run if all previous tasks succeeded
    )
    
    # Connect the last load task to consolidation
    previous_task >> consolidate_task
```

- Save

- Add Lambda Invoke Permission to EC2 Role
    - Add Lambda Permission to EC2 Role
    - IAM Console → Roles → fp_ec2_access
    - Add permissions → Attach policies
    - Search & Check: AWSLambdaRole
    - Add permissions
  
- Restart Airflow
<br>

<img width="700" alt="trigger consolidate" src="https://github.com/user-attachments/assets/9968138c-f3cc-4209-988e-39517af6e612" />

- Trigger the DAG UI

<br>
<br>

### 3. Redshift Loading
#### 3.1: Add Redshift
- Create Redshift Cluster
  - AWS Console → Amazon Redshift 
  - Clusters → Create cluster
  - Basic Configuration:
  ```
    Cluster identifier: financial-data-cluster
    Node type: ra3.xlplus
    Number of nodes: 1
    ```


<BR>

- Associate IAM Roles
I need to associate an IAM role so Redshift can access S3.
```
Create IAM role
Any S3 bucket
Create IAM role as default
```

<BR>

- Configure Security Group (Allow connections to Redshift from your EC2 and from anywhere.) 
  - Redshift cluster > property > Network and security settings
  - VPC security group > security group link 
  - Edit inbound rules
  ```
  # Add a new rule:
  Type: Custom TCP
  Port: 5439
  Source: 0.0.0.0/0
  Description: Allow Redshift access
  Save rules
  ```


<br>
<br>

#### 3.2: Launch Query Editor 

- Redshift Console 
- Query editor v2
- `financial-data-cluster`
- Fill in Database username and password


<br>
<br>



