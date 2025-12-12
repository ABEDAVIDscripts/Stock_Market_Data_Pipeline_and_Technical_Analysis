# Data Transformation with Lambda

<BR>


Purpose: 
- Create Lambda functions to process raw stock data
- Calculate technical indicators (SMA, RSI, MACD)
- Store transformed data in intermediate bucket
- Trigger Lambda automatically when new files arrive in S3

<br>

### 1. Create Lambda Execution Role
<img width="700" alt="permissions policies" src="https://github.com/user-attachments/assets/fff0a621-f65e-4c89-bbd9-1097c5a7fe5c" />

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
```
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
<img width="700" alt="calculate sma" src="https://github.com/user-attachments/assets/dadbdeb7-7d5c-4ea8-b70b-78e1fab2d129" />

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

<BR>
<BR>

<BR>
