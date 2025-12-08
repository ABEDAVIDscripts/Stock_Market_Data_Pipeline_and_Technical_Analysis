# Stock Market Data Pipeline and Technical Analysis

This project demonstrates a production-grade ETL pipeline that automates stock market data collection and analysis for 5 major tech stocks (AAPL, AMZN, GOOGL, MSFT, TSLA). The pipeline extracts daily stock prices, calculates 10+ technical indicators, and stores the results in a data warehouse for SQL-based analysis.

<br>
<br>

### Project Overview

This project demonstrates a fully automated Stock Market Data Pipeline built using:
- Apache Airflow for orchestrating ETL pipelines
- AWS S3 for multi-layered storage (Landing → Intermediate → Transformed)
- AWS Lambda for computing SMA, RSI, MACD & consolidating results
- AWS Redshift as the data warehouse
- SQL analytics to answer real financial analysis questions


<br>
<br>

### Architecture Diagram



<br>
<br>


### Technologies Used

|          Service/Tool|                 Purpose          | Configuration / Details |
|---------------------:|---------------------------------:|--------------------------:|
| AWS EC2        | Airflow host & scheduler        | Ubuntu 20.04, t2.medium, Airflow 2.x |
| AWS S3          | Data Lake Storage               | Buckets: landing / intermediate / transformed |
| AWS Lambda      | Technical Indicator Processing  | Python (SMA / RSI / MACD functions), boto3 |
| AWS Redshift    | Data Warehouse for Analytics    | ra3.xlplus, 1-node cluster |
| Amazon QuickSight| Dashboard & Visualization      | Standard Edition |
| Python          | ETL & Lambda logic              | pandas, numpy, requests, boto3 |
| **APIs**            | Market Data Ingestion           | Alpha Vantage, Yahoo Finance (yfinance) |
| **IAM Roles & Policies** | Secure Access Control      | S3 → Lambda → Redshift |




<br>
<br>


### Project Structure Diagram

<div align="center">

┌─────────────────────────────────┐  
Stock Market Data Pipeline  
└─────────────────────────────────┘  
↓  
Multiple APIs (Alpha Vantage, Yahoo Finance)  
↓  
┌───────────────────────────────────────┐  
Airflow on EC2 (Multi-Symbol Extract)  
Symbols: AAPL, GOOGL, MSFT, AMZN, TSLA  
└───────────────────────────────────────┘  
↓  
┌───────────────────────────────────────┐  
S3 Landing Zone (Raw JSON by Symbol)  
└───────────────────────────────────────┘  
↓  
┌───────────────────────────────────────┐  
Lambda 1: Parse & Calculate Indicators  
└───────────────────────────────────────┘  
↓  
┌───────────────────────────────────────┐  
S3 Transformed Zone (CSV + Indicators)  
└───────────────────────────────────────┘  
↓  
┌───────────────────────────────────────┐  
Redshift (Time-Series Optimized)  
└───────────────────────────────────────┘  
↓  
┌────────────────────────────────────────┐  
QuickSight (Trading Dashboard)  
Candlestick charts  
Technical indicators overlay  
Multi-symbol comparison  
Real-time alerts  
└────────────────────────────────────────┘  

</div>





<BR>
<BR>
<BR>

## End-to-End Pipeline Workflow : Part 1 - Infrastructure & Data Extraction


### Step 1. AWS Infrastructure Setup

#### 1.1: Quick IAM Setup (If Starting Fresh)
- Create IAM User Group:
  - IAM Console → User Groups → Create Group
  - Name: financial_pipeline_group
  - Attach: `AdministratorAccess`
  - Create Group

<br>

- Create IAM User:
  - IAM → Users → Create User
  - Username: `financial_pipeline_user`
  - Provide AWS Management Console access
  - Set password: *****
  - Add to group: `financial_pipeline_group`
  - Download credentials CSV  

<br>

- Create Access Keys:
  - User → Security Credentials
  - Create Access Key → CLI
  - Download CSV (keep secure!)

<br>
<br>

#### 1.2: Launch EC2 Instance
- EC2 Console → Launch Instance
- Configuration:
  - Name: `fp-ec2`
  - AMI: Ubuntu Server 22.04 LTS
  - Instance Type: `t2.medium` (2 vCPU, 4GB RAM)
  - Key Pair: Create `fp-keypair.pem` (download and save)

- Network Settings:
  - Allow SSH (port 22)
  - Allow HTTP (port 80)
  - Allow HTTPS (port 443)
  - Custom TCP port 8080 (Airflow UI)

- Launch Instance

<br>
<br>

#### 1.3: Add Custom Security Rule
- EC2 → Instances → Select instance → Security tab
- Security group → Inbound Rules → Edit
- Add Rule for Airflow:
```
Type: Custom TCP
Port: 8080
Source: 0.0.0.0/0
Description: Airflow UI
```
- Save Rules


<br>
<br>


#### 1.4: Create IAM Role for EC2
- Allow EC2 to access S3, Redshift without storing credentials
  - IAM → Roles → Create Role
  - Trusted entity: AWS Service → EC2
  - Attach policies:
    ```
    AmazonS3FullAccess
    AmazonRedshiftFullAccess
    Role name: fp_ec2_full_access
    Create Role
    ```

<br>

- Attach to EC2:
  - EC2 → Instances → Select instance
  - Actions → Security → Modify IAM Role
  - Select: `fp_ec2_full_access`
  - Update IAM Role


<br>
<br>

#### 1.5: Create S3 Buckets
Create 3 buckets for data zones:
- Bucket 1 - Landing Zone:
  - S3 Console → Create Bucket
  - Name: `fp-landing-zone-bucket`
  - Region: `eu-north-1` (Europe - Stockholm)
  - Create Bucket

<br>

- Bucket 2 - Intermediate Zone:
  - Name: `fp-intermediate-bucket`
  - Same region
  - Create

<br>

- Bucket 3 - Transformed Zone:
  - Name: `fp-transformed-bucket`
  - Same region
  - Create



<BR>
<BR>
<BR>

### 2. EC2 & Airflow Installation
Identical to Zillow setup with minor adjustments.

#### 2.1: Connect to EC2
  - EC2 Console → Select instance → Connect
  - EC2 Instance Connect → Connect

<br>

#### 2.2: System Updates & Python
```bash
# Update package lists
sudo apt update

# Install Python tools
sudo apt install python3-pip -y
sudo apt install python3-venv -y

# Install screen for process management
sudo apt install screen -y
```

<br>

#### 2.3: Create Virtual Environment
```bash
# Create virtual environment
python3 -m venv fp_env

# Activate
source fp_env/bin/activate
```

<br>

#### 2.4: Install Apache Airflow & Additional Libraries
```bash
# Install Airflow
pip install apache-airflow

# Financial data libraries
pip install yfinance          # Yahoo Finance API
pip install alpha-vantage     # Alpha Vantage API wrapper
pip install pandas-ta         # Technical analysis indicators

# AWS providers
pip install apache-airflow-providers-amazon

# Data processing
pip install numpy pandas requests
```

<br>

#### 2.5: Initialize Airflow
```bash
# Set Airflow home
export AIRFLOW_HOME=~/airflow

# Initialize database
airflow db migrate 

# Create admin user
airflow users create \
--username admin \
--firstname David \
--lastname Abe \
--role Admin \
--email emailaddress@xyz.com \
--password: *******

# Verify
airflow users list
```

<br>

#### 2.6: Start Airflow
- Configure standalone_dag_processor
``` bash
#inside terminal:
nano ~/airflow/airflow.cfg
#Changed: standalone_dag_processor = False to True
```

- Start Airflow Standalone:
```bash
screen -S airflow standalone
source fp_env/bin/activate
export AIRFLOW_HOME=~/airflow
airflow standalone
```

<br>

#### 2.7: Access Airflow UI
- Get EC2 public IP: 
  - EC2 Console → Instance → Public IPv4
  - Browser: `http://<EC2-PUBLIC-IP>:8080`
  - Login: username `admin`, password `*****`

<br>

- Remove the Default Dag Examples
  - Stop airflow standalone
  - Run this in a terminal: `nano ~/airflow/airflow.cfg`
  - change load_examples = True to False
  - restart airflow standalone


<BR>
<BR>
<BR>


### 3. API Selection & Setup
Stock data requires choosing the right API(s):

- Alpha Vantage
  - Free Tier Limits and reliable
  - 500 API calls per day
  - 5 API calls per minute
  - All endpoints available

- Yahoo Finance
  - Backup for Alpha Vantage
  - Failover logic: If Alpha Vantage limit reached, use Yahoo
  - Unlimited but unofficial


<br>

#### 3.1: Get Alpha Vantage API Key
- Go to: https://www.alphavantage.co/support/#api-key
- Enter email, click GET FREE API KEY
- Copy API key and store securely (e.g., `ABC123XYZ`)

<br>

- Create API Configuration File
- Connect VS Code to EC2:
  - VS Code → Remote Explorer → SSH
  - Add host configuration with your EC2 IP and keypair
  - Connect to EC2

<br>

- Create config file:
  - In VS Code, navigate to `/home/ubuntu/airflow/`
  - New File → `config_api.json`
  - Script:
    ```json
    {
    "alpha_vantage_key": "ALPHA_VANTAGE_KEY_HERE",
    "symbols": ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
    "interval": "daily",
    "outputsize": "compact"
    }
    ```
  - Save file

<br>


#### 3.2: Install yfinance as Backup
In EC2 terminal (with venv activated)
```bash
pip install yfinance
```

<BR>
<BR>
<BR>


### 4: Building the Extraction Pipeline

#### 4.1: Create DAGs Folder
In VS Code (connected to EC2):
- Navigate to `/home/ubuntu/airflow/`
- New Folder → `dags`

<br>

#### 4.2: Create Financial Analytics DAG
- Create file: `/home/ubuntu/airflow/dags/financial_analytics.py`
- Dag Script:
```bash
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import requests
import time

# Load API configuration
with open('/home/ubuntu/airflow/config_api.json', 'r') as config_file:
    api_config = json.load(config_file)

ALPHA_VANTAGE_KEY = api_config['alpha_vantage_key']
SYMBOLS = api_config['symbols']

def extract_stock_data(**kwargs):
    """Extract stock data from Alpha Vantage API"""
    symbol = kwargs['symbol']
    api_key = kwargs['api_key']
    
    # Generate timestamp
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d_%H%M%S")
    
    print(f"Extracting data for {symbol}...")
    
    # API endpoint
    url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'apikey': api_key,
        'outputsize': 'compact',
        'datatype': 'json'
    }
    
    # Make API request with retry
    max_retries = 3
    response_data = None
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            response_data = response.json()
            
            # Check for errors
            if 'Error Message' in response_data:
                raise Exception(f"API Error: {response_data['Error Message']}")
            
            if 'Note' in response_data:
                print(f"Rate limit hit for {symbol}. Waiting 60 seconds...")
                time.sleep(60)
                continue
            
            if 'Time Series (Daily)' not in response_data:
                raise Exception(f"No time series data found for {symbol}")
            
            break
            
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {symbol}: {str(e)}")
            if attempt == max_retries - 1:
                print(f"Falling back to Yahoo Finance for {symbol}")
                return extract_yahoo_finance(symbol, dt_string)
            time.sleep(10)
    
    # Save JSON file
    output_file_path = f"/home/ubuntu/stock_{symbol}_{dt_string}.json"
    
    with open(output_file_path, 'w') as output_file:
        json.dump(response_data, output_file, indent=4)
    
    print(f"Successfully extracted {symbol} data to {output_file_path}")
    
    # Return the file path for the load task
    return output_file_path

def extract_yahoo_finance(symbol, dt_string):
    """Fallback function using Yahoo Finance"""
    import yfinance as yf
    
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="3mo")
    
    # Convert to Alpha Vantage format
    time_series = {}
    for date, row in hist.iterrows():
        date_str = date.strftime('%Y-%m-%d')
        time_series[date_str] = {
            '1. open': str(row['Open']),
            '2. high': str(row['High']),
            '3. low': str(row['Low']),
            '4. close': str(row['Close']),
            '5. volume': str(int(row['Volume']))
        }
    
    response_data = {
        'Meta Data': {
            '1. Information': 'Daily Prices (Yahoo Finance)',
            '2. Symbol': symbol,
            '3. Last Refreshed': datetime.now().strftime('%Y-%m-%d'),
            '4. Time Zone': 'US/Eastern'
        },
        'Time Series (Daily)': time_series
    }
    
    output_file_path = f"/home/ubuntu/stock_{symbol}_{dt_string}.json"
    
    with open(output_file_path, 'w') as f:
        json.dump(response_data, f, indent=4)
    
    return output_file_path

# DAG settings
default_args = {
    'owner': 'financial_pipeline',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'email': ['your-email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
with DAG(
    'financial_market_pipeline',
    default_args=default_args,
    description='Extract stock market data for multiple symbols',
    schedule='0 18 * * 1-5',  # 6 PM, Monday-Friday
    catchup=False,
    max_active_runs=1
) as dag:
    
    previous_task = None
    
    for symbol in SYMBOLS:
        # Extract data task
        extract_task = PythonOperator(
            task_id=f'extract_{symbol}',
            python_callable=extract_stock_data,
            op_kwargs={
                'symbol': symbol,
                'api_key': ALPHA_VANTAGE_KEY
            }
        )
        
        # Load to S3 task - FIXED SYNTAX
        load_task = BashOperator(
            task_id=f'load_{symbol}_to_s3',
            bash_command="""
            FILE_PATH="{{ ti.xcom_pull(task_ids='extract_""" + symbol + """') }}"
            echo "Moving file: $FILE_PATH"
            aws s3 mv "$FILE_PATH" s3://fp-landing-zone-bucket/""" + symbol + """/
            """
        )
        
        # Set task order
        extract_task >> load_task
        
        # Add delay between symbols (respect rate limits)
        if previous_task is not None:
            previous_task >> extract_task
        
        previous_task = load_task
```

<br>

#### 4.3: Create AWS Connection in Airflow

- Airflow UI → Admin → Connections → Add Connection
- Fill in:
``` bash
Connection Id: aws_fp_conn
Connection Type: Amazon Web Services
Extra: {"region_name": "eu-north-1"}
```
- Save

<br>

#### 4.4: Trigger Dag Via UI
This will load the json file to the landing zone bucket

- Airflow UI → Home 
- Select `financial_market_pipeline`
- Trigger

<BR>
<BR>
<BR>


## Part 2: Data Transformation with Lambda

Purpose: 
- Create Lambda functions to process raw stock data
- Calculate technical indicators (SMA, RSI, MACD)
- Store transformed data in intermediate bucket
- Trigger Lambda automatically when new files arrive in S3

<br>

### 1. Create Lambda Execution Role
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

#### 3.1: Create Lambda Function - Calculate SMA 

- Create the function:
  - Lambda Console → Functions → Create function
  ```
  Configure:
  Author from scratch
  Function name: calculate_sma
  Runtime: Python 3.12
  Architecture: x86_64
  Permissions: existing role → fp_lambda_execution_role
  ```
  - Create function

<br>

- Add the layer:
  - Layers section → Add a layer → 
  - Custom layers → pandas-numpy-layer
  - Add
  
<br>

- Configure memory and timeout:
  ```
  Configuration
  General configuration → Edit
  Memory: 512 MB
  Timeout: 3 minutes
  ```
  - Click "Save"

<br>

- Add script in the Code tab:

``` bash
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
```
Configuration tab → Permissions 
Role name → Opens IAM 
Add permissions → Attach policies → `AWSLambdaRole`
Check AWSLambdaRole 
Add permissions
```

<br>

- Add S3 Trigger ONLY to calculate_sma
This configure Lambda to run automatically when files arrive:
```
Lambda function → Add trigger
Configure trigger:
Source: S3
Bucket: fp-landing-zone-bucket
Event type: All object create events
Suffix: .json
Acknowledge recursive invocation
Add
```

<br>

- Test the Lambda Function 
  - Manual test:
  ```
  Test tab in Lambda
  Create new event
  Event name: test_sma
  Test event code:
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
```
Lambda → Create function
Function name: calculate_rsi
Runtime: Python 3.12
Use existing role: financial_lambda_execution_role
Create function
```

<br>

- Add layer and configure (same as SMA):
```
Add pandas-numpy-layer
Memory: 512 MB
Timeout: 3 minutes
```

<br>

- Add code:
```bash
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
```
Lambda → Create function
Function name: calculate_macd
Runtime: Python 3.12
Use existing role: financial_lambda_execution_role
```

<br>

- Add layer and configure (same as RSI):
```
Add pandas-numpy-layer
Memory: 512 MB
Timeout: 3 minutes
```

<br>

- Add code
```bash
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
```
IAM Console → Roles → fp_ec2_access
Add permissions → Attach policies
Search: CloudWatchLogsReadOnlyAccess
Add permissions
```

<br>

- Trigger DAG UI

<BR>
<BR>
<BR>

## Part 3: Data Consolidation & Redshift Loading
### 1: Create Consolidation Lambda
This Lambda will combine all 3 indicators for each stock into one consolidated JSON file.  
<br>
 
- Create the Lambda Function: 
```
Lambda Console → Functions → Create function
Function name: consolidate_stock_data 
Runtime: Python 3.12  
Architecture: x86_64  
Permissions: Use existing role → fp_lambda_execution_role
```

<br> 

- Add the pandas layer: 
```
Layers → Add a layer
Custom layers → pandas-numpy-layer → Version 1  
Add
```

<br>

- Configure memory/timeout: 
```
Configuration tab → General configuration → Edit  
Memory: 512 MB  
Timeout: 5 minutes  
Save
```

<br>

- Add the consolidation code: 
```bash
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
- Add consolidation task at the end of `previous_task = load_task` <br>
```bash
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
  ```
  # Add Lambda Permission to EC2 Role
	IAM Console → Roles → fp_ec2_access
	Add permissions → Attach policies
	Search & Check: AWSLambdaRole
	Add permissions
  ```

- Restart Airflow
- Trigger the DAG UI

<br>
<br>

### 3. Redshift and SQL Analysis
#### 3.1: Add Redshift
- Create Redshift Cluster
```
AWS Console → Amazon Redshift → Clusters → Create cluster
Basic Configuration:

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

- Configure Security Group
Allow connections to Redshift from your EC2 and from anywhere. <BR>
```
Redshift cluster > property > Network and security settings
VPC security group > security group link 

Edit inbound rules
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
- Fill in Database user name and password


<br>
<br>

#### 3.3:  Create a Table 
- Create temporary staging table (matches CSV structure)
```bash
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

#### 3.4: Load Stock Data

1. AAPL STOCK
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
```bash
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
```BASH
SELECT COUNT(*) FROM stock_data_temp;
```

<BR>

- Move AAPL Data to Final Table
```bash
INSERT INTO Final_stock_data 
SELECT 
    'AAPL' AS symbol,
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
```BASH
SELECT COUNT(*) 
FROM Final_stock_data 
WHERE symbol = 'AAPL';
```

- Clear temp table for next stock
```BASH
TRUNCATE TABLE stock_data_temp;
```

<BR>
<BR>

2. AMZN STOCK
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
```bash
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
```bash
SELECT COUNT(*)
FROM stock_data_temp;
```

<br> 

- Move to final table
```bash
INSERT INTO Final_stock_data 
SELECT 
    'AMZN' AS symbol,
    date, close, sma_7, sma_14, sma_30, rsi, rsi_signal,
    macd, macd_signal_line, histogram, macd_crossover, recommendation
FROM stock_data_temp;
```

<br>

- Verify
```bash
SELECT COUNT(*)
FROM final_stock_data
WHERE symbol='AMZN';
```

<br>

- Clear temp table for next stock
```bash
TRUNCATE TABLE stock_data_temp;
```

<br>
<br>

3. GOOGL STOCK
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
```bash
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
```bash
INSERT INTO Final_stock_data 
SELECT 
    'AMZN' AS symbol,
    date, close, sma_7, sma_14, sma_30, rsi, rsi_signal,
    macd, macd_signal_line, histogram, macd_crossover, recommendation
FROM stock_data_temp;
```

<br>

-- Clear temp table for next stock
`TRUNCATE TABLE stock_data_temp;`

<br>
<br>

4. Same process for `MSFT` and `TSLA`

<br>
<br>

#### 3.5: Data Preparation
1. 
- Check data record
```bash
SELECT
COUNT(DISTINCT symbol) AS no_of_symbols,
COUNT(*) AS no_of_records,
MAX(date) AS newest_date,
MIN(date) AS oldest_date,
MAX(date) - MIN(date) AS date_range_in_days
FROM final_stock_data;
```

- Record per Symbol
```bash
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

2. Check for Duplicate
```
SELECT
    COUNT(*) AS duplicates, 
    symbol, date
FROM final_stock_data
GROUP BY symbol, date
HAVING COUNT(*) > 1;
```



#### 3.6: Business Analysis
Business question


