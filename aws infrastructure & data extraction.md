## AWS Infrastructure & Data Extraction 

### 1. AWS Infrastructure Setup

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
<img width="700" alt="fp landing zone bucket" src="https://github.com/user-attachments/assets/820c7faf-8581-46d7-81a0-75709729f18a" />

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
```

<br>

#### 2.6: Start Airflow
- Configure standalone dag processor
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
This will load the json file to the landing zone bucket <br>

<img width="700"  alt="trigger successful" src="https://github.com/user-attachments/assets/aab5c69e-c040-47fa-8c65-51613eeb23e3" />

<br>

- Airflow UI → Home 
- Select `financial_market_pipeline`
- Trigger

<br>

Output in landing zone bucket <br>
<div style="display: flex; justify-content: space-between; gap: 20px;">
  <img src="https://github.com/user-attachments/assets/c705b2f7-a511-4deb-9864-32c06e71555e" alt="fp landing zone bucket 2" width="48%">
  <img src="https://github.com/user-attachments/assets/0100c558-b5d9-4b7d-82fd-8ae7a78d897d" alt="graph 1" width="45%">
</div>

<BR>
<BR>
<BR>






