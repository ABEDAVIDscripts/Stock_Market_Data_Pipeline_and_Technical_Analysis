# Data Consolidation & Redshift Loading

<BR>
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
    - Add Lambda Permission to EC2 Role
    - IAM Console → Roles → fp_ec2_access
    - Add permissions → Attach policies
    - Search & Check: AWSLambdaRole
    - Add permissions
  
- Restart Airflow
- Trigger the DAG UI

<br>
<br>

### 3. Redshift and SQL Analysis
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

