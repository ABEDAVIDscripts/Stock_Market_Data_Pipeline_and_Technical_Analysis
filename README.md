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

