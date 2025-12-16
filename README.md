
# Financial Stock Market Data Pipeline and Technical Analysis

This project demonstrates a production-grade ETL pipeline that automates stock market data collection and analysis for 5 major tech stocks (AAPL, AMZN, GOOGL, MSFT, TSLA). The pipeline extracts daily stock prices, calculates 10+ technical indicators, and stores the results in a data warehouse for SQL-based analysis.

<br>
<br>


### Project Overview
<img width="700" alt="graph 2" src="https://github.com/user-attachments/assets/5d363277-637f-43dc-8227-83491f399954" />

This project demonstrates a fully automated Stock Market Data Pipeline built using:
- Apache Airflow for orchestrating ETL pipelines
- AWS S3 for multi-layered storage (Landing → Intermediate → Transformed)
- AWS Lambda for computing SMA, RSI, MACD & consolidating results
- AWS Redshift as the data warehouse
- SQL analytics to answer real financial stock analysis questions


<br>
<br>


### Architecture Flow Diagram

<img width="524" height="532" alt="flow diagram" src="https://github.com/user-attachments/assets/cc07d025-efcf-4e57-976a-4b722bc9d742" />


<br>
<BR>


### Technologies Used

<BR>

|          Service/Tool|                 Purpose          | Configuration / Details |
|---------------------:|---------------------------------:|--------------------------:|
| AWS EC2        | Airflow host & scheduler        | Ubuntu 20.04, t2.medium, Airflow 2.x |
| AWS S3          | Data Lake Storage               | Buckets: landing / intermediate / transformed |
| AWS Lambda      | Technical Indicator Processing  | Python (SMA / RSI / MACD functions), boto3 |
| AWS Redshift    | Data Warehouse for Analytics    | ra3.xlplus, 1-node cluster |
| Python          | ETL & Lambda logic              | pandas, numpy, requests, boto3 |
| APIs            | Market Data Ingestion           | Alpha Vantage, Yahoo Finance (yfinance) |
| IAM Roles & Policies | Secure Access Control      | S3 → Lambda → Redshift |


<br>
<br>

**Note: To manage cloud costs, the AWS resources for this data pipeline have been temporarily disabled or removed.**

<BR>
<BR>

