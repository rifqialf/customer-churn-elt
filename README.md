# Telco Customer Churn 
Scenario: A major telecommunication company in California, US, is experiencing a concerning rate of customer churn, particularly in certain counties. To address this issue and improve customer retention, the company has decided to develop a comprehensive dashboard that provides actionable insights into customer churn trends.

## Introduction
### Project Requirements
The goal is to create a churn analysis dashboard for a telecom company, showing customer behavior, reasons for churn, and churned revenue across regions. Key metrics include:
1. **Churn Rate**: Percentage of customers who canceled service.
2. Average **Monthly Recurring Revenue (MRR)** for churned customers.
3. **Churned Service Plans**: Service types associated with churn.
4. **Churn Reasons**: Breakdown of why customers left (e.g., price, dissatisfaction, competitors).
5. **Demographics**: Gender, marital status, and age group correlations with churn.
6. **Customer Tenure**: How contract length influences churn.

The visual is to include interactive map with dynamic filter, as well as complementary charts and tables when necessary.

### Dataset
The customer churn dataset is retrieved from [this Kaggle page](https://www.kaggle.com/datasets/aadityabansalcodes/telecommunications-industry-customer-churn-dataset). Five CSV files were used in this project:
1. Telco_customer_churn_status.csv
2. Telco_customer_churn_services.csv
3. Telco_customer_churn_location.csv
4. Telco_customer_churn_demographics.csv
5. Telco_customer_churn_population.csv

EDA of the dataset can be explored at the Kaggle page, which are put into consideration but no extra action is deemed necessary for this project. A small modification were made to the customer churn dataset: A part of the original Q3 data is slightly altered, moved into separate files, and then labeled as Q4 data. This is to allow a showcase of how the developed data pipeline accepts new data, update existing data, and apply the updates into the dashboard as end result.

Below are data model of the customer churn data:
<img width="1024" alt="Customer Churn Data Model" src="https://github.com/user-attachments/assets/1a067ad8-8b74-4636-afbc-894b0dd03b7c">


Another dataset is shapefile of California county boundaries which was retrieved from [California Open Data Portal](https://data.ca.gov/dataset/ca-geographic-boundaries).

## Solution Architecture
### Process: Azure Databricks
Azure Databricks is well-suited for batch processing of this project size. It offers scalable, distributed computing through Spark, which can handle complex ETL (Extract, Transform, Load) tasks efficiently.

### Storage: Azure Data Lake Storage Gen2 + Delta Lake + Unity Catalog
Azure Data Lake Storage Gen2 provides scalable and secure storage for massive datasets, with high throughput and optimized performance through its hierarchical namespace. The ADLS is treated as Delta Lake to improve data reliability and performance by offering ACID transactions, schema enforcement, and time travel capabilities, which are crucial for handling evolving datasets. Meanwhile, Unity Catalog allows for easier management of data access policies across different teams while ensuring consistent and secure data usage across all Databricks workspaces.

In this project, two Delta Lakes are used:
1. Project ADLS Storage Delta Lake

   Owned data, such as CSVs containing telecom customer information, is ingested into the Delta Lake in the bronze layer. 
  
2. External ADLS Storage Delta Lake

   California county boundaries' shapefile data are accessed via Unity Catalog's external volumes.

Both Delta Lakes were setup with Access Connector for Databricks (namely managed identity) that were assigned as "Storage Blob Data Contributor" to let the ADLSs and the corresponding Unity Catalog to be used in the Databricks workspace.

***--ADLS Credential Illustration--***

This project adopts **Medallion Architecture**: structuring the data flow into three layers—bronze, silver, and gold—where raw data is first ingested into the bronze layer, refined in the silver layer, and served as analytics-ready datasets in the gold layer. This architecture ensures cleaner and organised structure of data in each phase.





-
-
-
-
-




#### Orchestration: Azure Data Factory
Azure Data Factory is a great choice for dealing with periodic data ingestion and processing. It allows you to build pipelines that automate the ETL process, integrate data, and execute Azure Databricks notebooks on a scheduled basis.
#### Visualization: Power BI
Power BI is an excellent visualization tool for presenting advanced and interactive churn insights to stakeholders especially with its ability to easily connect to Azure-based solutions.

Below is illustration of applied solution architecture in this project:

### Storage



