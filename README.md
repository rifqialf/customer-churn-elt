# Telco Customer Churn ELT Project
Scenario: A major telecommunication company in California, US, is experiencing a concerning rate of customer churn, particularly in certain counties. To address this issue and improve customer retention, the company has decided to develop a comprehensive dashboard that provides actionable insights into customer churn trends.

[![Portfolio Video](https://img.youtube.com/vi/OKXdMpj-atU/0.jpg)](https://www.youtube.com/watch?v=OKXdMpj-atU)

## Introduction
### Project Requirements
The goal is to create a data pipeline for the company's customer churn data and visualization that shows customer behavior, reasons for churn, and churned revenue across county in the California state. Key metrics include:
1. **Churn Rate** - Percentage of customers who canceled service.
2. Average **Monthly Recurring Revenue (MRR)** for churned customers.
3. **Churned Service Plans** - Service types associated with churn.
4. **Churn Reasons** - Breakdown of why customers left (e.g., price, dissatisfaction, competitors).
5. **Demographics** - Gender, marital status, and age group correlations with churn.
6. **Customer Tenure** - How contract length influences churn.

The visual is to include interactive map with dynamic filter, as well as complementary charts and tables when necessary.

### Dataset
The customer churn dataset is retrieved from [this Kaggle page](https://www.kaggle.com/datasets/aadityabansalcodes/telecommunications-industry-customer-churn-dataset). Five CSV files were used in this project:
1. Telco_customer_churn_status.csv - Customer churn-related information such as churn labels (whether a customer has churned or not), churn scores, and churn reason.
2. Telco_customer_churn_services.csv - Details about the telecom services customers are subscribed to, such as phone, internet, TV, and additional services.
3. Telco_customer_churn_location.csv - Geographic information such as zip code and coordinates, potentially giving insights into how customer churn varies by region.
4. Telco_customer_churn_demographics.csv - Demographic information such as age, gender, and other customer classifications.
5. Telco_customer_churn_population.csv - Data about population in the regions.

The exploratory data analysis (EDA) of the dataset can be explored at the Kaggle page, which are put into consideration but no extra action is deemed necessary for this project. A small modification were made to the customer churn dataset: A part of the original Q3 data is slightly altered, moved into separate files, and then labeled as Q4 data. This is to allow a showcase of how the developed data pipeline accepts new data, update existing data, and apply the updates into the dashboard as end result.

Below are data model of the customer churn data:
<img width="1024" alt="Customer Churn Data Model" src="https://github.com/user-attachments/assets/1a067ad8-8b74-4636-afbc-894b0dd03b7c">

Another dataset is shapefile of California county boundaries which was retrieved from [California Open Data Portal](https://data.ca.gov/dataset/ca-geographic-boundaries).

## Solution Architecture
In this project, a set of solutions are designed for building a simple ELT (Extract, Load, Transform) pipeline, shown in figure below. This project based its solution architecture on [this Azure web page](https://learn.microsoft.com/en-us/azure/architecture/solution-ideas/articles/ingest-etl-stream-with-adb).

<img width="720" alt="Solution Architecture" src="https://github.com/user-attachments/assets/05eef5c0-3918-43b4-9f4c-a1aa75c744c3">


### Process: Azure Databricks
Azure Databricks is well-suited for batch processing of this project size. It offers scalable, distributed computing through Spark, which can handle complex ETL or ELT tasks efficiently.

<img width="720" alt="Customer Churn Data Model" src="https://github.com/user-attachments/assets/32dddfec-a432-45ca-b4cb-0142e02e6d90">

This Azure Databricks (as well as other Azure resources / services) leverages Pay-As-You-Go subscription with specified region _UK South_, which was selected as one of best CPU decisions during author's time in Europe in terms of resource availability (which is not the case anymore, unfortunately making UK South a costly decision for the project). Three budget alerts were set to anticipate so that the subscription is not exceeding $20 as shown below.

<img width="720" alt="Budget Alerts" src="https://github.com/user-attachments/assets/f4eaa77c-55c9-433a-8188-a42621350d85">

The ADLS access security to the Databricks is **Managed Identity** using Azure Access Connector for Databricks, which is more convenient security access to allow scalable project compared to other accesses (access keys, SAS token, and service principal).

In the workspace, a cluster was configured as shown below:

<img width="720" alt="Databricks Cluster Configuration" src="https://github.com/user-attachments/assets/678e172d-132b-4d2c-b49f-86da79cb3ec3">

Justification for the configuration are listed below:
1. Single user of Single Node - the most suitable configuration in terms of cost and workload for the project.
2. 15.4 LTS runtime - latest runtime option that enables Unity Catalog to be used in the workspace.
3. Standard_DS3_v2 with 4 Cores - for cost-wise decision suitable with the subscription's CPU quota.
4. Auto Termination: 20 minutes - to quickly terminate the cluster and to minimize cost.
5. Cluster Policy: Unrestricted - cluster policy is not necessary as only one cluster is being created in this project.

### Storage: Azure Data Lake Storage Gen2 + Delta Lake + Unity Catalog
Azure Data Lake Storage Gen2 provides scalable and secure storage for massive datasets, with high throughput and optimized performance through its hierarchical namespace. The ADLS is treated as Delta Lake to improve data reliability and performance by offering ACID transactions, schema enforcement, and time travel capabilities, which are crucial for handling evolving datasets. 

Meanwhile, Unity Catalog allows for easier management of data access policies across different teams while ensuring consistent and secure data usage across all Databricks workspaces.

<img width="720" alt="Unity Catalog Structure" src="https://github.com/user-attachments/assets/92649c92-dada-425e-acb2-beaa62fe184f">

In this project, two Delta Lakes are used:
1. Project ADLS Storage Delta Lake: The CSV files of telecom customer information are stored in the Delta Lake in the bronze layer.
2. External ADLS Storage Delta Lake: California county boundaries' shapefile data are stored in and accessed via Unity Catalog's external volumes.

The separated storage setup above is only to showcase how the Azure Databrick workspace can ingest data from multiple (external) Delta Lake. Both Delta Lakes were setup with Access Connector for Databricks (namely managed identity) that were assigned as "Storage Blob Data Contributor" to let the ADLSs and the corresponding Unity Catalog to be used in the Databricks workspace.

This project adopts **Medallion Architecture**: structuring the data flow into three layers—bronze, silver, and gold—where raw data is first ingested into the bronze layer, refined in the silver layer, and served as analytics-ready datasets in the gold layer. This architecture ensures cleaner and organised structure of data in each phase.

<img width="720" alt="Medallion" src="https://github.com/user-attachments/assets/5a2e2733-fc68-4113-afad-c6e7f4cebbec">

### Orchestration: Azure Data Factory
Azure Data Factory is a great choice for dealing with periodic data ingestion and processing. ADF allows you to build pipelines that automate the ETL process, integrate data, and execute Azure Databricks notebooks on a scheduled basis. Linked services were created to connect the Databricks workspace (using managed identity) and the two ADLSs (using simple access keys) to allow the pipeline to run accordingly. 

<img width="720" alt="Databricks Cluster Configuration" src="https://github.com/user-attachments/assets/8b3e7547-a363-431e-a56f-884089c729d7">

### Visualization: Power BI
Power BI is an excellent visualization tool for presenting advanced and interactive churn insights to stakeholders especially with its ability to easily connect to Azure-based solutions. In this project, Unity Catalog tables from Azure Databricks were imported (without DirectQuery) into Power BI Desktop using Personal Access Key.

<img width="720" alt="Databricks Cluster Configuration" src="https://github.com/user-attachments/assets/465bbe88-5842-483f-96c3-196ba42abdf6">

## Data Engineering
In the Databricks workspace, the project codebase is organised into 5 folders:
1. Ingestion - Contains scripts for extracting and loading data from sources.
2. Transformation - Includes code for cleaning, transforming, and structuring the ingested data to prepare it for analysis.
3. Presentation - Focuses on presenting the processed data.
4. Includes - Holds reusable modules or utility functions that are shared across different scripts.
5. Setup - Contains configuration scripts or initialization code needed to set up the environment.

<img width="260" alt="Notebook folders" src="https://github.com/user-attachments/assets/1f069bcd-7683-4051-9b70-182fe3ef55f0">

### Data Ingestion
The ingestion folder mainly contains Python-based notebooks to ingest each of the 5 raw files. One master notebook runs all of the ingestion notebooks in sequence (not in parallel as to not overload the cores, although it is capable of doing parallel ingestion in this project).

<img width="480" alt="Data Ingestion Notebook" src="https://github.com/user-attachments/assets/481cdf65-3b2f-4152-806f-68907ee697c4">

The main steps of data ingestion for all files are similar:
1. Create schema using StructField and StructType. This approach seems to provide tidier code and easy to maintain once the schema evolves.
2. Read the raw file as PySpark Dataframe using the built schema. 
3. Perform small transformations to the data such as removing unuseful columns, renaming columns for consistency, and add *ingestion_date* column.
4. Write the processed data as silver managed tables using function to create new delta table or upsert the data into existing table. Managed table for silver layer data is decided to allow simplified management and built-in data governance features, such as data quality checks and lineage tracking.

Several points to point out:
1. The performed transformations in this phase is determined as relatively small transformations, so that it is okay to be done during ingestion phase.
2. The *configuration* notebook is executed in all ingestion notebooks to provide pre-defined variables of bronze, silver, and gold folder paths in the project's ADLS.
3. The *common_functions* notebook is executed in all ingestion notebooks to provide pre-built functions: adding *ingestion_date* and write data.
4. A parameter *file_date* must be provided to the Notebook widgets created with dbutils before executing the notebooks, which should match with folder name of the raw data. This parameter will be especially useful in the orchestration in Azure Data Factory.

<img width="640" alt="Data Ingestion Result" src="https://github.com/user-attachments/assets/d3b20868-cac3-489b-8276-fb872a2d42f6">


### Data Transformation
The transformation folder contains two transformation notebooks and one master notebook to run the two in sequence. 

Filter churned customer - using SQL semi-join to Status silver table to only get churned customer data from all tables excluding Population table (not required for the requirement).

<img width="640" alt="Transformation Notebook" src="https://github.com/user-attachments/assets/1d147aa7-309d-47f7-a710-647a55e9f246">

Intersect with county - using Geopandas (installed in cluster) to perform spatial join between the silver layer location table with county shapefile data.

<img width="640" alt="Geopandas installed in cluster" src="https://github.com/user-attachments/assets/af432519-9eaf-47d0-873b-066496db3968">

The result from this phase are tables into the gold layer, ready for presentation. 

<img width="640" alt="Geopandas installed in cluster" src="https://github.com/user-attachments/assets/29e05888-2ec3-416f-b36a-8f41b8120691">

## Azure Data Factory (ADF)
Each data ingestion and data transformation have their own pipeline. Each of those pipelines use master notebook to run.

<img width="260" alt="ADF Pipelines and Datasets" src="https://github.com/user-attachments/assets/5ecda3e0-63f6-4143-b4ee-1326c3e8e9bc">

One master pipeline run both in sequence: *Execute Ingestion --> Execute Transformation*. The setup is designed in a way to make transformation run after ingestion has completed. 

<img width="720" alt="ADF Pipelines Trigger" src="https://github.com/user-attachments/assets/b59a7f4e-4e93-496e-8903-66fff9c62830">

A Schedule Trigger *monthly_customer_churn* was set to run the master pipeline every 5th day of new month, during which the pipeline will look for folder in the bronze directory that are named with the date of the run e.g. *2024-09-05* or *2025-01-05*. The trigger takes off at 20:00 to ensure no other processes that might overlap.

<img width="720" alt="ADF Pipelines Trigger" src="https://github.com/user-attachments/assets/ff37f4f4-19b5-412b-819c-1b9b2a9ce635">

In data ingestion pipeline, the logic starts with using *Get Metadata* activity to read the trigger and pipeline parameter for the run date. An if-condition waits for that activity to be completed, and look into the project's ADLS for the aforementioned raw data folder and check whether a folder named with the run date exists. If exists, activity to run data ingestion master notebook will start, otherwise an activity to return 404 error will run. Meanwhile, transformation pipeline only contains activity to run transformation master notebook.

<img width="360" alt="ADF Ingestion Pipelines" src="https://github.com/user-attachments/assets/cbda79a4-7355-4d0b-a5c9-f417f07989c1">

<img width="240" alt="ADF Transformation Pipelines" src="https://github.com/user-attachments/assets/f7f377be-1bf2-4ee3-a351-8a22e8745974">

## Power BI
After tables are stored in gold layer from Azure Databricks, they are imported to Power BI.

<img width="360" alt="Power BI data import" src="https://github.com/user-attachments/assets/f7e1c30e-2f59-4ae7-a69f-14a9555e4e7f">

The import execution is done by providing Databricks Server Hostname, HTTP path, and Personal Access Token. All data is imported instead of using DirectQuery to lower cost considering the data is updated less frequently in this scenario (quarterly or monthly at best).
    
<img width="360" alt="Personal Acces Token" src="https://github.com/user-attachments/assets/d5422044-0f1f-46b8-94f0-989c926bc3c5">

The data is modelled using Star Schema, which is a relatively scalable and maintainable approach compared to normalized table.

<img width="360" alt="Star schema" src="https://github.com/user-attachments/assets/ee453f17-46aa-4970-aa1b-9e922fd1ec76">

Several modifications are shown in the Star Schema figure above.
1. Unpivotted Service Table - This table is result of unpivoting service_churn table based on all columns regarding service plan types e.g. Internet Type, Tech Support, and Streaming services.
2. Add several new measures using DAX Query: _used_region_ to show active region of the data using if statement; _latest_quarter_ to show the latest quarter the data is; _average_churn_mrr_ to show average MRR lost due to customer churn
 
<img width="1080" alt="Star schema" src="https://github.com/user-attachments/assets/afe32355-345e-4eda-9a32-2e2ae5f0e192">
<img width="1080" alt="Star schema" src="https://github.com/user-attachments/assets/5ecbc8ca-4cb1-4dc8-8f96-ccd58e254b86">
<img width="1080" alt="Star schema" src="https://github.com/user-attachments/assets/353b9f77-5c11-480f-8f91-ff782ae091db">

The visualization can be observed in below pictures.
<img width="1080" alt="Dashboard default" src="https://github.com/user-attachments/assets/e4e25cc9-7fa4-45ff-9712-4fae0de3225f">

Below shows the interactive filter when a region on the map is clicked; or a part of the chart or a table row is clicked
<img width="1080" alt="Dashboard with map clicked" src="https://github.com/user-attachments/assets/7067c31b-e037-4402-91d7-ea72f2d3e99d">
<img width="1080" alt="Dashboard with chart clicked" src="https://github.com/user-attachments/assets/dfdecb64-465a-460b-99e5-1eb276cd92be">
<img width="1080" alt="Dashboard with row table clicked" src="https://github.com/user-attachments/assets/212a95c0-dfae-49fa-92eb-46f8d913b54e">

Several considerations to point out:
1. California Shape Map uses custom TopoJSON file of California state that is stored locally, which is not a problem due to very rare update of the data. 
2. The distribution of churn reason, contract type, and demographic data (customer age under 30, gender, and marital status) use pie chart for clear visualization.
3. Service Plans' Clustered Column Chart use the unpivoted service table.
4. Zip code and Customer ID are shown in table so user can granularly check in each level.

This dashboard is a proposed answer to the requirements.

## Cost Analysis
This pipeline is built from September 8 - 24, which ultimately cost $14.30. The flat graph during September 8 - 17 was when author developed the main codebase in local Jupyterlab in Anaconda after creating and testing small code in the Databricks workspace; as well as creating the Power BI dashboard using the resulted local CSVs. The biggest costs comes from the Databrick Notebooks, which is where the processing and code troubleshooting takes place.

<img width="1080" alt="Cost Analysis" src="https://github.com/user-attachments/assets/d90e33b1-25c7-4900-b8fe-86ca08a7ddf3">

Several evaluations for lowering the cost are:
1. Using closer regional resources. This project was one of portfolio ideas I prepared during my time as Master student in Faculty of ITC, University of Twente, the Netherlands. UK south was a viable resource option during that time. Unfortunately, during this project was developed and tested, I stayed in Indonesia, which makes UK South resources expensive. Using closer resources such as Southeast Asia region might have drawback in terms of available resources to pick from, but it will lower the cost.
2. Perform fewer testing on master notebook and ADF. The project's most challenging part was on troubleshooting errors and unexpected results, which caused more cost.

## Conclusion
Using this data pipeline, when new data such as new quarterly data (e.g. Q4 data) comes (inside a folder named with 'YYYY-mm-dd' format of when the data will be extracted by ADF; stored in the bronze layer directory of the project's ADLS), ADF will be triggered automatically and the gold layer data will be updated.

In this project, Power BI data update is still manual by refreshing data within the Power BI itself. A suggestion to trigger Power BI data refresh from ADF is possible, but it requires premium tier of Power BI, which is not available in this project.

<img width="1080" alt="Power BI Refresh" src="https://github.com/user-attachments/assets/070315f0-ac88-4d0c-a1c9-0d3f530eb88f">

Thank you for reading this portfolio project. Improvement for suggestions are greatly appreciated!

_Regards,
Rifqi Alfadhillah Sentosa_
