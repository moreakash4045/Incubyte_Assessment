# Incubyte_Assessment




Hospital ETL Process
Overview
This project is designed to perform Extract, Transform, Load (ETL) operations for a multi-specialty hospital's customer data. The primary aim is to ingest customer data into a staging table, process it, and then store the relevant information into a target table while ensuring data validation.

Technology Stack
Apache Spark: For processing data using PySpark.
Azure Databricks: As the cloud-based platform for running Spark jobs.
SQL: For creating and managing tables.
Project Structure
SQL Scripts:




Create and manage staging and target tables.
Python Scripts:
Load data into staging tables.
Transform data to derive additional fields.
Validate data integrity before loading into the target table.
Table Definitions
Staging_Customers
This table holds customer data temporarily before processing.






Column Name	Data Type
Customer_Name	VARCHAR(255)
Customer_Id	VARCHAR(18)
Open_Date	DATE
Last_Consulted_Date	DATE
Vaccination_Id	CHAR(5)
Dr_Name	VARCHAR(255)
State	CHAR(5)
Country	CHAR(5)
DOB	DATE
Is_Active	CHAR(1)
Table_India

This table holds processed data for customers from India.

Column Name	Data Type
Name	VARCHAR(255)
Cust_I	VARCHAR(18)
Open_Dt	DATE
Consul_Dt	DATE
VAC_ID	CHAR(5)
DR_Name	VARCHAR(255)
State	CHAR(5)
Country	CHAR(5)
DOB	DATE
FLAG	CHAR(1)
Age	INT
Days_Since_Last_Consulted	INT










ETL Process
Step 1: Data Ingestion
Drop the existing Staging_Customers table if it exists.
Create a new staging table.
Insert sample customer data into the Staging_Customers table.
Step 2: Data Transformation
Load data from the Staging_Customers table.
Derive the Age and Days_Since_Last_Consulted columns.
Filter for customers from India and prepare the DataFrame for loading into the target table.
Step 3: Data Loading
Load the processed DataFrame into the Table_India.
Step 4: Data Validation
Implement validation checks to ensure mandatory fields are present and date formats are valid.







Usage Instructions
Set up your Azure Databricks environment.
Run the SQL scripts to create the necessary tables.
Execute the Python ETL script to process the data.
Review logs and validation messages to ensure data integrity.
Conclusion
This ETL process facilitates the efficient management of customer data for the hospital chain, ensuring accurate and timely access to critical information.
