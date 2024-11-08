# XYZ Bank: Transition to Cloud-Based Azure Storage

## Overview

XYZ Bank is transitioning from a traditional Datawarehouse to a centralized, cloud-based Azure storage solution. This shift involves transmitting incremental daily snapshots of data to Azure Data Lake Storage from On-Prem systems and other sources.

The Marketing System (MS) now needs to design a system to ingest these datasets daily from the centralized storage into their designated storage account. Necessary operations must be performed on the datasets to ensure they are readily accessible for dashboarding purposes.

## Objective

Calculate the balance of each account number after each transaction using the provided dataset. The dataset includes details of all the credits and debits made to accounts daily. The task involves reading the dataset and performing the necessary transformations using PySpark.

Steps to Implement
------------------

1.  **Set Up the Azure Environment**
    
    *   Create an Azure Data Lake Storage Gen2 account for centralized storage.
        
    *   Create containers and directories to organize data by date.
        
2.  **Data Ingestion Process**
    
    *   Set up mechanisms to upload daily snapshots from on-prem systems to Azure Data Lake Storage using Azure Data Factory.
        
3.  **Validate and Test the Transformation**
    
    *   Use the provided sample dataset to validate the script.
        
    *   Perform integration testing to ensure the system works end-to-end.
        
4.  **Dashboarding**
    
    *   Set up the Marketing System to access the transformed datasets.
        
    *   Use visualization tools like Power BI to create dashboards from the transformed data.

## Dataset Description

The dataset includes the following columns:
- **TransactionDate:** The date of the transaction
- **AccountNumber:** The unique identifier for the account
- **TransactionType:** The type of transaction (Credit or Debit)
- **Amount:** The amount of the transaction
- **CurrentBalance:** The balance of the account after the transaction

## Sample Data

```plaintext
TransactionDate  AccountNumber  TransactionType  Amount
20230101          100            Credit           1000
20230102          100            Credit           1500
20230103          100            Debit            1000
20230102          200            Credit           3500
20230103          200            Debit            2000
20230104          200            Credit           3500
20230113          300            Credit           4000
20230114          300            Debit            4500
20230115          300            Credit           1500
```
## Python Code

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("FinancialTransactionAnalysis").getOrCreate()

# Load the dataset
data = [
    ("20230101", 100, "Credit", 1000),
    ("20230102", 100, "Credit", 1500),
    ("20230103", 100, "Debit", 1000),
    ("20230102", 200, "Credit", 3500),
    ("20230103", 200, "Debit", 2000),
    ("20230104", 200, "Credit", 3500),
    ("20230113", 300, "Credit", 4000),
    ("20230114", 300, "Debit", 4500),
    ("20230115", 300, "Credit", 1500)
]

columns = ["TransactionDate", "AccountNumber", "TransactionType", "Amount"]

df = spark.createDataFrame(data, columns)

# Calculate the CurrentBalance
df = df.withColumn(
    "CurrentBalance",
    _sum(
        col("Amount").cast("double") * 
        (col("TransactionType") == "Credit").cast("double") - 
        col("Amount").cast("double") * 
        (col("TransactionType") == "Debit").cast("double")
    ).over(Window.partitionBy("AccountNumber").orderBy("TransactionDate").rowsBetween(Window.unboundedPreceding, Window.currentRow))
)

# Write the result to a CSV file
df.write.csv("path/to/output/final_balances.csv", header=True)
spark.stop()
```

## Results
After running the PySpark script, the dataset is transformed to include the current balance of each account number after each transaction.

## Transformed Data Output

```plaintext
TransactionDate  AccountNumber  TransactionType  Amount  CurrentBalance
20230101          100            Credit           1000     1000
20230102          100            Credit           1500     2500
20230103          100            Debit            1000     1500
20230102          200            Credit           3500     3500
20230103          200            Debit            2000     1500
20230104          200            Credit           3500     5000
20230113          300            Credit           4000     4000
20230114          300            Debit            4500     -500
20230115          300            Credit           1500     1000
```

