# CodingAssignment
Coding Assignment for XYZ Bank 

Step 1: Set Up the Azure Environment
Create an Azure Storage Account:

Set up an Azure Data Lake Storage Gen2 account for centralized storage.

Create Containers and Directories:

Create containers in the storage account to hold the daily snapshots of data.

Create directories within the containers to organize the data by date.

Step 2: Data Ingestion Process
Daily Data Upload:

Set up mechanisms to upload daily snapshots from on-prem systems to Azure Data Lake Storage.

Use Azure Data Factory to automate and schedule the data transfer process.

Step 3: Design the PySpark Script for Data Transformation
Here is the PySpark script to calculate the balance of each account number after each transaction:
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

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


Step 4: Validate and Test the Transformation
Test Data:

Use the provided sample dataset to validate the script and ensure it calculates the balances correctly.

Integration Testing:

Test the entire workflow from data ingestion to final CSV output to ensure the system works end-to-end.

Step 5: Dashboarding
Accessing Data:

Once the data is transformed and stored in Azure Data Lake Storage, set up the Marketing System to access these datasets.

Visualization Tools:

Use Power BI or other visualization tools to create dashboards from the transformed data.

This approach ensures that the data is ingested, processed, and made available for dashboarding in a scalable and efficient manner.
