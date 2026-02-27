# Databricks notebook source
# MAGIC %md #Customer Transaction ETL

# COMMAND ----------

from pyspark.sql.types import*
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC #Extract

# COMMAND ----------

data1 = [
    ('T001', 'C001', 'Rahul Sharma', '2024-01-05', 15000.00, 'Credit', 'Salary', 'Completed', 'Mumbai'),
    ('T002', 'C001', 'Rahul Sharma', '2024-01-10', 3500.00, 'Debit', 'Shopping', 'Completed', 'Mumbai'),
    ('T003', 'C002', 'Priya Patel', '2024-01-08', 25000.00, 'Credit', 'Business', 'Completed', 'Delhi'),
    ('T004', 'C002', 'Priya Patel', '2024-01-15', 8000.00, 'Debit', 'Food', 'Completed', 'Delhi'),
    ('T005', 'C003', 'Amit Singh', '2024-01-12', 12000.00, 'Credit', 'Salary', 'Completed', 'Bangalore'),
    ('T006', 'C003', 'Amit Singh', '2024-01-20', 2000.00, 'Debit', 'Utilities', 'Failed', 'Bangalore'),
    ('T007', 'C004', 'Sneha Reddy', '2024-02-01', 30000.00, 'Credit', 'Business', 'Completed', 'Hyderabad'),
    ('T008', 'C004', 'Sneha Reddy', '2024-02-05', 5000.00, 'Debit', 'Shopping', 'Pending', 'Hyderabad'),
    ('T009', 'C005', 'Vikram Mehta', '2024-02-10', 18000.00, 'Credit', 'Salary', 'Completed', 'Chennai'),
    ('T010', 'C005', 'Vikram Mehta', '2024-02-15', 4500.00, 'Debit', 'Food', 'Completed', 'Chennai'),
    ('T011', 'C006', 'Ananya Gupta', '2024-02-20', 22000.00, 'Credit', 'Salary', 'Completed', 'Pune'),
    ('T012', 'C006', 'Ananya Gupta', '2024-02-25', 6000.00, 'Debit', 'Shopping', 'Failed', 'Pune'),
    ('T013', 'C007', 'Rohit Kumar', '2024-03-01', 35000.00, 'Credit', 'Business', 'Completed', 'Mumbai'),
    ('T014', 'C007', 'Rohit Kumar', '2024-03-05', 9000.00, 'Debit', 'Utilities', 'Completed', 'Mumbai'),
    ('T015', 'C008', 'Deepa Nair', '2024-03-10', 14000.00, 'Credit', 'Salary', 'Completed', 'Bangalore'),
    ('T016', 'C008', 'Deepa Nair', '2024-03-15', 3000.00, 'Debit', 'Food', 'Pending', 'Bangalore'),
    ('T017', 'C009', 'Karan Shah', '2024-03-20', 28000.00, 'Credit', 'Business', 'Completed', 'Delhi'),
    ('T018', 'C009', 'Karan Shah', '2024-03-25', 7500.00, 'Debit', 'Shopping', 'Completed', 'Delhi'),
    ('T019', 'C010', 'Meera Joshi', '2024-03-28', 16000.00, 'Credit', 'Salary', 'Completed', 'Mumbai'),
    ('T020', 'C010', 'Meera Joshi', '2024-03-30', 4000.00, 'Debit', 'Food', 'Failed', 'Mumbai')
]

# COMMAND ----------

schema1 = StructType([
    StructField('transaction_id', StringType(), True),
    StructField('customer_id', StringType(), True),
    StructField('customer_name', StringType(), True),
    StructField('transaction_date', StringType(), True),
    StructField('amount',DoubleType(), True),
    StructField('transaction_type', StringType(), True),
    StructField('category', StringType(),True),
    StructField('status', StringType(),True),
    StructField('city', StringType(), True)
])

# COMMAND ----------

df1 = spark.createDataFrame(data1, schema1)

# COMMAND ----------

df1.display()

# COMMAND ----------

# MAGIC %md #Transform

# COMMAND ----------

df1 = df1.withColumn('transaction_date',col('transaction_date').cast(DateType()))

# COMMAND ----------

df1 = df1.filter(upper(col('status')) == 'COMPLETED')

# COMMAND ----------

df1 = df1.withColumn('transaction_flag',when(lower(col('transaction_type'))=='credit','Money In').otherwise('Money out'))

# COMMAND ----------

df1 = df1.withColumn('amount_category',when(col('amount')>=20000,'High')\
    .when((col('amount')>=10000) & (col('amount') < 20000), 'Medium')\
        .otherwise('Low'))
    

# COMMAND ----------

df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Load

# COMMAND ----------

# Load - Save as Delta table
df1.write.format('delta')\
    .mode('overwrite')\
    .saveAsTable('customer_transactions_clean')

# COMMAND ----------

