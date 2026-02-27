# Customer Transaction ETL Pipeline 🏦

## Project Overview
An ETL (Extract, Transform, Load) pipeline built using PySpark on Databricks
that processes raw banking transaction data, applies business transformations,
and loads the cleaned data into a Delta table.

## Tech Stack
- **Platform:** Databricks
- **Language:** PySpark (Python)
- **File Format:** Delta

## ETL Pipeline

### Extract
- Created raw banking transaction data with 20 records
- Defined schema using StructType and StructField
- Columns: transaction_id, customer_id, customer_name, transaction_date,
  amount, transaction_type, category, status, city

### Transform
- Cast transaction_date from StringType to DateType
- Filtered only Completed transactions
- Added transaction_flag column (Money In / Money Out)
- Added amount_category column (High / Medium / Low)

### Load
- Saved the cleaned and transformed data as a Delta table
- Table name: customer_transactions_clean
- Write mode: Overwrite

## Key Transformations
| Transformation | Description |
|----------------|-------------|
| Date Casting | Converted transaction_date to DateType |
| Filtering | Kept only Completed transactions |
| transaction_flag | Credit = Money In, Debit = Money Out |
| amount_category | High >= 20000, Medium >= 10000, Low < 10000 |

## How to Run
1. Open Databricks and create a new notebook
2. Copy the code from `etl_pipeline.py`
3. Run all cells sequentially
4. Check the Delta table `customer_transactions_clean` in your catalog