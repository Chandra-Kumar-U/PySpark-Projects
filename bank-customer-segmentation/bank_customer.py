# Databricks notebook source
# MAGIC %md #Bank Customer Segmentation

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.format('csv')\
    .option('inferSchema',True)\
        .option('header',True)\
            .load('/Volumes/workspace/learning/my_volume/bank_customers.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('customer_segment',when(col('balance')>=400000,'Premium')\
    .when(((col('balance')>=150000)&(col('balance')<400000)),'Regular')\
        .otherwise('Basic'))

# COMMAND ----------

df = df.withColumn('savings_rate',round(((col('monthly_income') - col('monthly_expenses')) / col('monthly_income')) * 100, 2))

# COMMAND ----------

df = df.withColumn('credit_category', when(col('credit_score')>800,'Excellent')\
                   .when((col('credit_score')>700) & (col('credit_score')<=800),'Good')\
                   .when((col('credit_score')>600) & (col('credit_score')<=700),'Fair')\
                       .otherwise('Poor'))

# COMMAND ----------

df_segment = df.groupBy('customer_segment').agg(
    count('customer_id').alias('total_customers'),
    avg('balance').alias('avg_balance'),
    avg('credit_score').alias('avg_credit_score'),
    avg('savings_rate').alias('avg_savings_rate')
)
df_segment.display()

# COMMAND ----------

df = df.withColumn('rnk', rank().over(Window.partitionBy(col('customer_segment')).orderBy(col('balance').desc())))

# COMMAND ----------

df.display()

# COMMAND ----------

df.createTempView('customer_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT city, 
# MAGIC        avg(balance) as avg_balance, 
# MAGIC        avg(credit_score) as avg_credit_score,
# MAGIC        count(customer_id) as total_customers
# MAGIC FROM customer_data 
# MAGIC GROUP BY city
# MAGIC ORDER BY avg_balance DESC;

# COMMAND ----------

df.write.format('delta')\
    .mode('overwrite')\
    .saveAsTable('bank_customer_segments')

df_segment.write.format('delta')\
    .mode('overwrite')\
    .saveAsTable('customer_segment_summary')

# COMMAND ----------

