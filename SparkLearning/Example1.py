# Databricks notebook source
df = (spark.read
        .format("delta")
        .option("header","true")
        .option("inferschema","true")
        .load("dbfs:/databricks-datasets/nyctaxi-with-zipcodes/subsampled"))
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------


