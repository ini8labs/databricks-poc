# Databricks notebook source
# Add the local variables for the notebook
filepath = "NewFiles/TestFile.csv"

# COMMAND ----------

df = (spark.read        
        .format("delta")
        .option("header","true")
        .option("inferschema","true")
        .load(f"dbfs:/databricks-datasets/nyctaxi-with-zipcodes/subsampled"))
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.genxxstorage.dfs.core.windows.net",
    "lPDkg1/ocAngnmWFOfmIaH9qd/ZyHWKOzMrDyu1nCfeI7YC9DoJdY7uUs2Um+5goaJ0yvOtpJhGb+AStwZGcIw=="
)

df = spark.read.format("csv").load(f"abfss://tstcontainer-gourav@genxxstorage.dfs.core.windows.net/SparkLearning/orders.csv")
display(df)

# COMMAND ----------



# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.ini8databricks.dfs.core.windows.net",
    "L3pKE8uexZhdLjAwYMKc7i/bxGq8ClkMsMWkvR8a0SiePZW69IjOZGmYKATVEkBdE3i8xxtCog0c+AStfBfizA=="
)

df = ( spark.read.format("csv")
                    .option("inferschema","true")
                    .option("header","true")
                    .load(f"abfss://tst01@ini8databricks.dfs.core.windows.net/tstfolder/orders.csv") )
display(df)

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.ini8databricks.dfs.core.windows.net",
    "L3pKE8uexZhdLjAwYMKc7i/bxGq8ClkMsMWkvR8a0SiePZW69IjOZGmYKATVEkBdE3i8xxtCog0c+AStfBfizA=="
)
display(spark.read.format("text")
                    .load(f"abfss://tst01@ini8databricks.dfs.core.windows.net/tstfolder/Vendor_file2.txt") )

# COMMAND ----------


