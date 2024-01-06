# Databricks notebook source
file_location = "tstfolder/script_specification.txt"
container_name = "tst01"
storagename = "ini8databricks"

# COMMAND ----------

lines = (spark.read
        .format("text")
        .option("lineSep",".")
        .load(f"abfss://{container_name}@{storagename}.dfs.core.windows.net/{file_location}"))

# COMMAND ----------

from pyspark.sql.functions import explode, split, trim, lower
raw_words = lines.select(explode(split(lines.value," ")).alias("word"))
quality_words = (raw_words.select(lower(trim(raw_words.word)).alias("word"))
                    .where("word is not null")
                    .where("word rlike '[a-z]'"))
wordCounts = ( quality_words.groupBy("word").count().alias("occurence") )
display(wordCounts)


# COMMAND ----------

display(lines)

# COMMAND ----------


