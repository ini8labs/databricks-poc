# Databricks notebook source
# MAGIC %md
# MAGIC IMPORTING PACKAGES

# COMMAND ----------

from pyspark.sql.functions import explode, split, trim, lower

# COMMAND ----------

spark.conf.set("fs.azure.account.key.ini8databricks.dfs.core.windows.net","L3pKE8uexZhdLjAwYMKc7i/bxGq8ClkMsMWkvR8a0SiePZW69IjOZGmYKATVEkBdE3i8xxtCog0c+AStfBfizA==")

# COMMAND ----------

class WordCountStreaming:
    def __init__(self):
        self.storage_name = "ini8databricks"
        self.container_name = "staging"
        self.base_path = "abfss://staging@ini8databricks.dfs.core.windows.net/SparkLearning/"

    def read_txt_files(self):
        '''
        Return dataframe with each line in a different row
        '''
        
        return (spark.readStream
                .format("text")
                .option("lineSep",".")
                .load("f{self.base_path}01-wordcount-streaming/InputFiles"))

    def get_quality_words(self, rawData):
        '''
        Return a dataframe with quality words
        '''
        raw_words = rawData.select(explode(split(rawData.value," ")).alias("word"))
        quality_words = (raw_words.select(lower(trim(raw_words.word)).alias("word")) \
                            .where("word is not null") \
                            .where("word rlike '[a-z]'")) 
        print(quality_words)
        return quality_words

    def get_word_counts(self, quality_words):
        '''
        Return a dataframe with count of words in a document
        '''
        return (quality_words.groupBy("word").count().alias("occurence"))
    
    def write_data(self,word_counts):
        '''
        Write data to a delta table
        '''
        return  (word_counts.writeStream
                    .format("delta")
                    .option("checkpointLocation",f"{self.base_path}01-wordcount-streaming/Checkpoint")
                    .outputMode("complete")
                    .toTable("word_count_table_streaming")
                )

    def update_word_count(self):
        '''
        Calculate the word count and write to a delta table
        '''

        raw_data = self.read_txt_files()
        quality_words = self.get_quality_words(raw_data)
        word_counts = self.get_word_counts(quality_words)

        streaming_query = self.write_data(word_counts)
        print("Process Complete")
        return streaming_query

# COMMAND ----------

wcStreaming = WordCountStreaming()
sQuery = wcStreaming.update_word_count()

# COMMAND ----------

sQuery.stop()

# COMMAND ----------

storage_name = "ini8databricks"
container_name = "staging"
base_path = "abfss://staging@ini8databricks.dfs.core.windows.net/SparkLearning/"
print(f"{base_path}01-wordcount-streaming/Checkpoint")

# COMMAND ----------

dbutils

# COMMAND ----------


