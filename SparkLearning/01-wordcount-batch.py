# Databricks notebook source
# MAGIC %md
# MAGIC IMPORTING PACKAGES

# COMMAND ----------

from pyspark.sql.functions import explode, split, trim, lower

# COMMAND ----------

# MAGIC %md
# MAGIC MENTION NOTEBOOK PARAMETERS

# COMMAND ----------

file_location = "tstfolder/script_specification.txt"
container_name = "tst01"
storage_name = "ini8databricks"

# COMMAND ----------

def read_txt_files(self,storage_name, container_name, file_location):
        '''
        Return dataframe with each line in different row
        '''
        return (spark.read
                .format("text")
                .option("lineSep",".")
                .load(f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/{file_location}"))


# COMMAND ----------

def get_quality_words(self, rawData):
    '''
    Return a dataframe with quality words
    '''
    raw_words = rawData.select(explode(split(rawData.value," ")).alias("word"))
    quality_words = (raw_words.select(lower(trim(raw_words.word)).alias("word")) \
                        .where("word is not null") \
                        .where("word rlike '[a-z]'")) 
    
    return quality_words


# COMMAND ----------

def get_word_counts(self, quality_words):
    '''
    Return a dataframe with count of words in a document
    '''
    return ( quality_words.groupBy("word").count().alias("occurence") )


# COMMAND ----------

def write_data(self,word_counts):
    '''
    Write data to a delta table
    '''
    (word_counts.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable("word_count_table"))

    

# COMMAND ----------

def update_word_count(self):
    '''
    Calculate the word count and write to a delta table
    '''
    file_location = "tstfolder/script_specification.txt"
    container_name = "tst01"
    storage_name = "ini8databricks"

    raw_data = self.read_txt_files(storage_name, container_name, file_location)
    quality_words = self.quality_words(raw_data)
    word_counts = self.get_word_counts(quality_words)

    self.write_data(word_counts)
    print("Process Complete")

# COMMAND ----------

class WordCountbatch():

    def read_txt_files(self,storage_name, container_name, file_location):
        '''
        Return dataframe with each line in different row
        '''
        return (spark.read
                .format("text")
                .option("lineSep",".")
                .load(f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/{file_location}"))

    def get_quality_words(self, rawData):
        '''
        Return a dataframe with quality words
        '''
        raw_words = rawData.select(explode(split(rawData.value," ")).alias("word"))
        quality_words = (raw_words.select(lower(trim(raw_words.word)).alias("word")) \
                            .where("word is not null") \
                            .where("word rlike '[a-z]'")) 
        
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
        (word_counts.write
                    .format("delta")
                    .mode("overwrite")
                    .saveAsTable("word_count_table"))

    def update_word_count(self):
        '''
        Calculate the word count and write to a delta table
        '''
        file_location = "tstfolder/script_specification.txt"
        container_name = "tst01"
        storage_name = "ini8databricks"

        raw_data = self.read_txt_files(storage_name, container_name, file_location)
        quality_words = self.get_quality_words(raw_data)
        word_counts = self.get_word_counts(quality_words)

        self.write_data(word_counts)
        print("Process Complete")

# COMMAND ----------


