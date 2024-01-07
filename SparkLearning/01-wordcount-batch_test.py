# Databricks notebook source
folder_location = "SparkLearning/WordCount"
container_name = "staging"
storage_name = "ini8databricks"

# COMMAND ----------

spark.sql("drop table if exists word_count_table")
dbutils.fs.rm(f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/{folder_location}/",True)


# COMMAND ----------

class WordCountBatchTestSuite():
    def __init__(self):
        self.base_data_dir = ""
    
    def clean_test_setup(self):
        '''
        Cleanup the folders and tables for testing.
        '''
        spark.sql("drop table if exists word_count_table")
        dbutils.fs.rm(f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/{folder_location}/",True)
        print("Cleanup for execution of test cases completed")

    def ingest_test_data(self,itr):
        '''
        Ingest the test data to the inputted location.
        '''
        dbutils.fs.cp(f"abfss://testdata@{storage_name}.dfs.core.windows.net/databricks-development/01-wordcount-batch/tst-file-{itr}.txt",
                      f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/{folder_location}/")
        
    def assert_result(self,expected_count):
        '''
        Assert the results.
        '''
        actual_count = spark.sql("select sum(count) from word_count_table where left(word,1 ) == 'a'").collect()[0][0]
        assert expected_count == actual_count, f"Test Failed!! Actual count is {actual_count}"
