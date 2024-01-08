# Databricks notebook source
# MAGIC %run ./01-wordcount-streaming

# COMMAND ----------

import time

# COMMAND ----------

class WordCountSteamingTestSuite():
    def __init__(self):
        self.storage_name = "ini8databricks"
        self.container_name = "staging"
        self.base_test_file_path = "databricks-development/01-wordcount-streaming/tst-file-"
        self.folder_location = "SparkLearning/01-wordcount-streaming/"
    
    def clean_test_setup(self):
        '''
        Cleanup the folders and tables for testing.
        '''
        spark.sql("drop table if exists word_count_table_streaming")
        dbutils.fs.rm(f"abfss://{self.container_name}@{self.storage_name}.dfs.core.windows.net/{self.folder_location}InputFiles",True)
        #dbutils.fs.rm(f"abfss://{self.container_name}@{self.storage_name}.dfs.core.windows.net/{self.folder_location}/Checkpoint",True)
        print("Cleanup for execution of test cases completed")

    def ingest_test_data(self,itr):
        '''
        Ingest the test data to the inputted location.
        '''
        dbutils.fs.cp(f"abfss://testdata@{self.storage_name}.dfs.core.windows.net/{self.base_test_file_path}{itr}.txt",
                      f"abfss://{self.container_name}@{self.storage_name}.dfs.core.windows.net/{self.folder_location}InputFiles/tst-file-{itr}.txt")
        
    def assert_wordcount(self,expected_count):
        '''
        Assert the results.
        '''
        actual_count = spark.sql("select sum(count) from word_count_table_streaming where left(word,1 ) == 'a'").collect()[0][0]
        assert expected_count == actual_count, f"Test Failed!! Actual count is {actual_count}"

    def run_tests(self):
        '''
        Run the test cases
        '''
        wc = WordCountStreaming()
        squery = wc.update_word_count()
        # Running first test case 
        print("Testing first iteration of streaming word count ... \n")
        full_file_path = self.folder_location + "tst-file-01.txt"
        
        self.clean_test_setup()
        self.ingest_test_data("01")
        time.sleep(30)
        self.assert_wordcount(4)
        print("First Iteration of batch word count completed. \n")

        # Running second test case 
        print("Testing second iteration of batch word count ... \n")
        full_file_path = self.folder_location + "tst-file-02.txt"
        
        self.clean_test_setup()
        self.ingest_test_data("02")
        time.sleep(30)
        self.assert_wordcount(6)
        print("Second Iteration of batch word count completed. \n")

        squery.stop()
        

# COMMAND ----------

wct = WordCountSteamingTestSuite()
wct.run_tests()

# COMMAND ----------

actual_count = spark.sql("select sum(count) from word_count_table_streaming where left(word,1 ) == 'a'").collect()[0][0]

# COMMAND ----------

display(actual_count)

# COMMAND ----------

spark.sql("select * from word_count_table where left(word,1 ) == 'a'").show()

# COMMAND ----------

dbutils.fs.rm(f"abfss://staging@ini8databricks.dfs.core.windows.net//SparkLearning/01-wordcount-streaming/",recurse=True)

# COMMAND ----------

 
