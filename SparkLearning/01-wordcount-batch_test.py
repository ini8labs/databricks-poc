# Databricks notebook source
# MAGIC %run ./01-wordcount-batch

# COMMAND ----------

class WordCountBatchTestSuite():
    def __init__(self):
        self.storage_name = "ini8databricks"
        self.container_name = "staging"
        self.base_test_file_path = "databricks-development/01-wordcount-batch/tst-file-"
        self.folder_location = "SparkLearning/01-wordcount/"
    
    def clean_test_setup(self):
        '''
        Cleanup the folders and tables for testing.
        '''
        spark.sql("drop table if exists word_count_table")
        dbutils.fs.rm(f"abfss://{self.container_name}@{self.storage_name}.dfs.core.windows.net/{self.folder_location}/",True)
        print("Cleanup for execution of test cases completed")

    def ingest_test_data(self,itr):
        '''
        Ingest the test data to the inputted location.
        '''
        dbutils.fs.cp(f"abfss://testdata@{self.storage_name}.dfs.core.windows.net/{self.base_test_file_path}{itr}.txt",
                      f"abfss://{self.container_name}@{self.storage_name}.dfs.core.windows.net/{self.folder_location}tst-file-{itr}.txt")
        
    def assert_wordcount(self,expected_count):
        '''
        Assert the results.
        '''
        actual_count = spark.sql("select sum(count) from word_count_table where left(word,1 ) == 'a'").collect()[0][0]
        assert expected_count == actual_count, f"Test Failed!! Actual count is {actual_count}"

    def run_tests(self):
        '''
        Run the test cases
        '''
        wc = WordCountbatch()

        # Running first test case 
        print("Testing first iteration of batch word count ... \n")
        full_file_path = self.folder_location + "tst-file-01.txt"
        
        self.clean_test_setup()
        self.ingest_test_data("01")
        wc.update_word_count(self.storage_name, self.container_name, full_file_path)
        self.assert_wordcount(4)
        print("First Iteration of batch word count completed. \n")

        # Running second test case 
        print("Testing second iteration of batch word count ... \n")
        full_file_path = self.folder_location + "tst-file-02.txt"
        
        self.clean_test_setup()
        self.ingest_test_data("02")
        wc.update_word_count(self.storage_name, self.container_name, full_file_path)
        self.assert_wordcount(6)
        print("Second Iteration of batch word count completed. \n")
        

# COMMAND ----------

wct = WordCountBatchTestSuite()
wct.run_tests()

# COMMAND ----------

actual_count = spark.sql("select sum(count) from word_count_table where left(word,1 ) == 'a'").collect()[0][0]

# COMMAND ----------

display(actual_count)

# COMMAND ----------

spark.sql("select * from word_count_table where left(word,1 ) == 'a'").show()

# COMMAND ----------


