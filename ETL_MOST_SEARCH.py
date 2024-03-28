import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.context import SparkContext
import pandas as pd
from pyspark.sql.window import Window
import datetime


def convert_to_datevalue(value):
	date_value = datetime.datetime.strptime(value,"%Y%m%d").date()
	return date_value
	
def date_range(start_date, end_date):
	date_list = []
	current_date = start_date
	while current_date <= end_date:
		date_list.append(current_date.strftime("%Y%m%d"))
		current_date += datetime.timedelta(days=1)
	return date_list
	
def genarate_date_range(from_date, to_date):
	from_date = convert_to_datevalue(from_date)
	to_date = convert_to_datevalue(to_date)	
	date_list = date_range(from_date, to_date)
	return date_list

def main_task(path, data_key, month):
    data = spark.read.parquet(path)
    print("-----------------------------------")
    print("Reading data from file")
    print("-----------------------------------")
	
    print("-----------------------------------")
    print("Path: " + path)
    print("-----------------------------------")
    
    print("-----------------------------------")
    ds = spark.read.parquet(path)
    print("Read data success")
    print("-----------------------------------")

    print("-----------------------------------")
    print("Processing data")
    print("-----------------------------------")
    data = data.select("user_id", "keyword")
    data = data.groupBy("user_id", "keyword").count()
    data = data.withColumnRenamed("count", "search_count").orderBy("search_count", ascending=False)
    window = Window.partitionBy("user_id").orderBy(col("search_count").desc())
    data = data.withColumn("rank", row_number().over(window))
    data = data.filter(col("rank") == 1)
    data = data.withColumnRenamed("keyword", "Most_Search")
    data = data.select(col('user_id'), col('Most_Search'))
    data = data.fillna({'Most_Search': 'trống','user_id': 'Chưa xác định'})

    data = data.join(data_key, data.Most_Search == data_key.Most_Search, how='left').drop(data_key.Most_Search)
    data = data.fillna({'Category': 'UNKNOWN'})
    data = data.withColumnRenamed("user_id", "User_ID").withColumnRenamed("Most_Search", "Most_Search_T"+ str(month)).withColumnRenamed("Category", "Category_T"+ str(month))

    return data

def unionDataProcess(listData, month):
    data_key = spark.read.csv("D:\\WORKSPACE\\DE\\study_de\\Practice\\Class7\\ETL_MOST_SEARCH\\key_search.csv", header=True)
    data = None
    for file_name in listData:
        path_new = path + '\\' + file_name
        processed_data = main_task(path_new, data_key, month)
        if data is None:
            data = processed_data
        else:
            data = data.unionAll(processed_data)

    return data

def processTrendingType(data):
    data = data.withColumn("Trending_Type", 
        when(
            col("Category_T6") == col("Category_T7"), "Unchanged"
        ).otherwise("Changed")
    ) 
    return data

def processPrevious(data):
    data = data.withColumn("Previous", 
        when(
            col("Category_T6") == col("Category_T7"), "Unchanged"
        ).otherwise(concat_ws(" - ",col("Category_T6"), col("Category_T7")))
    )
    return data

if __name__ == "__main__":
	
    spark = SparkSession.builder.config("spark.driver.memory", "8g").config("spark.jars.packages","com.mysql:mysql-connector-j:8.3.0").getOrCreate()

    path = "D:\\WORKSPACE\\DE\\study_de\\Big_Data\\Items Shared on 4-29-2023\\Dataset\\log_search"
	
    start_date_t6 = "20220601"
    end_date_t6 = "20220602"
	
    list_date_t6 = genarate_date_range(start_date_t6, end_date_t6)
    	
    start_date_t7 = "20220701"
    end_date_t7 = "20220702"
	
    list_date_t7 = genarate_date_range(start_date_t7, end_date_t7)

    print("-----------------------------------")
    print("Start processing data t6")
    print("-----------------------------------")
    
    final_result_6 = unionDataProcess(list_date_t6,6)

    print("-----------------------------------")
    print("Start processing data T7")
    print("-----------------------------------")
    
    final_result_7 = unionDataProcess(list_date_t7,7)

    print("-----------------------------------")
    print("Join data T6 & T7")
    print("-----------------------------------")
    final_data_result = final_result_6.join(final_result_7, on='User_ID', how='inner')
    final_data_result = final_data_result.cache()

    print("-----------------------------------")
    print("Processing Trending Type")
    print("-----------------------------------")
    final_data_result = processTrendingType(final_data_result)
 
    print("-----------------------------------")
    print("Processing Previous")
    print("-----------------------------------")
    final_data_result = processPrevious(final_data_result)
    final_data_result.show()

    print("-----------------------------------")
    print("Save data to database")
    print("-----------------------------------")
    url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + 'etl_search'
    driver = "com.mysql.cj.jdbc.Driver"
    user = 'root'
    password = '123456'
    final_data_result.write.format('jdbc').option('url',url).option('driver',driver).option('dbtable','ETL_SEARCH').option('user',user).option('password',password).mode('append').save()
   