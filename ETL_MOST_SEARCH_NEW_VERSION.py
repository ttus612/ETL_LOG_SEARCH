import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.context import SparkContext
import pandas as pd
from pyspark.sql.window import Window
import datetime

spark = SparkSession.builder.config("spark.driver.memory", "8g").config("spark.jars.packages","com.mysql:mysql-connector-j:8.3.0").getOrCreate()

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

def read_data_parquet(path):
    data = spark.read.parquet(path)
    return data

def process_data(path, data_key, month):
    print("Path: " + path)
    print("------------     => Read data--------------")
    data = read_data_parquet(path)
    print("------------     => Processing data--------------")
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

def mappingAndUnionDataProcess(path, listData, month):
    data_key = spark.read.csv("D:\\WORKSPACE\DE\\study_de\\Practice\\Class7\\ETL_MOST_SEARCH\\key_search.csv", header=True)
    data = None
    for file_name in listData:
        path_new = path + '\\' + file_name
        processed_data = process_data(path_new, data_key, month)
        if data is None:
            data = processed_data
        else:
            data = data.unionAll(processed_data)

    return data

def join_Data_Process(data_6, data_t7):
    data = data_6.join(data_t7, on='User_ID', how='inner')
    data = data.cache()
    return data

def processTrendingType(df):
    data = df.withColumn("Trending_Type", 
        when(
            col("Category_T6") == col("Category_T7"), "Unchanged"
        ).otherwise("Changed")
    ) 
    return data

def processPrevious(df):
    data = df.withColumn("Previous", 
        when(
            col("Category_T6") == col("Category_T7"), "Unchanged"
        ).otherwise(concat_ws(" - ",col("Category_T6"), col("Category_T7")))
    )
    return data

def write_file_csv(df):
    save_path = "D:\\WORKSPACE\\DE\\study_de\\Practice\\Class7\\Storage\\DataMostSearch"
    df.repartition(1).write.csv(save_path, header=True)
    print("Data have been written file csv")


def save_data_into_db(df):
    url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + 'etl_db'
    driver = "com.mysql.cj.jdbc.Driver"
    user = 'root'
    password = 'sapassword'
    df.write.format('jdbc').option('url',url).option('driver',driver).option('dbtable','etl_log_search').option('user',user).option('password',password).mode('append').save()
    print("Data has been saved into DB")


def main_task(path, list_t6, month_6, list_date_t7, month_7):
    print("------------ 1. Process data month 6--------------")
    print("------------ => 1.1. Get list month 6--------------")
    print(list_t6)
    print("------------ => 1.2. Mapping and Union Data Process month 6--------------")
    data_6 = mappingAndUnionDataProcess(path, list_t6, month_6)  
    data_6.show()
    print("------------ 2. Process data month 7--------------")
  
    print("------------ => 2.1. Get list month 7--------------")
    print(list_date_t7)
    print("------------ => 2.2. Mapping and Union Data Process month 7--------------")   
    data_t7 = mappingAndUnionDataProcess(path, list_date_t7, month_7)
    data_t7.show()

    print("------------ 3. Join data month 6 and month 7--------------")
    data = join_Data_Process(data_6, data_t7)
    print("------------ 4. Process Trending Type--------------")
    data = processTrendingType(data)
    print("------------ 5. Process Previous--------------")
    data = processPrevious(data)
    data.show()
    print("------------ 6. Save data--------------")
    write_file_csv(data)
    save_data_into_db(data)


path = "D:\\WORKSPACE\\DE\\study_de\\Big_Data\\Items Shared on 4-29-2023\\Dataset\\log_search"	
start_date_t6 = "20220601"
end_date_t6 = "20220614"
list_date_t6 = genarate_date_range(start_date_t6, end_date_t6)
start_date_t7 = "20220701"
end_date_t7 = "20220714"
list_date_t7 = genarate_date_range(start_date_t7, end_date_t7)
main_task(path, list_date_t6, 6, list_date_t7, 7)


    	
	