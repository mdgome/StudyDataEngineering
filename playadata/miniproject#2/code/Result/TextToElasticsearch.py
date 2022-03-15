# %%writefile TextToElasticSearchUseSpark_ver0.2.py

import findspark
findspark.init() # find spark 
import datetime as dt

from pyspark.sql import SparkSession
import re
import findspark
import json
import pandas as pd 

from elasticsearch import Elasticsearch
from elasticsearch import helpers


# log data parsing 
def parsing(row):
    row_ = dict()
    ts_pattern = re.compile( r"\[(\d+-\d+-\d+) ((\d+:\d+:\d+),\d+)\]") 

    if "INFO" in row :
        row_["Timestamp"] = ts_pattern.match(row).group(1) +" "+ ts_pattern.match(row).group(3)
        row_["Status"] = "INFO" # Status Parsing 
        row_["Message"] = row.split("INFO - ")[1] # Message Parsing        
        
    elif "WARNING" in row :
        row_["Timestamp"] = ts_pattern.match(row).group(1) +" "+ ts_pattern.match(row).group(3)
        row_["Status"] = "WARNING" # Status Parsing 
        row_["Message"] = row.split("WARNING - ")[1] # Message Parsing 
        
        
    elif "ERROR" in row :
        row_["Timestamp"] = ts_pattern.match(row).group(1) +" "+ ts_pattern.match(row).group(3)
        row_["Status"] = "ERROR" # Status Parsing 
        row_["Message"] = row.split("ERROR - ")[1] # Message Parsing   
    
    return row_    

# Elastic Search Store  
def bulk_insert(host, port, df, index):
    es = Elasticsearch(host = host,port = port)

    data = [
      {
        "_index": index,
        "_source": {
            "datetime": x[0],
            "log-level": x[1],
            "message":x[2]}
      }
        for x in zip(df['Datetime'],df['Status'],df['Message'])
    ]

    helpers.bulk(es, data)
       

    ## 데이터 저장 확인 
    
    #index = "airflow_log"
    #body = "select * from airflow_log"

    #res = es.search(index=index)
    #print(res)

    
if __name__ == "__main__":
     
    spark = SparkSession.builder.master('local[2]').appName('airflow log test').getOrCreate()    
    data = spark.read.text("error_log_ex.txt") # change path
       
    dict_list = []
    ts_pattern = re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})]') # timestamp
    
    for index in range(len(data.collect()) - 1):
        row = data.collect()[index]['value']

        if ts_pattern.match(row):
             dict_list.append(parsing(row))
                
    df = pd.DataFrame(dict_list)   # pandas DataFrame 변경   
    df["Datetime"] = pd.to_datetime(df["Timestamp"],format="%Y-%m-%d %M:%H:%S", errors = 'coerce')  # datetime 변경 
 
    bulk_insert("localhost", "9200", df, "test")    # es에 데이터 적재 