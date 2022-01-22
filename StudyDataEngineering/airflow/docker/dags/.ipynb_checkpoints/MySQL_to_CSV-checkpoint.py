#!/usr/bin/env python
# coding: utf-8

# import general package
from datetime import datetime
import logging
import sys
# import Transform
import pandas as pd
from pandas import DataFrame
# import mysql.connector
from sqlalchemy import create_engine
import pymysql
# import airflow package
from airflow import DAG
from airflow.operators.python import PythonOperator


def _get_MySQL_Connection():
    #import os
    #sys.path.append(os.getcwd())    
    # from mydb_credentials import aws_mysql_ec2_study_Driver
    # from mydb_credentials import *

    host = "ec2-15-164-164-229.ap-northeast-2.compute.amazonaws.com"
    user = "mdgome"  # 본인 ID 사용
    password = "Rlawjdals1!"  # 본인 Password 사용
    port = 3306
    dbname = "airflow"
    charset='utf8'
    encoding='utf-8'
    # user = aws_mysql_ec2_study_Driver['user']
    # password = aws_mysql_ec2_study_Driver['password']
    # host = aws_mysql_ec2_study_Driver['host']
    # port = aws_mysql_ec2_study_Driver['port']
    # dbname = aws_mysql_ec2_study_Driver['dbname']
    # charset = aws_mysql_ec2_study_Driver['charset']

    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            db=dbname,
            charset=charset
        )
        logging.info('DB Connect : ',datetime.now().strftime('%Y.%m.%d.%H.%M') )
        return conn.cursor()
    except Exception as e:
        logging.error('DB Connect Error : ',datetime.now().strftime('%Y.%m.%d.%H.%M') )
        logging.error(e.message)
        sys.exit(5)

def DataToCSV(df):
    import os
    
    path = os.getcwd()
    mkdir_path = path + "/data/mysqlData"
    if not os.path.exists(mkdir_path):
        os.mkdir(mkdir_path)
    
    now_date = datetime.now().strftime('%Y.%m.%d_%H.%M')
    file_name = now_date+"_output.csv"
    output_file = mkdir_path+"/"+file_name
    
    df.to_csv(output_file,index=False)
    if os.path.isfile(output_file) :
        logging.info('File Created: ', output_file)
def load():
    cur = _get_MySQL_Connection()
    sql = "select name, email from member;"
    cur.execute(sql)
    result = cur.fetchall()
    df = DataFrame(result)    
    
    return df

def etl():
    df = load()
    DataToCSV(df)

dag_second_assingnment = DAG(
    dag_id='MySQLTOCsv',
    catchup = False,
    start_date = datetime(2022,1,19),
    schedule_interval = '20 * * * *'
)

task = PythonOperator(
    task_id = 'MySQLTOCsv',
    python_callable = etl,
    dag = dag_second_assingnment
)


