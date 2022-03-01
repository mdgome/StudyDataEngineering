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
    import os
    import configparser
    from pathlib import Path
    sys.path.append(os.getcwd())
    
    config = configparser.ConfigParser()
    config.read(Path("./config/config.ini"),encoding='utf-8')
    # config.read(os.getcwd()+os.sep+'config'+os.sep+'config.ini',encoding='utf-8')

    user = config['aws_ec2_mysql']['user']
    password = config['aws_ec2_mysql']['password']
    host = config['aws_ec2_mysql']['host']
    port = config['aws_ec2_mysql']['port']
    dbname = config['aws_ec2_mysql']['dbname']
    charset = config['aws_ec2_mysql']['charset']

    try:
        conn = pymysql.connect(
            host=host,
            port=int(port),
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
    from pathlib import Path
    now_date = datetime.now().strftime('%Y.%m.%d.%H.%M')
    file_name = now_date+"_output.csv"
    mkdirPath = Path("./data/mysqlData")
    mkdirPath.mkdir(parents=True, exist_ok=True)
    df.to_csv(mkdirPath/file_name,index=False)

def etl():
    df = load()
    DataToCSV(df)

dag_second_assingnment = DAG(
    dag_id='second_assingnment',
    catchup = False,
    start_date = datetime(2022,1,14),
    schedule_interval = '0 * * * *'
)

task = PythonOperator(
    task_id = 'MySQLTOCsv',
    python_callable = etl,
    dag = dag_second_assingnment
)


