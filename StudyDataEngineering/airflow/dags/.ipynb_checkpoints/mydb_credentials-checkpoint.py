#!/usr/bin/env python
# coding: utf-8

aws_mysql_ec2_study_Driver = {
    "host" : "ec2-15-164-164-229.ap-northeast-2.compute.amazonaws.com"
    "user" : "mdgome"  # 본인 ID 사용
    "password" : "Rlawjdals1!"  # 본인 Password 사용
    "port" : "3306"
    "dbname" : "airflow"
    "charset":'utf8'
}


def aws_mysql_ec2_study_engine_string():
    host = "ec2-15-164-164-229.ap-northeast-2.compute.amazonaws.com"
    user = "mdgome"  # 본인 ID 사용
    password = "Rlawjdals1!"  # 본인 Password 사용
    port = 3306
    dbname = "airflow"
    charset='utf8'
    encoding='utf-8'
    mysql_url = "mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}?{charset}".format(user=user,password=password,host=host,port=port,dbname=dbname,charset=charset)
    
    return mysql_url

