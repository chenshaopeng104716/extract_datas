# -*- coding:utf-8 -*-

"""
@author:csp
@function: 获取type=3时vid对应的name  表结构id,name
@finished time :2016 11 4 11:11
"""
import zipfile
import os
import re
import csv
import codecs
import MySQLdb
import psycopg2
import datetime
import pandas as pd
import numpy as np
from tqdm import tqdm
import sys

###获取每天新增的type=3对应的vid
def mglive_vid_data_get(year,month,day):
    try:
        conn = psycopg2.connect(database="dm_pv_fact", user="product_readonly", password="SDjTty7202d7Dldfui", host="54.222.196.128",port="2345")
        sql="select vid from mglive_hour_fact where type=3 and year=%s and month=%s and day=%s group by vid;"%(year,month,day)
        print "start to get mglive_vid_data_get_type_3"
        try:
            vid_data = pd.read_sql(sql,conn)
            print "get mglive_vid_data_get_type_3 data success"
        except:
            vid_data = pd.DataFrame()
            print "get mglive_vid_data_get_type_3  data  fails"
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[ 0 ], e.args[ 1 ])
    return vid_data

###从芒果直播cms对应下max_live数据库下的live_room表查找对应的vid所对应的name
def  name_get_from_max_live(vid_str):
       try:
           conn = MySQLdb.connect(host='rr-2ze4q5fvgw96y5109o.mysql.rds.aliyuncs.com', user='readonly', passwd='MaxReadonly', port=3306, db='max_live', charset='utf8')
           sql = "select id as vid,name from live_room where id in(%s) "%vid_str
           print "start to get name_get_type3"
           try:
                name_get_type3=pd.read_sql(sql,conn)
                print "get name_get_type3 success"
           except:
               name_get_type3=pd.DataFrame()
               print "get name_get_type3 failure"
       except MySQLdb.Error,e:
           print "Mysql Error %d: %s" % (e.args[ 0 ], e.args[ 1 ])
       return name_get_type3

# 检查信息
def write_checkinfo(message):
    file_path = '/root/hf/live_user/mglive_vid_name'  # 存放检验文本的目录
    if not os.path.exists(file_path):
        os.mkdir(file_path)
    file_name = '/root/hf/live_user/mglive_vid_name/mglive_vid_name_3_check.txt'  # 检验文本的名称
    f = open(file_name, 'a')
    print "start write checkfile"
    f.write(message + '\n')
    print "write checkfile success"
    f.close()

###将数据插入到表mglive_vid_name_3
def insert_vid_name(conn,cur,vid_name_list,date):
   error_path = '/root/hf/live_user/mglive_vid_name'  #
   if not os.path.exists(error_path):
       os.mkdir(error_path)
   insert_tag = 1  ###插入成功表示1
   try:
        sql = 'insert into mglive_vid_name_3 values(' + ','.join(map(lambda o: "%s", range(0, 2))) + ')'
        cur.executemany(sql,vid_name_list)
        conn.commit()
   except MySQLdb.Error, e:
        insert_tag = 0  ###插入失败表示0
        error_insertlog_path = '/root/hf/live_user/mglive_vid_name/mglive_vid_name_3_error_'+date+".txt"  # 存放插入错误的日志信息
        f = open(error_insertlog_path, 'a')
        print "Mysql Error %d: %s" % (e.args[ 0 ], e.args[ 1 ])
        f.write("Mysql Error %d: %s,%s" % (e.args[ 0 ], e.args[ 1 ], vid_name_list) + "\n")
        f.close()
        pass
   return insert_tag

if __name__ == '__main__':
    # 获取当前时间
    d = datetime.datetime.now()
    oneday = datetime.timedelta(days=1)
    day = d - oneday
    insert_date = "%04d%02d%02d" %(day.year,day.month, day.day)
    print insert_date
    sql_day="%02d"  %day.day
    sql_month="%02d" %day.month
    sql_year="%04d" %day.year
    try:
        conn = MySQLdb.connect(host='10.100.3.64', user='hefang', passwd='NYH#dfjirD872C$d&ss', db='live_user', port=3306,charset='utf8')
        cur = conn.cursor()
        vid_data=mglive_vid_data_get(sql_year,sql_month,sql_day)
        length=len(vid_data)
        ##如果当天存在vid
        if length>0:
            vid_list=list()
            for i in range(len(vid_data)):
                vid_target = vid_data.loc[ i ]
                vid_list.append(vid_target[ 'vid'])
            vid_str = ','.join(vid_list)
            ##获取vid对应的name
            name_get= name_get_from_max_live(vid_str)
            ##获取数据的行数
            name_get_length=len(name_get)
            ##判断插入的数据行数是否大于0
            if name_get_length>0:
               vid_name_list=list()
               ##将获得vid和name加入到列表中
               for j in range(name_get_length):
                   data_everyrow = list()
                   vid_name_target = name_get.loc[ j ]
                   vid = vid_name_target[ 'vid' ]
                   name = vid_name_target[ 'name' ]
                   data_everyrow.extend((vid, name))
                   vid_name_list.append(data_everyrow)
               insert_tag=insert_vid_name(conn,cur,vid_name_list,insert_date)
               if insert_tag==1:
                   info="%s data insert success  100.00%"%name_get_length
               else:
                   info="%s data insert fail   00.00%"%name_get_length
               ###写入插入是否成功的信息
               write_checkinfo(info)
               print info
            else:
              info="%s have no find name for vid=%s"%(insert_date,vid_str)
              write_checkinfo(info)
              print info
        else:
            info="%s have no vid data" %insert_date
            write_checkinfo(info)
            print info
        ###关闭资源
        cur.close
        conn.close()
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[ 0 ], e.args[ 1 ])