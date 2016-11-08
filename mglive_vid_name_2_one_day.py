# -*- coding:utf-8 -*-

"""
@author:csp
@function: 获取type=2时vid，liveid对应的name  表结构id,uid,name
@finished time :2016 11 4 15:11
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

###获取每天新增的type=2对应的vid
def mglive_vid_liveid_data_get(year,month,day):
    try:
        conn = psycopg2.connect(database="dm_pv_fact", user="product_readonly", password="SDjTty7202d7Dldfui", host="54.222.196.128",port="2345")
        sql="select vid,liveid  from mglive_hour_fact where type=2 and year=%s and month=%s and day=%s group by vid,liveid;"%(year,month,day)
        print "start to get mglive_vid_data_get_type_2"
        try:
            vid_data = pd.read_sql(sql,conn)
            print "get mglive_vid_data_get_type_2 data success"
        except:
            vid_data = pd.DataFrame()
            print "get mglive_vid_data_get_type_2 data  fails"
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[ 0 ], e.args[ 1 ])
    return vid_data


##从直播平台节目对应下max_user数据库下的user表查找对应的vid所对应的name
def name_get_from_max_user(vid, liveid):
    conn = MySQLdb.connect(host='101.200.97.131', user='max_user_ro', passwd='Max_user_ro', port=3306, db='max_user',charset='utf8')
    sql = "select id ,uid,nickName as name from user where id=%s and uid='%s';" % (vid, liveid)
    try:
        name_get_type2 = pd.read_sql(sql, conn)
    except:
        name_get_type2 = pd.DataFrame()
    return name_get_type2

# 检查信息
def write_checkinfo(message):
    file_path = '/root/hf/live_user/mglive_vid_name'  # 存放检验文本的目录
    if not os.path.exists(file_path):
        os.mkdir(file_path)
    file_name = '/root/hf/live_user/mglive_vid_name/mglive_vid_name_2_check.txt'  # 检验文本的名称
    f = open(file_name, 'a')
    print "start write checkfile"
    f.write(message + '\n')
    print "write checkfile success"
    f.close()

###将数据插入到表mglive_vid_name_1
def insert_vid_name(conn,cur,vid_name_list,date):
   error_path = '/root/hf/live_user/mglive_vid_name'  #
   if not os.path.exists(error_path):
       os.mkdir(error_path)
   insert_tag = 1  ###插入成功表示1
   try:
        sql = 'insert into mglive_vid_name_2 values(' + ','.join(map(lambda o: "%s", range(0, 3))) + ')'
        cur.executemany(sql,vid_name_list)
        conn.commit()
   except MySQLdb.Error, e:
        insert_tag = 0  ###插入失败表示0
        error_insertlog_path = '/root/hf/live_user/mglive_vid_name/mglive_vid_name_2_error_'+date+".txt"  # 存放插入错误的日志信息
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
        conn = MySQLdb.connect(host='10.100.3.64', user='hefang', passwd='NYH#dfjirD872C$d&ss', db='live_user',port=3306, charset='utf8')
        cur = conn.cursor()
        vid_liveid_data=mglive_vid_liveid_data_get(sql_year,sql_month,sql_day)
        length=len(vid_liveid_data)
        ##如果当天存在vid
        if length>0:
            ##遍历获得vid和liveid
            data_list = list()
            for i in range(length):
                vid_liveid_target = vid_liveid_data.loc[ i ]
                vid = vid_liveid_target[ 'vid' ]
                liveid = vid_liveid_target[ 'liveid' ]
                ###获得vid和liveid对应的的id和name
                id_name = name_get_from_max_user(vid,liveid)
                if len(id_name) > 0:
                    data_evergrow = list()
                    id_name_target = id_name.loc[ 0 ]
                    id = id_name_target[ 'id' ]
                    uid=id_name_target['uid']
                    name = id_name_target[ 'name' ]
                    data_evergrow.extend((id,uid,name))
                    data_list.append(data_evergrow)
                else:
                    info="%s have no find name for vid=%s  liveid=%s"%(insert_date,vid,liveid)
                    write_checkinfo(info)
            ###vid都查询完毕之后，执行多行插入
            insert_tag = insert_vid_name(conn,cur,data_list,insert_date)
            if insert_tag == 1:
                info = "%s data insert success  100.00%" % name_get_length
            else:
                info = "%s data insert fail   00.00%" % name_get_length
            ###写入插入是否成功的信息
            write_checkinfo(info)
        else:
                info="%s have no vid data" %insert_date
                write_checkinfo(info)
                print info
            ###关闭资源
        cur.close
        conn.close()
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[ 0 ], e.args[ 1 ])