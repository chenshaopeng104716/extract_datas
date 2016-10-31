#!/usr/local/Python2.7/bin/python
# -*- coding: utf-8 -*-

"""@author:csp
@file: perfect_vacation
@time: 2016/10/27
"""

import datetime
import MySQLdb
import os
import re
from tqdm import tqdm
import pandas as pd
import numpy as np
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

###获取从开始日期到现在的日期间�?
def datelist(start_date,end_date):
    result = []
    curr_date = start_date
    while curr_date != end_date:
        result.append("%04d%02d%02d" % (curr_date.year, curr_date.month, curr_date.day))
        curr_date += datetime.timedelta(1)
    result.append("%04d%02d%02d" % (curr_date.year, curr_date.month, curr_date.day))
    result_1 = list()
    for i in range(len(result)):
        result_1.append('perfect_vacation_'+result[i])
    datestr = ','.join(result_1)
    return datestr
###获取完美假期对应的弹幕数�?
def barrage_data_get(date):
    conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,charset='utf8')
    conn.select_db('live_user')
    sql = "select date,uuid,count(1) as barrage_num from barrage where date='%s' and activityId='1000064' GROUP BY date,uuid;"%date
    print 'start to get barrage daily data %s'%date
    try:
        barrage = pd.read_sql(sql,conn)
        print 'get barrage daily data %s success!,size %s'%(date,len(barrage))
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[ 0 ], e.args[ 1 ])
        barrage = pd.DataFrame()
        print 'get barrage daily data %s fail!'%date
    return barrage
###获取送礼数据
def gift_data_get(date):
    conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,charset='utf8')
    conn.select_db('live_user')
    sql = "select date,from_uuid as uuid,count(1) as gift_num,sum(total_price) as gift_price from gift where date='%s'and to_uuid in('2b2ada7db02a7cbf825cfec805d9e95c', " \
          "'52172a7981556a3db30edf96f626a6eb', 'a0315f1c38c4532362fb5c9ff7cd6855', " \
          "'de92e7fd8ca4c254d3c1155c2d959ea2', 'c79e9b085047f4b992bf0a1a5beab3fd', " \
          "'464d58838a673d145020067ec04362ac', '14b1c7efe06ac93649fe31fbc24883f8', 'd974ecb5bc3dda3f77a69dd44d3e23e3'," \
          " '28bdf64587ee2b056510fe4435d8d9b8', 'a3a53a85d93b92687a265727b1324305', '5dd361e76acd23d2b712a13ab517ff0c'," \
          " '6c1837a5930c9df754e9b7819ab66b37', 'ef32f5a59c16c959a7732b08f8aa872f', " \
          "'3335398a37ae11c3e4f2b71b152eda1b', '84c575653585ef3b5b34a552769d6f71', " \
          "'5993d176fc821b212436d449b8aca0f2', '7e526b601d6c7c8b0f7b8f4b6a4a7073'," \
          " '29ec0a2e22595132238de515e768f881', '9c35bd360435b1b59179f34986880736') GROUP BY date,from_uuid;"%date
    print 'start to get gift daily data %s'%date
    try:
        gift = pd.read_sql(sql,conn)
        print 'get gift daily data %s success! size %s'%(date,len(gift))
    except:
        gift = pd.DataFrame()
        print 'get gift daily data %s fail!'%date
    return gift
###获取vv数据
def vv_data_get(date):
    conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,charset='utf8')
    conn.select_db('live_user')
    sql = "select date,uuid,count(1) as vv_num from vv where date = '%s' and uuid != ''and activityId='1000064' group by date,uuid"%date
    try:
        vv = pd.read_sql(sql,conn)
        print 'get vv daily data %s success! size %s'%(date,len(vv))
    except:
        vv = pd.DataFrame()
        print 'get vv daily data %s fail!'%date
    return vv
###获取充值数�?
def pay_data_get(date):
    conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,charset='utf8')
    conn.select_db('live_user')
    sql = "select date,uuid,sum(money) as money from pay where date='%s' GROUP BY date,uuid;"%date
    print 'start to get pay daily data %s'%date
    try:
        pay = pd.read_sql(sql,conn)
        print 'get pay daily data %s success! size %s'%(date,len(pay))
    except:
        pay = pd.DataFrame()
        print 'get pay daily data %s fail!'%date
    return pay
###建立每日用户汇总表
def perfect_vacation_daily_create(date):
    try:
        conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,charset='utf8')
        cur=conn.cursor()
        conn.select_db('live_user')
        cur.execute('drop table if exists perfect_vacation_%s;'%date)
        cur.execute('create table perfect_vacation_%s (`date` int(8),uuid varchar(50),vv_num int(8),barrage_num int(8),gift_num int(8),gift_price int(10),money int(10))ENGINE=MyISAM;'%date)
        cur.execute('alter table perfect_vacation_%s add index perfect_vacation_index_%s (`date`)'%(date,date))
        conn.commit()
        cur.close
        conn.close()
    except:
        pass
###更新每日用户总表及其子表
def perfect_vacation_create(date_str):
    try:
        conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,db='live_user',charset='utf8')
        cur=conn.cursor()
        cur.execute('drop table if exists perfect_vacation;')
        cur.execute('create table perfect_vacation (`date` int(8),uuid varchar(50),vv_num int(8),barrage_num int(8),gift_num int(8),gift_price int(10),money int(10))ENGINE=MERGE;')
        cur.execute('alter table perfect_vacation add index perfect_vacation_index (`date`);')
        cur.execute('alter table perfect_vacation union=(%s);'%date_str)
        cur.close
        conn.close()
        print 'perfect_vacation is updated!'
    except:
        print 'perfect_vacation update fail!'
###汇总每日用户数据，插入每日用户�?
def perfect_vacation_daily_insert(conn,cur,date,perfect_vacation_insert):
    error_path = '/root/hf/live_user/perfect_vacation'  # 存放插入错误的日志信�?
    if not os.path.exists(error_path):
        os.mkdir(error_path)
    try:
        sql = 'insert into perfect_vacation_'+date+' values('+','.join(map(lambda o: "%s",range(0,7)))+')'
        cur.executemany(sql,perfect_vacation_insert)
        conn.commit()
    except MySQLdb.Error,e:
        error_insertlog_path = '/root/hf/live_user/perfect_vacation/perfect_vacation_error_' + date + ".txt"  # 存放插入错误的日志信�?
        f = open(error_insertlog_path, 'a')
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        f.write("Mysql Error %d: %s,%s" % ( e.args[0], e.args[1],perfect_vacation_insert) + "\n")
        f.close()
        pass

##获取完美假期中人物的uuid
def get_actor_uuid():
   try:
     conn = MySQLdb.connect(host='10.100.3.64', user='hefang', passwd='NYH#dfjirD872C$d&ss', port=3306,  db='live_user', charset='utf8')
     cur = conn.cursor()
     cur.execute("select uuid  as id from actor;")
     result=cur.fetchall()
     actor_list=[]
     for i in range(len(result)):
         actor_list.append(result[i][0])
     ##关闭资源
     cur.close
     conn.close()
     print "get actor uuid"
     return actor_list
   except:
       print "get actor failure"

if __name__ == '__main__':
    start_date = datetime.date(2016,7,17)###总表数据统计开始时�?
    end_date = datetime.date(2016,7,17)###数据汇总插入时�?
    date = end_date.strftime('%Y%m%d')###本次数据插入时间格式�?
    perfect_vacation_date_str = datelist(start_date,end_date)#获得日期列表
    barrage = barrage_data_get(date)
    gift = gift_data_get(date)
    vv = vv_data_get(date)
    pay = pay_data_get(date)
    print 'start to perfect_vacation %s daily data!'%date
    data_all = vv.merge(barrage,how='outer',on=['uuid','date'])
    data_all = data_all.merge(gift,how='outer',on=['uuid','date'])
    data_all = data_all.merge(pay,how='left',on=['uuid','date'])###因为用户付费可以是观看点播后的付�?
    data_all = data_all.fillna(0)
    if len(data_all) > 0:
        print 'perfect_vacation %s daily data success'%date
        print 'start insert daily %s data into database!'%date
        perfect_vacation_daily_create(date)
        try:
            if len(data_all) > 0:
                conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,db='live_user',charset='utf8')
                cur=conn.cursor()
                length_list = 10000###�?0000行插入一次数�?
                length = len(data_all)###数据框长�?
                length_split = length/length_list+1###将数据分段，每一万行为一�?
                for i in tqdm(range(length_split)):
                    perfect_vacation_insert = list()
                    print i
                    if i < length_split - 1:###在每10000为一份时一次插10000�?
                        xrange_length = length_list
                    elif i == length_split - 1:###在最后的一份取剩下�?
                        xrange_length = length-length_list*i
                    for j in xrange(xrange_length):
                        data_everyrow = list()
                        j_loc = i * 10000 + j
                        perfect_vacation_target = data_all.loc[j_loc]
                        try:
                            day = str(int(perfect_vacation_target['date']))
                            uuid = perfect_vacation_target['uuid']
                            vv_num = perfect_vacation_target['vv_num']
                            barrage_num = perfect_vacation_target['barrage_num']
                            gift_num = perfect_vacation_target['gift_num']
                            gift_price = perfect_vacation_target['gift_price']
                            money = perfect_vacation_target['money']
                            data_everyrow.extend((day,uuid,vv_num,barrage_num,gift_num,gift_price,money))
                            perfect_vacation_insert.append(data_everyrow)
                        except:
                            print perfect_vacation_target
                    perfect_vacation_daily_insert(conn,cur,date,perfect_vacation_insert)
                cur.close()
                conn.close()
                print 'insert into perfect_vacation daily %s data success!'%date
        except:
            print 'insert into perfect_vacation daily %s data fail!'%date
        perfect_vacation_create(date_str=perfect_vacation_date_str)###更新当日的汇总表
    else:
        print 'perfect_vacation %s daily data fail!'%date
        pass