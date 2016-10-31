#!/usr/local/Python2.7/bin/python
# -*- coding: utf-8 -*-

"""@author:hefang
@file: date_combine.py
@time: 2016/10/13 9:47
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
        result_1.append('combine_'+result[i])
    datestr = ','.join(result_1)
    return datestr
###获取弹幕数据
def barrage_data_get(date):
    conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,charset='utf8')
    conn.select_db('live_user')
    sql = "select date,uuid,count(1) as barrage_num from barrage where date='%s' GROUP BY date,uuid;"%date
    print 'start to get barrage daily data %s'%date
    try:
        barrage = pd.read_sql(sql,conn)
        print 'get barrage daily data %s success!'%date
    except:
        barrage = pd.DataFrame()
        print 'get barrage daily data %s fail!'%date
    return barrage
###获取送礼数据
def gift_data_get(date):
    conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,charset='utf8')
    conn.select_db('live_user')
    sql = "select date,from_uuid as uuid,count(1) as gift_num,sum(total_price) as gift_price from gift where date='%s' GROUP BY date,from_uuid;"%date
    print 'start to get gift daily data %s'%date
    try:
        gift = pd.read_sql(sql,conn)
        print 'get gift daily data %s success!'%date
    except:
        gift = pd.DataFrame()
        print 'get gift daily data %s fail!'%date
    return gift
###获取vv数据
def vv_data_get(date):
    conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,charset='utf8')
    conn.select_db('live_user')
    sql = "select date,uuid,count(1) as vv_num from vv where date = '%s' and uuid != '' group by date,uuid"%date
    try:
        vv = pd.read_sql(sql,conn)
        print 'get vv daily data %s success!'%date
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
        print 'get pay daily data %s success!'%date
    except:
        pay = pd.DataFrame()
        print 'get pay daily data %s fail!'%date
    return pay
###建立每日用户汇总表
def combine_daily_create(date):
    try:
        conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',db='live_user',port=3306,charset='utf8')
        cur=conn.cursor()
       # conn.select_db('live_user')
        cur.execute('drop table if exists combine_%s;'%date)
        cur.execute('create table combine_%s (`date` int(8),uuid varchar(50),vv_num int(8),barrage_num int(8),gift_num int(8),gift_price int(10),money int(10))ENGINE=MyISAM;'%date)
        cur.execute('alter table combine_%s add index combine_index_%s (`date`)'%(date,date))
        conn.commit()
        cur.close
        conn.close()
    except:
        pass
###更新每日用户总表及其子表
def combine_create(date_str):
    try:
        conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,db='live_user',charset='utf8')
        cur=conn.cursor()
        cur.execute('drop table if exists combine;')
        cur.execute('create table combine (`date` int(8),uuid varchar(50),vv_num int(8),barrage_num int(8),gift_num int(8),gift_price int(10),money int(10))ENGINE=MERGE;')
        cur.execute('alter table combine add index combine_index (`date`);')
        cur.execute('alter table combine union=(%s);'%date_str)
        cur.close
        conn.close()
        print 'combine is updated!'
    except:
        print 'combine update fail!'
###汇总每日用户数据，插入每日用户�?
def combine_daily_insert(conn,cur,date,combine_insert):
    error_path = '/root/hf/live_user/combine'  # 存放插入错误的日志信�?
    if not os.path.exists(error_path):
        os.mkdir(error_path)
    try:
        sql = 'insert into combine_'+date+' values('+','.join(map(lambda o: "%s",range(0,7)))+')'
        cur.executemany(sql,combine_insert)
        conn.commit()
    except MySQLdb.Error,e:
        error_insertlog_path = '/root/hf/live_user/combine/combine_error_' + date + ".txt"  # 存放插入错误的日志信�?
        f = open(error_insertlog_path, 'a')
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        f.write("Mysql Error %d: %s,%s" % ( e.args[0], e.args[1],combine_insert) + "\n")
        f.close()
        pass

#获取前一天的日期
def day_get(d):
    oneday = datetime.timedelta(days=1)
    day = d - oneday
    #date_end=datetime.date(2016,8,27)
    date_end=datetime.date(int(day.year),int(day.month),int(day.day))
    return date_end

if __name__ == '__main__':
    start_date = datetime.date(2016,7,17)###总表数据统计开始时�?
    # 获取当前时间
    d = datetime.datetime.now()
    end_date = day_get(d)  ##当前日期的前一�?
    #end_date = datetime.date(2016,7,19)###数据汇总插入时�?
    date = end_date.strftime('%Y%m%d')###本次数据插入时间格式�?
    combine_date_str = datelist(start_date,end_date)#获得日期列表
    barrage = barrage_data_get(date)
    gift = gift_data_get(date)
    vv = vv_data_get(date)
    pay = pay_data_get(date)
    print 'start to combine %s daily data!'%date
    data_all = vv.merge(barrage,how='outer',on=['uuid','date'])
    data_all = data_all.merge(gift,how='outer',on=['uuid','date'])
    data_all = data_all.merge(pay,how='left',on=['uuid','date'])###因为用户付费可以是观看点播后的付�?
    data_all = data_all.fillna(0)
    if len(data_all) > 0:
        print 'combine %s daily data success'%date
        print 'start insert daily %s data into database!'%date
        combine_daily_create(date)
        try:
            if len(data_all) > 0:
                conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,db='live_user',charset='utf8')
                cur=conn.cursor()
                length_list = 10000###�?0000行插入一次数�?
                length = len(data_all)###数据框长�?
                length_split = length/length_list+1###将数据分段，每一万行为一�?
                for i in tqdm(range(length_split)):
                    combine_insert = list()
                    if i < length_split - 1:###在每10000为一份时一次插10000�?
                        xrange_length = length_list
                    elif i == length_split - 1:###在最后的一份取剩下�?
                        xrange_length = length-length_list*i
                    for j in xrange(xrange_length):
                        data_everyrow = list()
                        j_loc = i * 10000 + j
                        combine_target = data_all.loc[j_loc]
                        try:
                            day = str(int(combine_target['date']))
                            uuid = combine_target['uuid']
                            vv_num = combine_target['vv_num']
                            barrage_num = combine_target['barrage_num']
                            gift_num = combine_target['gift_num']
                            gift_price = combine_target['gift_price']
                            money = combine_target['money']
                            data_everyrow.extend((day,uuid,vv_num,barrage_num,gift_num,gift_price,money))
                            combine_insert.append(data_everyrow)
                        except:
                            print combine_target
                    combine_daily_insert(conn,cur,date,combine_insert)
                cur.close()
                conn.close()
                print 'insert into combine daily %s data success!'%date
        except:
            print 'insert into combine daily %s data fail!'%date
        combine_create(date_str=combine_date_str)###更新当日的汇总表
    else:
        print 'combine %s daily data fail!'%date
        pass