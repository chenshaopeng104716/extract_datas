#!/usr/local/Python2.7/bin/python
# -*- coding: utf-8 -*-
'''
this module process daily barrage data
'''
import datetime
import MySQLdb
import os
import re
import psycopg2
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
        result_1.append('barrage_'+result[i])
    datestr = ','.join(result_1)
    return datestr
###获取弹幕数据
def barrage_get(date):
    print 'start to get daily barrage %s data!'%date
    conn = MySQLdb.connect(host="10.100.5.41",user="guozhengying",passwd="guozhengying1234",db="barrage_hisbk",charset="utf8",port=3307)
    sql = "select FROM_UNIXTIME(create_time,'%%Y%%m%%d%%H%%i') as time,device as bid_name,video_id as channelId,uuid,barrage_content as barrage,ip from barrage_audit " \
          "where FROM_UNIXTIME(create_time,'%%Y%%m%%d') = '%s'"%date
    try:
        barrage_data = pd.read_sql(sql,conn)
    except:
        barrage_data = pd.DataFrame()
    if len(barrage_data) == 0:
        print 'get daily barrage %s data fail!'%date
    else:
        print 'daily barrage data get!'
        print 'daily barrage %s is %s'%(date,len(barrage_data))
    return barrage_data
###建立当天的弹幕明细表
def barrage_daily_create(date):
    try:
        conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,charset='utf8')
        cur=conn.cursor()
        conn.select_db('live_user')
        cur.execute('drop table if exists barrage_%s;'%date)
        cur.execute('create table barrage_%s (`date` int(8),`bid` varchar(5),hour int(2),minute int(2),activityId varchar(50),'\
                    'channelId varchar(50),uuid varchar(50),barrage varchar(255),ip varchar(20))ENGINE=MyISAM;'%date)
        cur.execute('alter table barrage_%s add index barrage_index_%s (`date`)'%(date,date))
        conn.commit()
        cur.close
        conn.close()
    except:
        pass
###更新弹幕总表及其子表
def barrage_create(date_str):
    try:
        conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,db='live_user',charset='utf8')
        cur=conn.cursor()
        cur.execute('drop table if exists barrage;')
        cur.execute('create table barrage (`date` int(8),`bid` varchar(5),hour int(2),minute int(2),activityId varchar(50),channelId varchar(50),uuid varchar(50),barrage varchar(255),ip varchar(20))ENGINE=MERGE;')
        cur.execute('alter table barrage add index barrage_index(`date`);')
        cur.execute('alter table barrage union=(%s);'%date_str)
        cur.close
        conn.close()
        print 'barrage is updated!'
    except:
        print 'barrage update fail!'
        pass
###按照bid_name返回bid
def bid_converse(bid_name):
    try:
        if bid_name == 'pcweb':
            return '2'
        elif bid_name == 'phone':
            return '13'
        elif bid_name == 'pcclient':
            return '8'
        elif bid_name in ('max-app','mgtv_live'):
            return '14'
    except:
        return '-1'
###将数据插入每日弹幕明细表
def barrage_daily_insert(date,barrage_insert):
    error_path='/root/hf/live_user/barrage' #存放插入错误的日志信息
    if not os.path.exists(error_path):
        os.mkdir(error_path)
    insert_tag = 1  ###插入成功表示1
    try:
        sql = 'insert into barrage_'+date+' values('+','.join(map(lambda o: "%s",range(0,9)))+')'
        cur.execute(sql,barrage_insert)
        conn.commit()
    except MySQLdb.Error,e:
        insert_tag = 0  ###插入失败表示1
        error_insertlog_path='/root/hf/live_user/barrage/barrage_error_'+date +".txt" #存放插入错误的日志信�?
        f=open(error_insertlog_path, 'a')
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        f.write("Mysql Error:" + '\t' + str(e.args[0]) + '\t' + str(e.args[1]) + '\t' + str(barrage_insert) + "\n")
        f.close()
    return insert_tag
###每日更新live_channel表，关联activityId
def live_channel_data():
    try:
        conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,db='live_user',charset='utf8')
        channel_data = pd.read_sql('select activityId,channelId from live_channel group by activityId,channelId;',conn)
        conn.close()
        return channel_data
    except:
        pass

###获取从开始日期到现在的日期列表，日期的格式为yyyy,mm,dd
def datelist_new(start_date,end_date):
    result = []
    curr_date = start_date
    while curr_date != end_date:
        result.append("%04d,%02d,%02d" % (curr_date.year, curr_date.month, curr_date.day))
        curr_date += datetime.timedelta(1)
    result.append("%04d,%02d,%02d" % (curr_date.year, curr_date.month, curr_date.day))
    return result

#获取前一天的日期
def day_get(d):
    oneday = datetime.timedelta(days=1)
    day = d - oneday
    date_end=datetime.date(int(day.year),int(day.month),int(day.day))
    #date_end = "%04d%02d%02d" % (day.year, day.month, day.day)
    return date_end
#将校验数据的信息写入文件种
def  write_checkinfo(check_date,orignal_rows,success_rows,percentage):
    file_path = '/root/hf/live_user/barrage'  # 存放插入错误的日志信息
    if not os.path.exists(file_path):
        os.mkdir(file_path)
    file_name='/root/hf/live_user/barrage/barrage_check.txt'
    f=open(file_name, 'a')
    print "start write checkfile"
    f.write(str(check_date) + '\t\t' + str(orignal_rows) + '\t\t' + str(success_rows) + '\t\t' + str('%.5f%%' % percentage) + '\n')
    print "write checkfile success"
    f.close()

if __name__ == '__main__':
    start_date = datetime.date(2016,7,17)###数据统计开始时间
    # 获取当前时间
    d = datetime.datetime.now()
    #获取
    end_date =day_get(d)  ###本次数据插入时间
    barrage_date_str = datelist(start_date,end_date)###获取时间区间内的时间
    date = end_date.strftime('%Y%m%d')###本次数据插入时间格式化
    barrage_daily_create(date)###建立当天的弹幕明细表子表
    date_str = datelist(start_date=start_date,end_date=end_date)###数据明细表包含时间区间
    barrage_data = barrage_get(date)###获取当天的弹幕数据
    channel_data = live_channel_data()
    try:
        barrage_data_all = barrage_data.merge(channel_data,how='left',on='channelId')
        barrage_data_all = barrage_data_all.fillna(-1)
        if len(barrage_data_all) > 0:
            print 'start insert barrage daily data into database'
            conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,db='live_user',charset='utf8')
            cur=conn.cursor()
            length = len(barrage_data_all)  ####插入数据总共行数
            insert_success=0 ##记录插入成功的条数
            for i in range(length):
                barrage_target= barrage_data_all.loc[i]
                data_everyrow = list()  # 插入到新表的参数值
                try:
                    day = barrage_target['time'][0:8] if len(barrage_target['time']) == 12  else ''
                    bid = bid_converse(barrage_target['bid_name']) if bid_converse(barrage_target['bid_name']) > 0 else ''
                    hour = barrage_target['time'][8:10] if len(barrage_target['time']) == 12 else ''
                    min = barrage_target['time'][10:12] if len(barrage_target['time']) == 12 else ''
                    activityId = barrage_target['activityId'] if len(str(barrage_target['activityId'])) > 0 else ''
                    channelId = barrage_target['channelId'] if len(str(barrage_target['channelId'])) > 0 else ''
                    uuid = barrage_target['uuid'] if len(barrage_target['uuid']) > 0 else ''
                    barrage = barrage_target['barrage'] if len(barrage_target['barrage']) > 0 else ''
                    ip = barrage_target['ip'] if len(str(barrage_target['ip'])) > 0 else ''
                    data_everyrow.extend((day,bid,hour,min,activityId,channelId,uuid,barrage,ip))
                    insert_tag=barrage_daily_insert(date,data_everyrow) #向新表中插入数据
                    if insert_tag == 1:
                        insert_success += 1  # 记录成功插入的条数
                    else:
                        pass
                except:
                   print barrage_target
            cur.close()
            conn.close()
            # 将校验数据信息写入文件中
            percentage = insert_success / float(length) * 100 #成功的百分比
            write_checkinfo(date,length ,insert_success,percentage)
            print 'insert into barrage daily data success!\nall data length is %s,insert success is %s\nsuccess percent is %.2f%%'%(len(barrage_data_all),insert_success,len(barrage_data_all)/float(insert_success)*100)
    except:
        print 'insert into barrage daily data fail!'
    barrage_create(date_str=barrage_date_str)###更新当日的汇总表


