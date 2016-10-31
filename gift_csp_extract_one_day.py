#!/usr/local/Python2.7/bin/python
# -*- coding: utf-8 -*-

"""@author:hefang
@file: gift.py
@time: 2016/9/28 18:18
this module process daily gift data
"""

import datetime
import os
from tqdm import tqdm
import MySQLdb
from sshtunnel import SSHTunnelForwarder
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
        result_1.append('gift_'+result[i])
    datestr = ','.join(result_1)
    return datestr

###获取送礼数据
def gift_get(date):
    print 'start to get daily gift %s data!'%date
    with SSHTunnelForwarder(('123.57.136.93', 22),ssh_password="Test@max1",ssh_username="root",remote_bind_address=('drdsh3f3wa056yr1.drds.aliyuncs.com', 3306)) as server:  #B机器的配�?
        conn = MySQLdb.connect(host='127.0.0.1',port=server.local_bind_port,user='max_artist_popularity_ro',passwd='Max_artist_popularity_ro',db='max_artist_popularity')#A机器的配�?此处host必须是是127.0.0.1
        sql = "select a.date,a.hour,a.min,a.bid_name,a.type,a.from_uuid,a.to_uuid,a.gid,a.ip,sum(a.popularity_inc) as popularity_inc,sum(a.total_price) as total_price from " \
              "(select DATE_FORMAT(ctime,'%%Y%%m%%d') as date,DATE_FORMAT(ctime,'%%H') as hour,DATE_FORMAT(ctime,'%%i') as min,invokerSource as bid_name,type,from_uid as from_uuid,to_uid as to_uuid," \
              "popularity_inc,gid,total_price,ip from eventlist where day = '%s') a " \
              "GROUP BY a.date,a.hour,a.min,a.bid_name,a.type,a.from_uuid,a.to_uuid,a.gid,a.ip"%date
        try:
            gift_data = pd.read_sql(sql,conn)
        except:
            gift_data = pd.DataFrame()
        if len(gift_data) == 0:
            print 'get daily gift %s data fail!'%date
        else:
            print 'daily gift %s data get!'%date
        conn.close()
    return gift_data
###建立当天的送礼明细�?
def gift_daily_create(date):
    try:
        conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,charset='utf8')
        cur=conn.cursor()
        conn.select_db('live_user')
        cur.execute('drop table if exists gift_%s;'%date)
        cur.execute('create table gift_%s (`date` int(8),`bid` varchar(5),hour int(2),minute int(2),mode varchar(5),from_uuid varchar(50),to_uuid varchar(50),gid varchar(5),ip varchar(20),popularity_inc int(10),total_price int(10))ENGINE=MyISAM;'%date)
        cur.execute('alter table gift_%s add index gift_index_%s (`date`)'%(date,date))
        conn.commit()
        cur.close
        conn.close()
    except:
        pass
###更新送礼总表及其子表
def gift_create(date_str):
    try:
        conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,db='live_user',charset='utf8')
        cur=conn.cursor()
        cur.execute('drop table if exists gift;' )
        cur.execute('create table gift (`date` int(8),`bid` varchar(5),hour int(2),minute int(2),mode varchar(5),from_uuid varchar(50),to_uuid varchar(50),gid varchar(5),ip varchar(20),popularity_inc int(10),total_price int(10))ENGINE=MERGE;')
        cur.execute('alter table gift add index gift_index (`date`);')
        cur.execute('alter table gift union=(%s);'%date_str)
        cur.close
        conn.close()
        print 'gift is updated!'
    except:
        print 'gift update fail!'
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
        elif bid_name == 'mgzb':
            return '14'
        elif bid_name == 'mgtv':
            return '15'
    except:
        return ''
###将数据插入每日送礼明细表
def gift_daily_insert(date,gift_insert):
    error_path='/root/hf/live_user/gift' #存放插入错误的日志信息
    if not os.path.exists(error_path):
        os.mkdir(error_path)
    insert_tag = 1  ###插入成功表示1
    try:
        sql = 'insert into gift_'+date+' values('+','.join(map(lambda o: "%s",range(0,11)))+')'
        cur.execute(sql,gift_insert)
        conn.commit()
    except MySQLdb.Error,e:
        insert_tag = 0  ###插入失败表示1
        error_insertlog_path='/root/hf/live_user/gift/gift_error_'+date +".txt" #存放插入错误的日志信�?
        f=open(error_insertlog_path, 'a')
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        f.write("Mysql Error:" + '\t' + str(e.args[0]) + '\t' + str(e.args[1]) + '\t' + str(gift_insert) + "\n")
        f.close()
    return insert_tag
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
    return date_end

#将校验数据的信息写入文件种
def  write_checkinfo(check_date,orignal_rows,success_rows,percentage):
    file_path = '/root/hf/live_user/gift'  # 存放插入错误的日志信息
    if not os.path.exists(file_path):
        os.mkdir(file_path)
    file_name='/root/hf/live_user/gift/gift_check.txt'
    f=open(file_name, 'a')
    print "start write checkfile"
    f.write(str(check_date)+'\t\t'+str(orignal_rows)+'\t\t'+str(success_rows)+'\t\t'+str('%.5f%%'% percentage)+'\n')
    print "write checkfile success"
    f.close()

if __name__ == '__main__':
    start_date = datetime.date(2016,7,17)###数据统计开始
    # 获取当前时间
    d = datetime.datetime.now()
    end_date = day_get(d)###本次数据插入时间
    gift_date_str = datelist(start_date,end_date)###获取时间区间内的时间
    date = end_date.strftime('%Y%m%d')###本次数据插入时间格式�?
    gift_daily_create(date)###建立当天的送礼明细表子�?
    date_str = datelist(start_date=start_date,end_date=end_date)###数据明细表包含时间区�?
    gift_data = gift_get(date)###获取当天的送礼数据
    print len(gift_data)
    try:
        if len(gift_data) > 0:
            print 'start insert gift daily data into database'
            conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,db='live_user',charset='utf8')
            cur=conn.cursor()
            length=len(gift_data)   ####插入数据总共行数
            insert_success = 0###插入成功次数
            for i in range(length):
                    gift_target = gift_data.loc[i] #获取原表中的每行记录
                    data_everyrow=list()    #插入到新表的参数值
                    try:
                        day = gift_target['date'] if gift_target['date'] is not None else ''
                        bid = bid_converse(gift_target['bid_name']) if bid_converse(gift_target['bid_name']) is not None else ''
                        hour = gift_target['hour'] if gift_target['hour'] is not None else ''
                        min = gift_target['min'] if gift_target['min'] is not None else ''
                        mode = gift_target['type'] if gift_target['type'] is not None else ''
                        from_uuid = gift_target['from_uuid'] if gift_target['from_uuid'] is not None else ''
                        to_uuid = gift_target['to_uuid'] if gift_target['to_uuid'] is not None else ''
                        gid = int(gift_target['gid']) if gift_target['gid'] >= 0 else ''
                        ip = gift_target['ip'] if gift_target['ip'] is not None else ''
                        popularity_inc = gift_target['popularity_inc'] if gift_target['popularity_inc'] >= 0 else 0
                        total_price = gift_target['total_price'] if gift_target['total_price'] >= 0 else 0
                        data_everyrow.extend((day,bid,hour,min,mode,from_uuid,to_uuid,gid,ip,popularity_inc,total_price))
                        insert_tag=gift_daily_insert(date,data_everyrow)  #向新表中插入数据
                        if insert_tag==1:
                           insert_success += 1 #记录成功插入的条数
                        else:
                            pass
                    except:
                        print gift_target
            cur.close()
            conn.close()
            # 将校验数据信息写入文件中
            percentage=insert_success/float(length)*100   ####成功的百分比
            write_checkinfo(date, length, insert_success,percentage)
            print 'insert into gift daily data success!\nall data length is %s,insert success is %s\nsuccess percent is %.2f%%'%(len(gift_data),insert_success,len(gift_data)/float(insert_success)*100)
    except:
        print 'insert gift daily data fail!'
    gift_create(date_str=gift_date_str)###更新当日的汇总表
