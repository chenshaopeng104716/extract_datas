# -*- coding:utf-8 -*-
"""
@this module extract data from aws->dm_pv_fact->mglive_hour_fact(year,month,day,hour,bid,uid,vid,liveid,did,type)
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
reload(sys)
sys.setdefaultencoding('utf-8')
####获取来源于mglive数据
def mglive_vv_data_get(year,month,day):
    try:
        conn = psycopg2.connect(database="dm_pv_fact", user="product_readonly", password="SDjTty7202d7Dldfui", host="54.222.196.128",port="2345")
        sql="select hour,uid as uuid,vid,liveid,did,type from mglive_hour_fact where year='%s' and month='%s' and day='%s';"%(year,month,day)
        print "start to get mglive_vv %s%s%s" %(year,month,day)
        try:
            mglive_data = pd.read_sql(sql,conn)
            print "get mglive_vv  data  %s%s%s success"%(year,month,day)
        except:
            mglive_data = pd.DataFrame()
            print "get mglive_vv  data  %s%s%s fails" % (year, month, day)
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[ 0 ], e.args[ 1 ])
    return mglive_data

###建立当天的mglive_vv明细表
def mglive_vv_daily_create(date):
    try:
        conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',db='live_user',port=3306,charset='utf8')
        cur=conn.cursor()
        cur.execute('drop table if exists mglive_vv_%s;' % date)
        cur.execute('create table mglive_vv_%s (date int(8),hour int(2),bid int(2),uuid varchar(50),vid varchar(11),liveid varchar(50),did varchar(50),type int(2))ENGINE=MyISAM;' % date)
        cur.execute('alter table mglive_vv_%s add index mglive_vv_index_%s (`date`)' % (date, date))
        conn.commit()
        cur.close
        conn.close()
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[ 0 ], e.args[ 1 ])
        pass

###更新mglive_vv总表及其子表
def mglive_vv_create(date_str):
    try:
        conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,db='live_user',charset='utf8')
        cur=conn.cursor()
        cur.execute('drop table if exists mglive_vv;')
        cur.execute('create table mglive_vv (date int(8),hour int(2),bid int(2),uuid varchar(50),vid varchar(11),liveid varchar(50),did varchar(50),type int(2))ENGINE=MERGE;')
        cur.execute('alter table mglive_vv add index mglive_vv_index (`date`);')
        cur.execute('alter table mglive_vv union=(%s);' % date_str)
        cur.close
        conn.close()
        print 'mglive_vv is updated!'
    except:
        print 'mglive_vv update fail!'
        pass

###插入每日的mglive_vv
def mglive_vv_daily_insert(conn,cur,date,mglive_vv_insert):
    error_path = '/root/hf/live_user/mglive_vv'
    if not os.path.exists(error_path):
        os.mkdir(error_path)
    insert_tag = 1  ###插入成功表示1
    try:
        sql = 'insert into mglive_vv_'+date+' values('+','.join(map(lambda o: "%s",range(0,8)))+')'
        cur.executemany(sql,mglive_vv_insert)
        conn.commit()
    except MySQLdb.Error,e:
        insert_tag = 0  ###插入失败表示0
        error_insertlog_path = '/root/hf/live_user/mglive_vv/mglive_vv_error_' + date + ".txt"  # 存放插入错误的日志信息
        f = open(error_insertlog_path, 'a')
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        f.write("Mysql Error %d: %s,%s" % ( e.args[0], e.args[1],mglive_vv_insert) + "\n")
        f.close()
        pass
    return insert_tag

# 将校验数据的信息写入文件种
def write_checkinfo(check_date, orignal_rows, success_rows, percentage):
    file_path = '/root/hf/live_user/mglive_vv'  # 存放检验文本的目录
    if not os.path.exists(file_path):
        os.mkdir(file_path)
    file_name = '/root/hf/live_user/mglive_vv/mglive_vv_check.txt'  # 检验文本的名称
    f = open(file_name, 'a')
    print "start write checkfile"
    f.write(str(check_date) + '\t\t' + str(orignal_rows) + '\t\t' + str(success_rows) + '\t\t' + str('%.5f%%' % percentage) + '\n')
    print "write checkfile success"
    f.close()

#获取前一天的日期
def day_get(d):
    oneday = datetime.timedelta(days=1)
    day = d - oneday
    date_end=datetime.date(int(day.year),int(day.month),int(day.day))
    return date_end


###获取从开始日期到现在的日期区间列表
def datelist(start_date,end_date):
    result = []
    curr_date = start_date
    while curr_date != end_date:
        result.append("%04d%02d%02d" % (curr_date.year, curr_date.month, curr_date.day))
        curr_date += datetime.timedelta(1)
    result.append("%04d%02d%02d" % (curr_date.year, curr_date.month, curr_date.day))
    result_1 = list()
    for i in range(len(result)):
        result_1.append('mglive_vv_'+result[i])
    datestr = ','.join(result_1)
    return datestr

###获取从开始日期到现在的日期列表，日期的格式为yyyy,mm,dd
def datelist_new(start_date,end_date):
    result = []
    curr_date = start_date
    while curr_date != end_date:
        result.append("%04d,%02d,%02d" % (curr_date.year, curr_date.month, curr_date.day))
        curr_date += datetime.timedelta(1)
    result.append("%04d,%02d,%02d" % (curr_date.year, curr_date.month, curr_date.day))
    return result


if __name__ == '__main__':
    # 获取当前时间
    start_date = datetime.date(2016,8,1)  ###总表数据统计开始时间
    d = datetime.datetime.now()
    oneday = datetime.timedelta(days=1)
    day = d - oneday
    end_date = datetime.date(int(day.year), int(day.month), int(day.day))
    sql_day="%02d"  %day.day
    sql_month="%02d" %day.month
    sql_year="%04d" %day.year
    date_list = datelist_new(start_date,end_date);
    mglive_vv_date_str = datelist(start_date,end_date)  # 获得日期列表
    bid=14
    date = end_date.strftime('%Y%m%d')  ###本次数据插入时间格式
    print date
    mglive_vv_data=mglive_vv_data_get(sql_year,sql_month,sql_day)
    length = len(mglive_vv_data)  ###获取数据的长度
    print length
    try:
        if length>0:
            ##创建每日表
            mglive_vv_daily_create(date)
            print 'start insert %s daily mglive_vv into database' % date
            conn = MySQLdb.connect(host='10.100.3.64', user='hefang', passwd='NYH#dfjirD872C$d&ss', port=3306, db='live_user', charset='utf8')
            cur = conn.cursor()
            insert_success=0;##统计插入成功的行数
            length_list = 10000  ###每10000行插入一次
            length_split = (length - 1) / length_list + 1  ###将数据分段，每10000行为一段
            for j in tqdm(range(length_split)):
                data_list = list()
                if j < length_split - 1:###在每1000为一份时一次插1000条
                    xrange_length = length_list
                elif j == length_split - 1:###在最后的一份取剩下行数
                    xrange_length = length-length_list*j
                for k in xrange(xrange_length):
                    j_loc = j * length_list + k
                    mglive_vv_target = mglive_vv_data.loc[j_loc]
                    data_everyrow = list()  # 插入到新表的参数值
                    try:
                        hour = mglive_vv_target[ 'hour' ] if mglive_vv_target[ 'hour' ] is not None else ''
                        uuid=mglive_vv_target[ 'uuid' ] if mglive_vv_target[ 'uuid' ] is not None else ''
                        vid = mglive_vv_target[ 'vid' ] if mglive_vv_target[ 'vid' ] is not None else ''
                        liveid= mglive_vv_target[ 'liveid' ] if mglive_vv_target[ 'liveid' ] is not None else ''
                        did = mglive_vv_target[ 'did' ] if mglive_vv_target[ 'did' ] is not None else ''
                        type = mglive_vv_target[ 'type' ] if mglive_vv_target[ 'type' ] is not None else ''
                        data_everyrow.extend((date,hour,bid,uuid,vid,liveid,did,type))
                        data_list.append(data_everyrow)
                    except MySQLdb.Error,e:
                         print "Mysql Error %d: %s" % (e.args[ 0 ], e.args[ 1 ])
                     # print mglive_vv_target
                ###每一千行插入一次
                insert_tag = mglive_vv_daily_insert(conn, cur,date,data_list)
                if insert_tag == 1:  ###插入成功
                    insert_success += xrange_length # 记录成功插入的条数
                else:
                    pass
            cur.close
            conn.close()
            # 将校验数据信息写入文件中
            percentage = insert_success /float(length) * 100  ####成功的百分比
            write_checkinfo(date,length, insert_success, percentage)
            # 更新总表
            mglive_vv_create(date_str=mglive_vv_date_str)
        else:
            print  "get %s mglive_vv data failure" %date
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[ 0 ], e.args[ 1 ])
        print 'insert mglive_vv %s daily data fail!' %date



