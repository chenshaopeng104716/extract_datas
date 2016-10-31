#!/usr/local/Python2.7/bin/python
# -*- coding: utf-8 -*-

"""@author:hefang
@file: pay.py
@time: 2016/10/12 16:39
"""

import datetime
import commands
import pandas as pd
import MySQLdb
import json
from tqdm import tqdm
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


###获取从开始日期到现在的日期间隔
def datelist(start_date,end_date):
    result = []
    curr_date = start_date
    while curr_date != end_date:
        result.append("%04d%02d%02d" % (curr_date.year, curr_date.month, curr_date.day))
        curr_date += datetime.timedelta(1)
    result.append("%04d%02d%02d" % (curr_date.year, curr_date.month, curr_date.day))
    result_1 = list()
    for i in range(len(result)):
        result_1.append('pay_'+result[i])
    datestr = ','.join(result_1)
    return datestr
###获取付费数据
def pay_get(date):
    print 'start to get daily pay %s data!' %date
    conn = MySQLdb.connect(host="10.100.10.61",user="yangjuanjuan",passwd="yangjuanjuan1234",db="payment_system",charset="utf8",port=3306)
    sql = "select FROM_UNIXTIME(p.create_time,'%%Y%%m%%d%%H%%i') as `create_time`," \
          "case p.platform_id when '1030' then '15' when '1036' then '15' when '110' then '1' when '103' then '1' when '1032' then '14' else '' end as bid," \
          "p.product_id,p.title,p.pay_account,p.paid_amount/100 as `money` from pay_order p " \
          "where FROM_UNIXTIME(p.create_time,'%%Y%%m%%d')='%s' and p.platform_id in ('1030','1036','110','103','1032') " \
          "and p.`status` = '3' and p.paid_amount/100 !='0';"%date
    try:
        pay_data = pd.read_sql(sql,conn)
    except:
        pay_data = pd.DataFrame()
    if len(pay_data) == 0:
        print 'get daily pay data fail!'
    else:
        print 'daily pay data get!'
        print 'daily pay %s is %s' % (date, len(pay_data))
    return pay_data
###建立当天的付费明细表
def pay_daily_create(date):
    try:
        conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,charset='utf8')
        cur=conn.cursor()
        conn.select_db('live_user')
        cur.execute('drop table if exists pay_%s;'%date)
        cur.execute('create table pay_%s (`date` int(8),`bid` varchar(5),hour int(2),minute int(2),product_id varchar(10),title varchar(100),uuid varchar(50),money int(10))ENGINE=MyISAM;'%date)
        cur.execute('alter table pay_%s add index pay_index_%s (`date`)'%(date,date))
        conn.commit()
        cur.close
        conn.close()
    except:
        pass
###更新付费总表及其子表
def pay_create(date_str):
    try:
        conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,db='live_user',charset='utf8')
        cur=conn.cursor()
        cur.execute('drop table if exists pay;')
        cur.execute('create table pay (`date` int(8),`bid` varchar(5),hour int(2),minute int(2),product_id varchar(10),title varchar(100),uuid varchar(50),money int(10))ENGINE=MERGE;')
        cur.execute('alter table pay add index pay_index (`date`);')
        cur.execute('alter table pa union=(%s);'%date_str)
        cur.close
        conn.close()
        print 'pay is updated!'
    except:
        print 'pay update fail!'
        pass
###将数据插入每日弹幕明细表
def pay_daily_insert(date,pay_insert):
    error_path='/root/hf/live_user/pay' #存放插入错误的日志信息
    if not os.path.exists(error_path):
        os.mkdir(error_path)
    insert_tag=1  ###插入成功表示1
    try:
        sql = 'insert into pay_'+date+' values('+','.join(map(lambda o: "%s",range(0,8)))+')'
        cur.execute(sql,pay_insert)
        conn.commit()
    except MySQLdb.Error,e:
        insert_tag = 0  ###插入失败表示1
        error_insertlog_path='/root/hf/live_user/pay/pay_error_'+date +".txt" #存放插入错误的日志信息
        f=open(error_insertlog_path, 'a')
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        f.write("Mysql Error:" + '\t' + str(e.args[0]) + '\t' + str(e.args[1]) + '\t' + str(pay_insert) + "\n")
        f.close()
    pass
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
    file_path = '/root/hf/live_user/pay'  # 存放插入错误的日志信息
    if not os.path.exists(file_path):
        os.mkdir(file_path)
    file_name='/root/hf/live_user/pay/pay_check.txt'
    f=open(file_name, 'a')
    print "start write checkfile"
    f.write(str(check_date) + '\t\t' + str(orignal_rows) + '\t\t' + str(success_rows) + '\t\t' + str('%.5f%%' % percentage) + '\n')
    print "write checkfile success"
    f.close()

if __name__ == '__main__':
    start_date = datetime.date(2016,7,17)###数据统计开始时间
    # 获取当前时间
    d = datetime.datetime.now()
    end_date =day_get(d) ###本次数据插入时间
    pay_date_str = datelist(start_date,end_date)###获取时间区间内的时间
    date = end_date.strftime('%Y%m%d')###本次数据插入时间格式化
    pay_daily_create(date)###建立当天的付费明细表子表
    pay_data = pay_get(date)###获取当天的付费数据
    try:
        if len(pay_data) > 0:
            print 'start insert pay daily data into database'
            conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,db='live_user',charset='utf8')
            cur=conn.cursor()
            length_list = 10000###每10000行插入一次数据
            length =len(pay_data)###数据框长度
            insert_success = 0  ###插入成功次数
            for i in range(length):
                pay_target = pay_data.loc[i]
                data_everyrow = list()  # 插入到新表的参数值
                try:
                    day = pay_target['create_time'][0:8]
                    bid = pay_target['bid'] if len(pay_target['bid']) > 0 and pay_target.notnull()['bid'] else ''
                    hour = pay_target['create_time'][8:10] if type(pay_target['create_time'][9:10]) == unicode and pay_target.notnull()['create_time'] else ''
                    minute = pay_target['create_time'][10:12] if type(pay_target['create_time'][11:12]) == unicode and pay_target.notnull()['create_time'] else ''
                    product_id = pay_target['product_id'] if pay_target['product_id'] and pay_target.notnull()['product_id'] else ''
                    title = pay_target['title'] if pay_target['title'] and pay_target.notnull()['title'] else ''
                    uuid = pay_target['pay_account'] if pay_target['pay_account'] and pay_target.notnull()['pay_account'] else ''
                    money = pay_target['money'] if pay_target['money'] and pay_target.notnull()['money'] else ''
                    data_everyrow.extend((day,bid,hour,minute,product_id,title,uuid,money))
                    insert_tag=pay_daily_insert(date,data_everyrow)
                    if insert_tag==1:
                       insert_success+=1
                    else:
                        pass
                except:
                    print pay_target
            cur.close()
            conn.close()
            # 将校验数据信息写入文件中
            # 将校验数据信息写入文件中
            percentage = insert_success / float(length) * 100  ####成功的百分比
            write_checkinfo(date, length, insert_success,percentage)
            print 'insert into pay daily data success!'
    except:
        print 'insert into pay daily data fail!'
    pay_create(date_str=pay_date_str)###更新当日的汇总表


