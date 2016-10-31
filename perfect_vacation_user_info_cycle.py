#!/usr/local/Python2.7/bin/python
# -*- coding: utf-8 -*-

"""@author:hefang
@file: user_info.py
@time: 2016/10/13 9:47
"""
import urllib2
import urllib
import requests
import json
import datetime
from urllib import quote
import hashlib
import pandas as pd
import MySQLdb
import os
from tqdm import tqdm
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

###读取当日的uuid
def uuid_get(date):
    conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,charset='utf8')
    conn.select_db('live_user')
    if date == '20160717':
        sql = "select uuid from perfect_vacation where date='%s';"%date###若为第一天则为全部用户，往后则为新增用�?
    else:
        sql = "select uuid from perfect_vacation where date='%s' and uuid not in (select uuid from perfect_vacation where date>='20160717' and date<'%s')"%(date,date)
    print 'start to get uuid daily data %s'%date
    try:
        uuid = pd.read_sql(sql,conn)
        print 'get uuid data %s success!'%date
    except:
        uuid = pd.DataFrame()
        print 'get uuid data %s fail!'%date
    return uuid
###加密签名
def make_sign(data,secret_key):
    string = ''
    keys = data.keys()
    keys.sort()
    for i in keys:
        string += '%s=%s&'%(i,data[i])
    string = (string + 'secret_key=' + secret_key).lower()
    return hashlib.sha1(string.lower()).hexdigest().lower()
###通过接口获取用户信息
def uuid_info_get(date,uuid_str):
    data = dict()
    data['uip'] = '10.32.0.100'
    data['uuid'] = uuid_str
    data['from'] = '123'
    datajson = json.dumps(data)
    http_data = {'invoker':'pc','data':datajson}
    secret_key = '&^khiwf*#%1'
    sign = make_sign(http_data,secret_key)
    url = 'http://idp.hifuntv.com/in/GetUserInfoByUuid?invoker=pc&data=%s&sign=%s' % (quote(datajson), sign)
    resp = urllib2.urlopen(url)
    content = resp.read()
    msg = json.loads(content)['msg']
    uuid_info_list = list()###用户信息列表
    for uuid_info_dict in msg:
        uuid_dict = msg[uuid_info_dict]
        uuid_info = uuid_dict_process(date,uuid_dict)
        uuid_info_list.append(uuid_info)
    return uuid_info_list
###处理用户信息dict
def uuid_dict_process(date,uuid_dict):
    uuid_info = list()
    uuid = uuid_dict['uuid'] if uuid_dict.has_key('uuid') else ''
    birthday = uuid_dict['birthday'] if uuid_dict.has_key('birthday') else ''
    sex = uuid_dict['sex'] if uuid_dict.has_key('sex') else ''
    province = uuid_dict['province'] if uuid_dict.has_key('province') else ''
    city = uuid_dict['city'] if uuid_dict.has_key('city') else ''
    nickname = uuid_dict['nickname'] if uuid_dict.has_key('nickname') else ''
    mobile = uuid_dict['mobile'] if uuid_dict.has_key('mobile') else ''
    isVip = uuid_dict['isVip'] if uuid_dict.has_key('isVip') else ''
    vipExpiretime = uuid_dict['vipExpiretime'] if uuid_dict.has_key('vipExpiretime') else ''
    uuid_info.extend((date,uuid,birthday,sex,province,city,nickname,mobile,isVip,vipExpiretime))
    return uuid_info
###建立用户�?
def perfect_vacation_new_user_info_create():
    try:
        conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,charset='utf8')
        cur=conn.cursor()
        conn.select_db('live_user')
        cur.execute('drop table if exists perfect_vacation_new_user;create table perfect_vacation_new_user (`date` int(8),`uuid` varchar(50),birthday varchar(50),sex varchar(10),province varchar(50),'\
                    'city varchar(50),nickname varchar(50),mobile varchar(20),isVip varchar(10),vipExpiretime varchar(50));')
        cur.execute('alter table perfect_vacation_new_user add index perfect_vacation_new_user_index (`date`);')
        conn.commit()
        cur.close
        conn.close()
    except:
        pass
###将数据插入用户表
def perfect_vacation_new_user_info_insert(conn,cur,date,uuid_insert):
    error_path = '/root/hf/live_user/perfect_vacation_new_user'  # 存放插入错误的日志信�?
    if not os.path.exists(error_path):
        os.mkdir(error_path)
    insert_tag = 1  ###�?入成功表�?
    try:
        sql = 'insert into perfect_vacation_new_user values('+','.join(map(lambda o: "%s",range(0,10)))+')'
        cur.executemany(sql,uuid_insert)
        conn.commit()
    except MySQLdb.Error,e:
        insert_tag = 0  ###插入失败表示0
        error_insertlog_path = '/root/hf/live_user/perfect_vacation_new_user/perfect_vacation_new_user_error_' + date + '.txt'  # 存放插入错误的日志信�?
        f = open(error_insertlog_path, 'a')
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        f.write("Mysql Error:"+'\t'+str(e.args[0])+ '\t'+str(e.args[1])+'\t'+str(uuid_insert)+ "\n")
        f.close()
    return insert_tag

##更新perfect_vacation_new_user中的birthday，将1970-01-01设置为空
def  update_perfect_vacation_new_user_birthday(conn,cur,date):
    try:
        cur.execute("update perfect_vacation_new_user set birthday=null where birthday='1970-01-01' and date=%s;" % date)
        conn.commit()
        print "update perfect_vacation_new_user birthday success"
    except:
        print "updatge perfect_vacation_new_user birthday failure"

###获取从开始日期到现在的日期列表，日期的格式为yyyy,mm,dd
def datelist_new(start_date,end_date):
    result = []
    curr_date = start_date
    while curr_date != end_date:
        result.append("%04d%02d%02d" % (curr_date.year, curr_date.month, curr_date.day))
        curr_date += datetime.timedelta(1)
    result.append("%04d%02d%02d" % (curr_date.year, curr_date.month, curr_date.day))
    return result
#获取前一天的日期
def day_get(d):
    oneday = datetime.timedelta(days=1)
    day = d - oneday
    date_end= datetime.date(day.year, day.month, day.day)
    #date_to = datetime.date(day.year, day.month, day.day)
    #print '---'.join([str(date_from), str(date_to)])
    return date_end

if __name__ == '__main__':
    start_date = datetime.date(2016,7,17)  ###总表数据统计开始时�?
    # 获取当前时间
    d = datetime.datetime.now()
    end_date_f = day_get(d)  ##当前日期的前一�?
    date_list = datelist_new(start_date, end_date_f)  # 获得日期列表
    for i in range(30,40):
    #for i in range(len(date_list)):
        date = date_list[i]
        uuid_data = uuid_get(date)###获取新增用户uuid
        length = len(uuid_data)
        if length > 0:
            try:
                if date == '20160717':
                    perfect_vacation_new_user_info_create()
                else:
                    pass
                print 'start insert %s daily new user info into database'%date
                conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,db='live_user',charset='utf8')
                cur=conn.cursor()
                length_list=50  ###�?0行插入一次，50的限制是接口限制
                length_split=(length-1)/length_list+1    ###将数据分段，�?0行为一�?
                insert_success = 0###插入成功次数
                for i in tqdm(range(length_split)):
                    uuid_list = list()
                    if i < length_split - 1:###在每50为一份时一次插50�?
                        xrange_length = length_list
                    elif i == length_split - 1:###在最后的一份取剩下行数
                        xrange_length = length-length_list*i
                    for j in xrange(xrange_length):
                        data_everyrow = list()
                        j_loc = i * length_list + j
                        uuid_target = uuid_data.loc[j_loc]['uuid']
                        uuid_list.append(uuid_target)
                    uuid_str = '|'.join(uuid_list)###传入的参数需要以|进行分割
                    uuid_insert = uuid_info_get(date,uuid_str)
                    insert_tag = perfect_vacation_new_user_info_insert(conn,cur,date,uuid_insert)
                    if insert_tag == 1:
                        insert_success += len(uuid_list)  # 记录�?功插入的�??
                    else:
                        pass
                 ##更新birthday
                update_perfect_vacation_new_user_birthday(conn, cur, date)
                cur.close
                conn.close
                print 'insert %s daily perfect vacation new user info into database success'%date
            except:
                print 'insert %s daily perfect vacation  new user info into database fail'%date