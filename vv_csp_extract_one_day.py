#!/usr/local/Python2.7/bin/python
# -*- coding: utf-8 -*-

"""@author:hefang
@file: vv.py
@time: 2016/9/30 10:54
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


###获取aws上的日志命令
def aws_log_get(date_aws):
    aws_shell = 'export PATH=/usr/local/bin;aws s3 cp s3://data-archive/data/live/bid-2.1.1.1-default/%s /root/hf/%s --recursive'%(date_aws,date_aws)
    return aws_shell
###执行shell命令
def shell(command):
    state = commands.getstatusoutput(command)[0]
    if state == 0:
        print 'shell execute command success!'
    else:
        print 'shell execute command fail!'
    return state###返回shell命令执行状态
###遍历日志文件目录
def GetFileList(dir,fileList):
    newDir = dir
    if os.path.isfile(dir):
        fileList.append(dir)
    elif os.path.isdir(dir):
        for s in os.listdir(dir):
            #如果需要忽略某些文件夹，使用以下代码
            #if s == "xxx":
                #continue
            newDir = os.path.join(dir,s)
            GetFileList(newDir,fileList)
    return fileList
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
        result_1.append('vv_'+result[i])
    datestr = ','.join(result_1)
    return datestr
###按照bid_name返回bid
def bid_converse(bid_name):
    try:
        if bid_name == 'aphone':
            return '9'
        elif bid_name == 'iphone':
            return '12'
        elif bid_name == 'ipad':
            return '11'
    except:
        return ''
###匹配act=play事件
def act_play_match(content_list):
    if type(content_list) == list:
        for content in content_list:
            if content.has_key('act') and content['act'] == 'aplay':
                return content
            else:
                continue
    elif type(content_list) == dict:
        return content_list
###建立当天的vv明细表
def vv_daily_create(date):
    try:
        conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,charset='utf8')
        cur=conn.cursor()
        conn.select_db('live_user')
        # cur.execute('drop table if exists vv_%s;create table vv_%s (`date` int(8),`bid` varchar(5),hour int(2),min int(2),activityId varchar(11),'\
        #             'sourceId varchar(11),did varchar(50),uuid varchar(50),act varchar(20),ip varchar(30),aver varchar(50),idx int(5))ENGINE=MyISAM;'\
        #             'alter table vv_%s add index vv_index_%s (`date`)'%(date,date,date,date))
        cur.execute('drop table if exists vv_%s;' % date)
        cur.execute('create table vv_%s (`date` int(8),`bid` varchar(5),hour int(2),min int(2),activityId varchar(11),sourceId varchar(11),did varchar(50),uuid varchar(50),act varchar(20),ip varchar(30),aver varchar(50),idx int(5))ENGINE=MyISAM;' % date)
        cur.execute('alter table vv_%s add index vv_index_%s (`date`)' % (date, date))
        conn.commit()
        cur.close
        conn.close()
    except:
        pass
###更新vv总表及其子表
def vv_create(date_str):
    try:
        conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,db='live_user',charset='utf8')
        cur=conn.cursor()
        # cur.execute('drop table if exists vv;create table vv (`date` int(8),`bid` varchar(5),hour int(2),minute int(2),activityId varchar(11),'\
        #             'sourceId varchar(11),did varchar(50),uuid varchar(50),act varchar(20),ip varchar(30),aver varchar(50),idx int(5))ENGINE=MERGE;'\
        #             'alter table vv add index vv_index (`date`);'\
        #             'alter table vv union=(%s);'%date_str)
        cur.execute('drop table if exists vv;')
        cur.execute( 'create table vv (`date` int(8),`bid` varchar(5),hour int(2),minute int(2),activityId varchar(11),sourceId varchar(11),did varchar(50),uuid varchar(50),act varchar(20),ip varchar(30),aver varchar(50),idx int(5))ENGINE=MERGE;')
        cur.execute('alter table vv add index vv_index (`date`);')
        cur.execute('alter table vv union=(%s);' % date_str)
        cur.close
        conn.close()
        print 'vv is updated!'
    except:
        print 'vv update fail!'
        pass
###将数据插入每日vv明细表
def vv_daily_insert(conn,cur,date,vv_insert):
    error_path = '/root/hf/live_user/vv'  # 存放插入错误的日志信息
    if not os.path.exists(error_path):
        os.mkdir(error_path)
    insert_tag = 1  ###插入成功表示1
    try:
        sql = 'insert into vv_'+date+' values('+','.join(map(lambda o: "%s",range(0,12)))+')'
        cur.execute(sql,vv_insert)
        conn.commit()
    except MySQLdb.Error,e:
        insert_tag = 0  ###插入失败表示1
        error_insertlog_path = '/root/hf/live_user/vv/vv_error_' + date + ".txt"  # 存放插入错误的日志信�?
        f = open(error_insertlog_path, 'a')
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        f.write("Mysql Error:"+'\t'+str(e.args[0])+ '\t'+str(e.args[1])+'\t'+str(vv_insert)+ "\n")
        f.close()
    return insert_tag
###解析日志
def log_process(date_mysql,filepath):
    file_content = open(filepath,'rb').readlines()
    print 'start insert vv daily data into database'
    conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,db='live_user',charset='utf8')
    cur=conn.cursor()
    log_rows=0     ###日志中符合插入条件的行数
    insert_success=0  #记录插入成功的次数
    for line in file_content:
        line_insert = list()
        content_split = line.split('\t')
        if len(content_split) != 8:
            # continue
            pass
        else:
            try:
                time = content_split[0]###取第一个字段为时间
                date = time[0:8]
                hour = time[8:10]
                minute = time[10:12]
                ip = content_split[1]###取第二个字段为ip
                activityId = ''
                content_7 = eval(content_split[7])###
                if type(content_7) == list:
                    common_dict = content_7[0]
                    act_dict = act_play_match(content_7)
                    if type(act_dict) == dict and act_dict.has_key('act') and act_dict['act'] == 'aplay':
                        aver = common_dict['aver']
                        bid_name,version = aver.split('-')[1],aver.split('-')[2]
                        # '''
                        # if bid_name == 'aphone' and version > '4.5.2':
                        #     continue
                        # elif (bid_name == 'aphone' and version <= '4.5.2') or (bid_name == 'iphone' and version <= '4.5.4'):'''
                        bid = bid_converse(bid_name)
                        if common_dict.has_key('lid'):
                            sourceId = common_dict['lid']
                        elif common_dict.has_key('sourceid'):
                            sourceId = common_dict['sourceid']
                        else:
                            sourceId = ''
                        did = common_dict['did'] if common_dict.has_key('did') else ''
                        uuid = common_dict['uuid'] if common_dict.has_key('uuid') else ''
                        act = act_dict['act'] if act_dict.has_key('act') else ''
                        idx = act_dict['idx'] if act_dict.has_key('idx') else ''
                        line_insert.extend([date,bid,hour,minute,activityId,sourceId,did,uuid,act,ip,aver,idx])
                    else:
                        if type(act_dict) == dict and act_dict['act'] in ('play','aplay'):
                            print act_dict['act'],act_dict['aver']
                        else:
                            pass
                        # continue
                elif type(content_7) == dict:
                    act_dict = act_play_match(content_7)
                    if act_dict.has_key('act') and act_dict['act'] == 'aplay':
                        aver = act_dict['aver']
                        bid_name, version = aver.split('-')[1], aver.split('-')[2]
                        # '''
                        # if bid_name == 'aphone' and version > '4.5.2':
                        #     continue
                        # elif (bid_name == 'aphone' and version <= '4.5.2') or (bid_name == 'iphone' and version <= '4.5.4'):'''
                        bid = bid_converse(bid_name)
                        if act_dict.has_key('lid'):
                            sourceId = act_dict['lid']
                        elif act_dict.has_key('sourceid'):
                            sourceId = act_dict['sourceid']
                        else:
                            sourceId = ''
                        did = act_dict['did'] if act_dict.has_key('did') else ''
                        uuid = act_dict['uuid'] if act_dict.has_key('uuid') else ''
                        act = act_dict['act'] if act_dict.has_key('act') else ''
                        idx = act_dict['idx'] if act_dict.has_key('idx') else ''
                        line_insert.extend([date, bid, hour, minute, activityId, sourceId, did, uuid, act, ip, aver, idx])
                    else:
                        if type(content_7) == dict and content_7['act'] in ('play', 'aplay'):
                            print content_7['act'], content_7['aver']
                        else:
                            pass
                            # continue
                if len(line_insert) != 0:
                    log_rows+=1    #表示存在一条记录加1
                    insert_tag=vv_daily_insert(conn,cur,date_mysql,line_insert)
                    if insert_tag==1:
                      insert_success+=1  ###插入成功记录一次
                    else:
                        pass
            except:
                pass
    cur.close()
    conn.close()
    print '%s is ok!'%file
    return insert_success,log_rows
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

###获取aws上的日志命令
def aws_log_delete(date_aws):
    aws_shell = 'rm -rf /root/hf/%s'%date_aws
    return aws_shell

#将校验数据的信息写入文件种
def  write_checkinfo(check_date,orignal_rows,success_rows,percentage):
    file_path = '/root/hf/live_user/vv'  #  存放检验文本的目录
    if not os.path.exists(file_path):
        os.mkdir(file_path)
    file_name='/root/hf/live_user/vv/vv_check.txt' #检验文本的名称
    f=open(file_name, 'a')
    print "start write checkfile"
    f.write(str(check_date)+'\t\t'+str(orignal_rows)+'\t\t'+str(success_rows)+'\t\t'+str('%.5f%%'% percentage)+'\n')
    print "write checkfile success"
    f.close()

if __name__ == '__main__':
    start_date = datetime.date(2016,7,17)###数据统计开始时间
    #获取当前时间
    d = datetime.datetime.now()
    end_date = day_get(d)##当前日期的前一天
    print "insert day is %s" %end_date
    date_mysql = end_date.strftime('%Y%m%d')###本次数据插入时间格式化(数据库表单日期格式)
    vv_date_str = datelist(start_date,end_date)###获取时间区间内的时间
    vv_daily_create(date_mysql)###建立当天的vv明细表子表
    date_aws = end_date.strftime('%Y/%m/%d')###本次数据插入时间格式化(aws中日志存放日期格式)
    aws_log_get_shell = aws_log_get(date_aws)###获取aws日志
    aws_log_get_state = shell(aws_log_get_shell)###判断shell命令是否执行
    #定义每天表的成功插入数目
    insert_success_sum=0
    #定义每天日志总量
    log_rows_sum=0
    if aws_log_get_state == 0:
        log_list = GetFileList('/root/hf/%s'%date_aws,[])
        print "log numbers is %d" %len(log_list)
        for file in log_list:
            rows_info=log_process(date_mysql,file) #获取插入总量和成功插入的条数
            insert_success_sum+=rows_info[0]
            log_rows_sum+=rows_info[1]
        # 将校验数据信息写入文件中
        percentage = insert_success_sum/ float(log_rows_sum) * 100  ####成功的百分比
        write_checkinfo(date_mysql,log_rows_sum,insert_success_sum, percentage)
        vv_create(date_str=vv_date_str)###更新当日的汇总表
        aws_log_delete_shell = aws_log_delete(date_aws)
        shell(aws_log_delete_shell)
    else:
        pass

