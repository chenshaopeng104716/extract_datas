#coding:utf-8
'''this module process the mobile pv raw data
__author__ = 'hf'
'''
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
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

###获取来源pv数据
def pv_from_data_get(start_date,end_date,bid,url,url_or_ref,another):
    conn=psycopg2.connect(database="dm_result",user="mf_readonly",password="mf_readonly1234",host="10.100.5.43",port="2345")
    sql="select b.date,b.bid,b.ref,b.url,sum(b.uv) as uv,sum(b.pv) as pv from (select a.date,a.bid,case when a.rank<='100' then a.%s else '其他页' end as %s,a.%s,a.uv,a.pv " \
        "from (select date,bid,ref,url,pv,uv,rank() over (partition by date,bid ORDER BY pv desc) as rank from url_r_ref_pv_day " \
        "where date>='%s' and date<='%s' and bid in %s and %s='%s') a) b " \
        "GROUP BY b.date,b.bid,b.ref,b.url ORDER BY b.date,b.bid,sum(b.pv) desc;"%(another,another,url_or_ref,start_date,end_date,bid,url_or_ref,url)
    pgdata = pd.read_sql(sql,conn)
    conn.close()
    return  pgdata
###返回url,ref
def extract_another(url_or_ref,k):
    for j in url_or_ref:
        if j == k:
            pass
        else:
            return k,j
###url处理
def url_process(url):
    url_type = []
    if url == '':
        url_type.extend([u'启动app',''])
        return url_type
    else:
        try:
            url_split = url.split('||')
            if len(url_split)==1:
                url_type.extend([url_split[0],''])
                return url_type
            else:
                url_type.extend([url_split[0],url_split[1]])
                return url_type
        except Exception,ex:
            print Exception,":",ex
            pass
###获取媒资名称
def mpp_mysql():
    conn = MySQLdb.connect(host="10.100.5.41",user="app_hefang",passwd="app_hefang1234",db="cms",charset="utf8")
    sql="SELECT a.id as vid,a.title as vid_title,a.pid,b.title as pid_title,c.id as cid,c.classcn as cid_title " \
        "from hunantv_v_videos a,hunantv_v_collection b,hunantv_v_class c " \
        "where a.pid=b.id and a.rootid=c.id;"
    mysqldata = pd.read_sql(sql,conn)
    conn.close()
    return mysqldata
###获取查询页面信息
def search_url_info(start_date,end_date,bid,url):
    conn=psycopg2.connect(database="dm_result",user="mf_readonly",password="mf_readonly1234",host="10.100.5.43",port="2345")
    sql="select date,bid,pv,entrypagecount,exitpagecount,uv from url_pv_day where date>='%s' and date<='%s' and bid in %s and url='%s'"%(start_date,end_date,bid,url)
    pgdata = pd.read_sql(sql,conn)
    conn.close()
    return  pgdata
###查询vid对应vv
def ref_vv(vid_str,bid_mobile,start_date,end_date):
    conn=psycopg2.connect(database="mofang_dota",user="mf_readonly",password="mf_readonly1234",host="10.100.5.43",port="2345")
    sql="select * from vid_vv_day where bid in %s and vid in %s and date>='%s' and date<='%s'"%(bid_mobile,vid_str,start_date,end_date)
    print sql
    pgdata = pd.read_sql(sql,conn)
    conn.close()
    return  pgdata
###查询vid对应pv
def ref_pv(target_str,bid,start_date,end_date):
    conn=psycopg2.connect(database="dm_result",user="mf_readonly",password="mf_readonly1234",host="10.100.5.43",port="2345")
    sql="select * from url_pv_day where bid in %s and url in %s and date>='%s' and date<='%s';"%(bid,target_str,start_date,end_date)
    pgdata = pd.read_sql(sql,conn)
    conn.close()
    return  pgdata
###pv来源数据计算并入库
def pv_from_data_process(data,process_url='url'):
    pv_from_data = data
    url_1_dict = {u'点播':u'播放页',u'演唱会直播':u'播放页',u'完美假期直播':u'播放页',u'电视直播':u'播放页',u'原生弹幕直播页':u'播放页',u'播放记录':u'其他页',u'收藏':u'其他页',
                  u'评论':u'其他页',u'会员中心':u'其他页',u'我的卡券':u'其他页',u'会员卡兑换':u'其他页',u'活动':u'其他页',u'设置':u'其他页',u'意见反馈':u'其他页',u'剧集列表':u'其他页',
                  u'开通会员':u'其他页',u'我的观影券':u'其他页',u'我的订单':u'其他页',u'会员片库':u'其他页',u'个人资料编辑':u'其他页',u'关于':u'其他页',u'片库页面类':u'其他页',
                  u'搜索':u'功能页',u'正在缓存页':u'功能页',u'我的':u'功能页',u'已经缓存页':u'功能页',u'启动app':u'功能页',u'搜索结果页':u'功能页',u'登陆页':u'功能页',u'离线播放':u'功能页',
                  u'微信登陆':u'功能页',u'腾讯登陆':u'功能页',u'新浪登陆':u'功能页',u'芒果TV登陆':u'功能页',u'缓存空白页':u'功能页',u'首页频道页':u'频道运营页',u'王牌强档':u'频道运营页',
                  u'频道管理':u'频道运营页',u'专题':u'频道运营页',u'H5页面类':u'频道运营页'}
    pv_from_data['url_1'] = pd.Series()
    pv_from_data['vid_title'] = pd.Series()
    pv_from_data['url_2'] = pd.Series()
    pv_from_data['url_1'] = pv_from_data['url_1'].astype('object')
    pv_from_data['vid_title'] = pv_from_data['vid_title'].astype('object')
    pv_from_data['url_2'] = pv_from_data['url_2'].astype('object')
    print 'start to process pv_from_data!'
    for i in range(len(pv_from_data[process_url])):
        url = pv_from_data.iloc()[i][process_url]
        pv_from_data.loc[i,'url_1'] = url_process(url)[0]
        pv_from_data.loc[i,'vid_title'] = url_process(url)[1]
        try:
            pv_from_data.loc[i,'url_2'] = url_1_dict[unicode(url_process(url)[0])]
        except:
            pv_from_data.loc[i,'url_2'] = u'top100以外页面合集'
        if i%100==0:
            print 'process %s is OK!'%i
    print 'pv_from_data process is ok!'
    return pv_from_data

#创建新文件
def new_filepath(search_path_all):
    if not os.path.exists(search_path_all):
        print 'search_path is not exists,create new filepath!'
        os.makedirs(search_path_all.replace('\n',''))
    else:
        pass

###计算pv来源、去向汇总页面
def from_to_url(start_date, end_date, pv_all_data_url, pv_all_data_ref, search_path, search_path_all):
    dataframe_target = pd.DataFrame({u'开始日期': '', u'结束日期': '', u'来源': [''], u'去向': ['']})
    dataframe_target[u'开始日期'][0] = start_date
    dataframe_target[u'结束日期'][0] = end_date
    dataframe_target[u'来源'][0] = pv_all_data_url['pv'].sum().astype('int')
    dataframe_target[u'去向'][0] = pv_all_data_ref['pv'].sum().astype('int')
    dataframe_target = dataframe_target.loc[:, [u'开始日期', u'结束日期', u'来源', u'去向']]
    file_name = search_path_all + '\\' + search_path + u'_来源_去向.xlsx'
    dataframe_target.to_excel(file_name, sheet_name='sheet1')
###计算退出率
def exitpage_rate(search_info,search_path,search_path_all):
    dataframe_exitpage = search_info.loc[:,['date','pv','exitpagecount']]
    dataframe_date = dataframe_exitpage.groupby(dataframe_exitpage['date']).sum()
    dataframe_date['exitpage_rate'] = pd.Series()
    for i in dataframe_date.index:
        dataframe_date.loc[i,'exitpage_rate'] = round(dataframe_date.loc[i,'exitpagecount']/dataframe_date.loc[i,'pv'],4)
    dataframe_date.index.name = ''
    file_name = search_path_all+'\\'+search_path+u'_页面退出率.xlsx'
    dataframe_date.to_excel(file_name,sheet_name='sheet1')
###计算来源、去向一级分类汇总
def ref_url_1(search_path,search_path_all):
    for i in ['pv_all_data_url','pv_all_data_ref']:
        dataframe = eval(i)
        dataframe_target = dataframe.loc[:,['url_1','pv']]
        dataframe_target = dataframe_target.groupby(dataframe_target['url_1']).sum()
        dataframe_target.index.name = ''
        if i == 'pv_all_data_url':
            file_name = search_path_all+'\\'+search_path+u'_来源_一级分类汇总.xlsx'
            dataframe_target.to_excel(file_name,sheet_name='sheet1')
        elif i == 'pv_all_data_ref':
            file_name = search_path_all+'\\'+search_path+u'_去向_一级分类汇总.xlsx'
            dataframe_target.to_excel(file_name,sheet_name='sheet1')

###计算来源、去向一级分类的页面类型汇总
def ref_url_2(search_path, search_path_all):
    for i in ['pv_all_data_url', 'pv_all_data_ref']:
        dataframe = eval(i)
        dataframe_target = dataframe.loc[:, ['url_2', 'pv']]
        dataframe_target = dataframe_target.groupby(dataframe_target['url_2']).sum()
        dataframe_target.index.name = ''
        if i == 'pv_all_data_url':
            file_name = search_path_all + '\\' + search_path + u'_来源_页面类型汇总.xlsx'
            dataframe_target.to_excel(file_name, sheet_name='sheet1')
        elif i == 'pv_all_data_ref':
            file_name = search_path_all + '\\' + search_path + u'_去向_页面类型汇总.xlsx'
            dataframe_target.to_excel(file_name, sheet_name='sheet1')

###对文件夹内的文件进行压缩
def zip_dir(dirname, zipfilename):
    filelist = []
    if os.path.isfile(dirname):
        filelist.append(dirname)
    else:
        for root, dirs, files in os.walk(dirname):
            for name in files:
                filelist.append(os.path.join(root, name))

    zf = zipfile.ZipFile(zipfilename, "w", zipfile.zlib.DEFLATED)
    for tar in filelist:
        arcname = tar[len(dirname):]
        # print arcname
        zf.write(tar, arcname)
    zf.close()





###查询参数定义
start_date = '20161024'    #修改修改
end_date = '20161030'        #修改修改
#search_url=['王牌强档']
search_url=['首页频道页||精选','首页频道页||综艺','首页频道页||电视剧','首页频道页||电影','首页频道页||动漫','王牌强档']
bid = "('12','9')"
bid_mobile = ('iphone','android')
url_or_ref= ['url','ref']

#只需要第一次执行
mpp_mysql_data = mpp_mysql()
mpp_filepath = u'E:\\工作\\临时需求\\PV上下游\\mobile\\mpp_data.xlsx'
mpp_mysql_data.to_excel(mpp_filepath, sheet_name='sheet1')
for cishu in range(len(search_url)):
   #初始化
    vid_str = ''
    vid_list = list()
    target_str = ''
    target_list = list()
    search_info = search_url_info(start_date=start_date,end_date=end_date,bid=bid,url=search_url[cishu])
    search_info_path = u'E:\\工作\\临时需求\\PV上下游\\mobile\\search_info.xlsx'
    search_info.to_excel(search_info_path,sheet_name='sheet1')
    for k in url_or_ref:
        if k == 'url':
            print 'start to get ref_data from new_mofang!'
            j = extract_another(url_or_ref=url_or_ref,k=k)[1]
            data_url = pv_from_data_get(start_date=start_date,end_date=end_date,bid=bid,url=search_url[cishu],url_or_ref=k,another=j)
            pv_all_data_url = pv_from_data_process(data=data_url,process_url='ref')
            pv_all_data_url_filepath = u'E:\\工作\\临时需求\\PV上下游\\mobile\\pv_all_data_url.xlsx'
            pv_all_data_url.to_excel(pv_all_data_url_filepath,sheet_name='sheet1')
        elif k== 'ref':
            print 'start to get url_data from new_mofang!'
            j = extract_another(url_or_ref=url_or_ref,k=k)[1]
            data_ref = pv_from_data_get(start_date=start_date,end_date=end_date,bid=bid,url=search_url[cishu],url_or_ref=k,another=j)
            pv_all_data_ref = pv_from_data_process(data=data_ref,process_url='url')
            pv_all_data_ref_filepath = u'E:\\工作\\临时需求\\PV上下游\\mobile\\pv_all_data_ref.xlsx'
            pv_all_data_ref.to_excel(pv_all_data_ref_filepath,sheet_name='sheet1')
    dataframe_concat = pd.concat([pv_all_data_url,pv_all_data_ref])###将两个数据表列向合并
    dataframe_concat = dataframe_concat.ix[:,['url_1','vid_title']]
    dataframe_concat = dataframe_concat[(dataframe_concat['url_1']=='点播') | (dataframe_concat['url_1']=='离线播放')]
    cms_dataframe = mpp_mysql_data.ix[:,['vid','vid_title']]###获取媒资vid,vid_title
    if len(dataframe_concat)>=1:
        for i in range(len(dataframe_concat)):
            try:
                vid_title = dataframe_concat.iloc()[i]['vid_title']
                vid = cms_dataframe[cms_dataframe['vid_title']==vid_title].iloc[0]['vid']
                if vid not in vid_list:
                    vid_list.append(vid)
                else:
                    pass
                if i%100 == 0:
                    print '%s is complete!'%i
            except:
                pass
        for vid in vid_list:
            vid_str = vid_str+",'"+str(vid)+"'"
        vid_str = vid_str[1:]
        vid_str = '('+vid_str+')'
        ref_vv_data = ref_vv(vid_str,bid_mobile,start_date,end_date)
        ref_vv_data = pd.merge(ref_vv_data,cms_dataframe,on='vid')
    else:
        ref_vv_data = pd.DataFrame()
    ref_vv_path = u'E:\\工作\\临时需求\\PV上下游\\mobile\\ref_vv_data.xlsx'
    ref_vv_data.to_excel(ref_vv_path,sheet_name='sheet1')
    for target in pv_all_data_url['ref']:
        if target not in target_list:
            target_list.append(target)
        else:
            pass
    for target in pv_all_data_ref['url']:
        if target not in target_list:
            target_list.append(target)
        else:
            pass
    for target in target_list:
        if "'" in target:
            target = re.sub("'","\\'",target)
        target_str = target_str+",'"+target+"'"
    target_str = target_str[1:]
    target_str = '('+target_str+')'
    ref_pv_data = ref_pv(target_str,bid,start_date,end_date)
    ref_pv_path = u'E:\\工作\\临时需求\\PV上下游\\mobile\\ref_pv_data.xlsx'
    ref_pv_data.to_excel(ref_pv_path,sheet_name='sheet1')
    print 'all data is ok!'
    # ###戴明叶需求数据导出
    # ###创建本次查询新文件夹
    search_path = start_date+'_'+end_date+'_'+search_url[cishu].replace('||','_')
    search_path_all = u'E:\\工作\\临时需求\\PV上下游\\mobile\\%s'%search_path


    def new_filepath(search_path_all):
        if not os.path.exists(search_path_all):
            print 'search_path is not exists,create new filepath!'
            os.makedirs(search_path_all.replace('\n', ''))
        else:
            pass

    new_filepath(search_path_all)

    ###计算pv来源、去向汇总页面
    from_to_url(start_date, end_date, pv_all_data_url, pv_all_data_ref, search_path, search_path_all)
    ###计算退出率
    exitpage_rate(search_info, search_path, search_path_all)
    ###计算来源、去向一级分类汇总
    ref_url_1(search_path, search_path_all)
    ###计算来源、去向一级分类的页面类型汇总
    ref_url_2(search_path, search_path_all)
    ###对文件夹内的文件进行压缩
    zip_dir(search_path_all, search_path_all+ u".zip")
    print 'data zip is ok!'



