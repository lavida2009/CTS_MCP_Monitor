#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/3/14 15:59
# @Author  : Evan
# @Site    : 
# @File    : CTS_Monitor_checkDataBase.py.py
# @Software: PyCharm
# 利用FY4A SSI（DirSSI、DifSSI以及CFR云检测，进行判断）

import datetime
import os

import pandas as pd
import pymysql
# 显示所有列
from apscheduler.schedulers.blocking import BlockingScheduler

# 定时任务

pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)
# 设置value的显示长度为100，默认为50
pd.set_option('max_colwidth', 100)


def main():
    # 获取当前脚本所在目录
    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)

    # Get a datetime object
    datetime_utc = datetime.datetime.now() - datetime.timedelta(hours=9)
    part_day = datetime_utc.day
    datetime_utc_str = datetime_utc.strftime("%Y-%m-%d %H:%M:%S")
    # 整点前一个时次
    datetime_utc_00_str = datetime_utc_str.replace(datetime_utc_str[-5:], '00:00')
    # 整点前半小时时次
    datetime_utc_00 = datetime.datetime.strptime(datetime_utc_00_str, "%Y-%m-%d %H:%M:%S")
    datetime_utc_30 = datetime_utc_00 + datetime.timedelta(minutes=30)
    datetime_utc_30_str = datetime_utc_30.strftime("%Y-%m-%d %H:%M:%S")
    # 天气现象智能观测仪 天气现象智能观测仪视频文件
    wlrd_mp4_file_type_code = 'A.0001.0055.R001'
    # 天气现象智能观测仪 天气现象智能观测仪图片文件
    wlrd_jpg_file_type_code = 'A.0001.0056.R001'

    sql_jpg = "SELECT c.区站号,count(*) as 图片文件数目 FROM (select SUBSTR(FILE_NAME,10,5) AS 区站号 FROM TB_SEND_FILE WHERE" \
              " PART_DAY=%d and DATA_TYPE_C='%s' and PRODUCT_TIME in ('%s', '%s') and SEND_USER='BABJ') c GROUP BY 区站号" \
              % (part_day, wlrd_jpg_file_type_code, datetime_utc_00_str, datetime_utc_30_str)

    sql_mp4 = "SELECT c.区站号,count(*) as 视频文件数目 FROM (select SUBSTR(FILE_NAME,10,5) AS 区站号 FROM TB_SEND_FILE WHERE" \
              " PART_DAY=%d and DATA_TYPE_C='%s' and PRODUCT_TIME in ('%s', '%s') and SEND_USER='BABJ') c GROUP BY 区站号" \
              % (part_day, wlrd_mp4_file_type_code, datetime_utc_00_str, datetime_utc_30_str)
    """
    
    sql_jpg = "SELECT c.区站号,count(*) as 图片文件数目 FROM (select SUBSTR(FILE_NAME,10,5) AS 区站号 FROM TB_SEND_FILE WHERE" \
              " PART_DAY=%d and DATA_TYPE_C='%s' and PRODUCT_TIME = '2021-03-15 01:30:00' and SEND_USER='BABJ') c GROUP BY 区站号" \
              % (part_day, wlrd_jpg_file_type_code)
    
    sql_mp4 = "SELECT c.区站号,count(*) as 视频文件数目 FROM (select SUBSTR(FILE_NAME,10,5) AS 区站号 FROM TB_SEND_FILE WHERE" \
              " PART_DAY=%d and DATA_TYPE_C='%s' and PRODUCT_TIME = '2021-03-15 01:30:00' and SEND_USER='BABJ') c GROUP BY 区站号" \
              % (part_day, wlrd_mp4_file_type_code)
    """
    # 连接 CTS_MCP Mysql监控数据库，读取数据
    mysql_conn = pymysql.connect(host='', port=3306, user='', password='', db='',
                                 charset='utf8')
    df_mp4 = pd.read_sql(sql_mp4, con=mysql_conn)
    df_jpg = pd.read_sql(sql_jpg, con=mysql_conn)
    df_mp4[['区站号']].astype(str)
    df_jpg[['区站号']].astype(str)
    df_db_out = pd.merge(df_mp4, df_jpg, how='left', on='区站号')

    mysql_conn.close()

    # 读取国家站站点信息
    nataws_info_filename = 'NATAWS_stationinfo.csv'
    nataws_info_filedir = os.path.join(script_dir, nataws_info_filename)

    df_nataws_info = pd.read_csv(nataws_info_filedir, dtype={'区站号': str})

    # 合并站点信息df和数据库查询df

    df_checkdb_result = pd.merge(df_nataws_info, df_db_out, how='left', on='区站号')
    # 将所有NaN替换为0
    df_checkdb_result.fillna(0, inplace=True)
    # 排序
    # df_checkdb_result.sort_values(by=['视频文件数目', '图片文件数目'], axis=0, ascending=(True, True))
    df_checkdb_result.sort_values(by=['视频文件数目', '图片文件数目'], axis=0, ascending=[True, True], inplace=True)
    # df_checkdb_result['视频文件数目'] < 3 or df_checkdb_result['图片文件数目'] < 3
    # print(df_checkdb_result[df_checkdb_result['视频文件数目'] < 3 or df_checkdb_result['图像文件数目'] < 3])
    df_checkdb_result[['视频文件数目', '图片文件数目']].astype(int)

    print(df_checkdb_result[(df_checkdb_result['视频文件数目'] < 6) | (df_checkdb_result['图片文件数目'] < 6)])

    sending_sms(df_checkdb_result, script_dir)


'''
sending_sms(df)
发送告警短信
dataout_df ： 'station_id', 'datetime','warning_info', 'waterlevel', 'warning_content', 'phoneNumber'
'''


def sending_sms(df, script_dir):
    df_warning = df[(df['视频文件数目'] < 6) | (df['图片文件数目'] < 6)]
    # 读取站点联系方式
    station_contact_filename = 'station_contact_1.csv'
    station_contact_filedir = os.path.join(script_dir, station_contact_filename)
    df_station_contact = pd.read_csv(station_contact_filedir, dtype={'区站号': str})

    if not df_warning.empty:

        df_warning[['区站号']].astype(str)
        # print(df_warning.dtypes)
        df_warning = pd.merge(df_warning, df_station_contact[['区站号', '联系方式', '测试']], how='left', on='区站号')

        indb_value_list = []
        for iiiii in df_warning['区站号'].tolist():
            # list_warning : ['站名', '区站号', '视频文件数目', '图片文件数目', '联系方式', '测试']
            list_warning = df_warning[df_warning['区站号'] == iiiii].values.tolist()[0]
            # 普通短信
            sms_type = 'O'
            # 联系方式
            sms_recipient = list_warning[4]

            sms_text = '【信息保障中心提醒】：%s/%s, 过去一小时天气现象智能识别仪文件上传情况为 视频文件数目 %d, 图片文件数目 %d, 请检查。' % \
                       (list_warning[1], list_warning[0], list_warning[2], list_warning[3])
            # sms_text = str(' '.join([iiiii, ',' + str(bjt.strftime('%Y-%m-%d %H:%M:%S')) + '(BJT),' + '蒸发水位为' + str(
            #    round(sms_content_list[3], 1)) + 'mm,', sms_content_list[4]]))
            now = datetime.datetime.now()
            sms_create_date = now.strftime("%Y-%m-%d %H:%M:%S")
            # 中文短信
            sms_encoding = 'U'
            sms_userid = ''

            indb_value_list.append((sms_type, sms_recipient, sms_text, sms_create_date, sms_encoding, sms_userid))

        # 连接金笛短信平台Mysql数据库，使用向库中smsserver_out表写入数据的方式，发送短信
        mysql_conn = pymysql.connect(host='', port=3308, user='', password='',
                                     db='',
                                     charset='utf8')
        # 插入数据
        sql = "INSERT INTO Smsserver_out (type, recipient, text, create_date, encoding, user_id) VALUES(%s, %s, %s, %s, %s, %s)"

        try:
            with mysql_conn.cursor() as cursor:
                cursor.executemany(sql, indb_value_list)
        except Exception as e:
            mysql_conn.rollback()

        mysql_conn.commit()
        mysql_conn.close()
        print('Sending SMS OK')
    # 82站传输无异常，短信通知值班手机
    else:
        # 连接短信平台Mysql数据库，使用向库中smsserver_out表写入数据的方式，发送短信
        mysql_conn = pymysql.connect(host='', port=3308, user='', password='',
                                     db='', charset='utf8')
        # 普通短信
        sms_type_1 = 'O'
        # 联系方式
        # sms_recipient = ''
        sms_recipient_1 = ''
        # 短信内容
        sms_text_1 = '【信息保障中心提醒】：过去一小时,全省天气现象智能识别仪,文件上传情况无异常。'
        sms_create_date_1 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # 中文短信
        sms_encoding_1 = 'U'
        sms_userid_1 = ''

        indb_value_list_1 = [sms_type_1, sms_recipient_1, sms_text_1, sms_create_date_1, sms_encoding_1, sms_userid_1]

        # 插入数据
        sql = "INSERT INTO Smsserver_out (type, recipient, text, create_date, encoding, user_id) VALUES (%s, %s, %s, %s, %s, %s)"
        try:
            with mysql_conn.cursor() as cursor:
                cursor.executemany(sql, indb_value_list_1)
        except Exception as e:
            mysql_conn.rollback()

        mysql_conn.commit()
        mysql_conn.close()
        print('Sending SMS OK')



if __name__ == '__main__':
    __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"

    start_time = datetime.datetime.now()
    scheduler = BlockingScheduler()
    scheduler.add_job(main, 'cron', minute=3)
    scheduler.start()

    main()
    #end_time_2 = datetime.datetime.now()

    #print('整个程序运行时间 :', end_time_2 - start_time)
