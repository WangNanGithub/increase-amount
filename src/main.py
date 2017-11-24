#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
    python script, use to repair bug
    PYTHON 脚本, 用来修复一些数据问题的 BUG
"""


import MySQLdb
import numpy as np
import pandas as pd
import json
import time
import oss2
import os
import logging

#########################################################################################################################################
"""
    日志
"""

# 创建一个logger
logger = logging.getLogger('my-logger')

# 设置日志输出级别
logger.setLevel(logging.DEBUG)

# 创建一个handler，用于写入日志文件
file_handler = logging.FileHandler('message.log')
file_handler.setLevel(logging.INFO)

# 定义handler的输出格式
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# 给logger添加handler
logger.addHandler(file_handler)

#########################################################################################################################################
"""
    数据库
"""

# 数据库配置
db_info = {
    'url': 'rm-2zeep0s2kx565dbojrw.mysql.rds.aliyuncs.com',
    'username': 'pdl',
    'password': 'waB*RJ4Gytn9L#Z3azYM4R2DT3hEMsu@',
    'database': 'pdl',
    'port': 3306
}


# 获取数据库连接
def open_connection():
    db = MySQLdb.connect(db_info['url'], db_info['username'], db_info['password'], db_info['database'], db_info['port'], charset='utf8')
    return db


# 关闭数据库连接
def close_connection(db):
    db.close()


# 执行SQL语句返回查询结果
def select(sql, db=None):
    flag = False
    try:
        if db is None:
            flag = True
            db = open_connection()
        cursor = db.cursor()
        cursor.execute(sql)
        data = np.array(cursor.fetchall())
        if data.__len__() <= 0:
            return None
        pd_df = pd.DataFrame(data, columns=zip(*cursor.description)[0])
        return pd_df
    finally:
        if flag:
            close_connection(db)


# 插入或者更新数据
def insert(sql, param, db=None):
    flag = False
    try:
        if db is None:
            flag = True
            db = open_connection()
        cursor = db.cursor()
        cursor.execute(sql, param)
        db.commit()
    finally:
        if flag:
            close_connection(db)

#########################################################################################################################################
"""
    OSS
"""

# OSS配置
oss_info = {
    'ACCESS_KEY_ID': 'LTAIpQ5fEBYV2RY7',
    'ACCESS_KEY_SECRET': 'VRp9JoYKDJqYk1gwrKaS7VAiAMCnJL',
    'BUCKET': 'pdl',
    'ENDPOINT': 'http://oss-cn-beijing-internal.aliyuncs.com'
}


def download(file_path):
    # 阿里认证
    access_key_id = os.getenv('OSS_TEST_ACCESS_KEY_ID', oss_info['ACCESS_KEY_ID'])
    access_key_secret = os.getenv('OSS_TEST_ACCESS_KEY_SECRET', oss_info['ACCESS_KEY_SECRET'])
    bucket_name = os.getenv('OSS_TEST_BUCKET', oss_info['BUCKET'])
    endpoint = os.getenv('OSS_TEST_ENDPOINT', oss_info['ENDPOINT'])
    bucket = oss2.Bucket(oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_name)

    # 检查文件在系统中是否存在
    exist = bucket.object_exists(file_path)
    if exist:
        result = bucket.get_object(file_path)
        content = b''
        for chunk in result:
            content += chunk
        return content
    else:
        return None


def down_to_local(file_path, local_file_path):
    # 阿里认证
    access_key_id = os.getenv('OSS_TEST_ACCESS_KEY_ID', oss_info['ACCESS_KEY_ID'])
    access_key_secret = os.getenv('OSS_TEST_ACCESS_KEY_SECRET', oss_info['ACCESS_KEY_SECRET'])
    bucket_name = os.getenv('OSS_TEST_BUCKET', oss_info['BUCKET'])
    endpoint = os.getenv('OSS_TEST_ENDPOINT', oss_info['ENDPOINT'])
    bucket = oss2.Bucket(oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_name)

    # 检查文件在系统中是否存在
    exist = bucket.object_exists(file_path)
    if exist:
        bucket.get_object_to_file(file_path, local_file_path)
        return True
    else:
        return False

#########################################################################################################################################
"""
    逻辑代码
"""


def execute():
    logger.info('raise money start ...')
    total_num, success_num, failure_num = 0, 0, 0
    error_record = pd.DataFrame(columns=[u'手机号', u'额度'])
    file_list = ['../resource/100.xls', '../resource/200.xls', '../resource/300.xls', '../resource/400.xls', '../resource/500.xls', ]
    for f in file_list:
        logger.info(f)
        quota = int(f[-7:-4])
        logger.info('start raise money %s ...', quota)

        data = pd.read_excel(f, sheetname='Sheet1', skiprows=0)
        for i in range(len(data)):
            mobile = data[u'手机号'][i]
            total_num += 1

            user_sql = 'SELECT `id` FROM `pdl_user_basic` WHERE `mobile` = %s' % mobile
            user = select(user_sql)

            try:
                if user is None:
                    failure_num += 1
                    error_record.loc[error_record.shape[0] + 1] = {u'手机号': mobile, u'额度': quota}
                    logger.error("could't find user with the phone number is %s", mobile)
                    continue

                raised_sql = 'SELECT * FROM `pdl_user_raise_amount` WHERE `mobile` = %s' % mobile
                raise_result = select(raised_sql)

                if raise_result is None:
                    raise_sql = 'insert into `pdl_user_raise_amount` (user_id, mobile, amount, period ) values (%s, %s, %s, %s)'
                    param = (user['id'][0], mobile, 1000 + quota, '14, 21')
                    insert(raise_sql, param)
                else:
                    raise_sql = 'UPDATE `pdl_user_raise_amount` SET `amount` = amount + %s WHERE `user_id` = %s'
                    param = (quota, user['id'][0])
                    insert(raise_sql, param)

                success_num += 1
                logger.info('%s raise amount success, amount : %s, mobile : %s' % (user['id'][0], quota, mobile))
            except Exception, e:
                failure_num += 1
                error_record.loc[error_record.shape[0] + 1] = {u'手机号': mobile, u'额度': quota}
                logger.error('%s raise amount failure, amount : %s, mobile : %s, error : %s', *(user['id'][0], quota, mobile, e))

    logger.info('raise money end, total count : %s, success count : %s, failure count : %s', *(total_num, success_num, failure_num))
    error_record.to_csv('../raise_amount_error.csv', mode='a+', encoding='utf-8', header=True, index=False, index_label=None)

#########################################################################################################################################
"""
    执行入口
"""
if __name__ == '__main__':
    execute()
