# -*- coding: utf-8 -*- 

import sys
import re
import urllib2
import MySQLdb
import time
import random

reload(sys)
sys.setdefaultencoding('utf-8')

"""
获取alexa上的站点排名：
1、获取站点url
2、请求alexa
3、存储结果入数据库

Args:
    url:需要提取的站点首页url，从Community表中读

Return:
    alexa排名，存储到Community中。

Created on 20161221
@author: HU Yi
"""

'''
处理数据库
'''
db_host = 'localhost'
db_username = 'root'
db_password = 'mysql'
db_database_name = 'Freebuf_Secpulse'
db_table_name = 'Community'


record_limit = 100


def getMysqlConn():
    return MySQLdb.connect(host = db_host,user = db_username,passwd = db_password,db = db_database_name,charset = "utf8")


def closeDb(database = None):
    if not database:
        database.close()


def Selecturl(id):
    mysqlConn = getMysqlConn()
    cur = mysqlConn.cursor()

    query_sql = "select id,SiteDomain from Community where id >= " + str(id) + " order by id asc limit " + str(record_limit);
    cur.execute(query_sql)
    results = cur.fetchall()
    mysqlConn.close()
    return results


def Updatealexa(id,alexa):
    mysqlConn = getMysqlConn()
    cur = mysqlConn.cursor()

    query_sql = "update Community set alexa = " + str(alexa) + " where id = " + str(id) ;
    cur.execute(query_sql)
    mysqlConn.commit()
    mysqlConn.close()


def get_alexa_rank(url):
    try:
        data = urllib2.urlopen('http://data.alexa.com/data?cli=10&dat=snbamz&url=%s' % (url)).read()
        #print data
        reach_rank = re.findall("REACH[^\d]*(\d+)", data)
        if reach_rank: reach_rank = reach_rank[0]
        else: reach_rank = -1

        popularity_rank = re.findall("POPULARITY[^\d]*(\d+)", data)
        if popularity_rank: popularity_rank = popularity_rank[0]
        else: popularity_rank = -1

        return int(popularity_rank)

    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        return None



if __name__ == '__main__':
    start_id = 14250
    while 1:  
        results = Selecturl(start_id)
        if results:
            for result in results:
                url = result[1]
                data = get_alexa_rank(url)
                popularity_rank = -1
                if data:
                    popularity_rank = data
                    print result[1]
                    print 'popularity rank = %s' % (popularity_rank)
                    Updatealexa(result[0],popularity_rank)
                time.sleep(random.randint(0,3))
            start_id += 100
        else:
            break
    print "Finish"



  
