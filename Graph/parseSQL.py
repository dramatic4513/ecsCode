# 用于解析sql语句，得到参与连接的表和属性
# 根据属性的开头分别该属性属于哪一个表
import sqlite3
import sqlparse
import os
import re


# 输入一个存有sql语句的文件路径，得到这个sql中=两端连接的属性
def parse_sql(filename):
    sql = ""
    with open(filename) as f:
        for line in f:
            sql += line
    # 利用正则表达式找到等号两端的单词
    # matches 是一个列表，里面是两个属性组成的列表
    matches = re.findall(r'\b(\w+)\s*=\s*(\w+)\b', sql)
    # 如果等号两端是数字，则跳过
    columns = []
    for match in matches:
        key, value = match
        # if not key.isdigit() and not value.isdigit() and key.find('_') == 1 and value.find('_') == 1:
        if not key.isdigit() and not value.isdigit():
            columns.append(match)
    return columns


# 根据存储sql语句文件夹的位置，然后决定哪些文件被解析，然后调用解析函数解析sql语句，得到解析后的属性列表
def folder_sql_to_columns():
    root_path = '/home/postgres/tpc/tpch/SQL/queries'
    columns = []
    for filename in os.listdir(root_path):
        # if filename not in ['db21.sql', 'db22.sql']:
        #     continue
        if filename in ['db20.sql']:
            continue
        query_path = os.path.join(root_path, filename)
        columns += parse_sql(query_path)  # 获得当前sql的连接属性
    return columns


#生成表和属性特征对应的字典
def table_columns_dict():
    file_path = '/home/postgres/tpc/tpch/SQL/createTable.txt'
    string = ""
    tc_dict = {}
    with open(file_path) as f:
        for line in f:
            string += line
    string.strip()
    sqls = string.split(';')
    for sql in sqls:
        print(sql)
        sql.strip()
        words = sql.split(" ")
        if (words.__len__() < 3):
            break
        vale = words[2]
        key = words[-1][5:-1].split("_")[0] + "_"
        print(vale)
        print(key)
        tc_dict[key] = vale
        print(tc_dict)
    # table_columns_dict = {'N_': 'NATION', 'R_': 'REGION', 'P_': 'PART', 'S_': 'SUPPLIER', 'PS_': 'PARTSUPP',
    #                       'C_': 'CUSTOMER', 'O_': 'ORDERS', 'L_': 'LINEITEM'}

def tpcds_table_size():
    file_path = '/home/postgres/code/data/tcpds_table_name.txt'
    with open(file_path) as f:
        for line in f:
            table = line.strip().split(".")[0]
            # sql_string = "pg_size_pretty(pg_total_relation_size(\'" + table + '\')) AS \"'table + '\",'
            sql = "pg_size_pretty(pg_total_relation_size('{0}')) AS \"{0}\"".format(table)
            print(sql + ',')

if __name__ == '__main__':
    tpcds_table_size()
    # columns = folder_sql_to_columns()
    # for column in columns:
    #     print(column)
    # table_columns_dict = {'N_': 'NATION', 'R_': 'REGION', 'P_': 'PART', 'S_': 'SUPPLIER', 'PS_': 'PARTSUPP',
    #                       'C_': 'CUSTOMER', 'O_': 'ORDERS', 'L_': 'LINEITEM'}




"""
查询数据库中的表名，并且返回结果的属性为表名
    SELECT
        pg_size_pretty(pg_total_relation_size('NATION')) AS "NATION",
        pg_size_pretty(pg_total_relation_size('REGION')) AS "REGION",
        pg_size_pretty(pg_total_relation_size('PART')) AS "PART",
        pg_size_pretty(pg_total_relation_size('SUPPLIER')) AS "SUPPLIER",
        pg_size_pretty(pg_total_relation_size('PARTSUPP')) AS "PARTSUPP",
        pg_size_pretty(pg_total_relation_size('CUSTOMER')) AS "CUSTOMER",
        pg_size_pretty(pg_total_relation_size('ORDERS')) AS "ORDERS",
        pg_size_pretty(pg_total_relation_size('LINEITEM')) AS "LINEITEM";
"""

'''
SELECT 
pg_size_pretty(pg_total_relation_size('call_center')) AS "call_center",
pg_size_pretty(pg_total_relation_size('catalog_page')) AS "catalog_page",
pg_size_pretty(pg_total_relation_size('catalog_returns')) AS "catalog_returns",
pg_size_pretty(pg_total_relation_size('catalog_sales')) AS "catalog_sales",
pg_size_pretty(pg_total_relation_size('customer_address')) AS "customer_address",
pg_size_pretty(pg_total_relation_size('customer')) AS "customer",
pg_size_pretty(pg_total_relation_size('customer_demographics')) AS "customer_demographics",
pg_size_pretty(pg_total_relation_size('date_dim')) AS "date_dim",
pg_size_pretty(pg_total_relation_size('dbgen_version')) AS "dbgen_version",
pg_size_pretty(pg_total_relation_size('household_demographics')) AS "household_demographics",
pg_size_pretty(pg_total_relation_size('income_band')) AS "income_band",
pg_size_pretty(pg_total_relation_size('inventory')) AS "inventory",
pg_size_pretty(pg_total_relation_size('item')) AS "item",
pg_size_pretty(pg_total_relation_size('promotion')) AS "promotion",
pg_size_pretty(pg_total_relation_size('reason')) AS "reason",
pg_size_pretty(pg_total_relation_size('ship_mode')) AS "ship_mode",
pg_size_pretty(pg_total_relation_size('store')) AS "store",
pg_size_pretty(pg_total_relation_size('store_returns')) AS "store_returns",
pg_size_pretty(pg_total_relation_size('store_sales')) AS "store_sales",
pg_size_pretty(pg_total_relation_size('time_dim')) AS "time_dim",
pg_size_pretty(pg_total_relation_size('warehouse')) AS "warehouse",
pg_size_pretty(pg_total_relation_size('web_page')) AS "web_page",
pg_size_pretty(pg_total_relation_size('web_returns')) AS "web_returns",
pg_size_pretty(pg_total_relation_size('web_sales')) AS "web_sales",
pg_size_pretty(pg_total_relation_size('web_site')) AS "web_site";SELECT 
pg_size_pretty(pg_total_relation_size('call_center')) AS "call_center",
pg_size_pretty(pg_total_relation_size('catalog_page')) AS "catalog_page",
pg_size_pretty(pg_total_relation_size('catalog_returns')) AS "catalog_returns",
pg_size_pretty(pg_total_relation_size('catalog_sales')) AS "catalog_sales",
pg_size_pretty(pg_total_relation_size('customer_address')) AS "customer_address",
pg_size_pretty(pg_total_relation_size('customer')) AS "customer",
pg_size_pretty(pg_total_relation_size('customer_demographics')) AS "customer_demographics",
pg_size_pretty(pg_total_relation_size('date_dim')) AS "date_dim",
pg_size_pretty(pg_total_relation_size('dbgen_version')) AS "dbgen_version",
pg_size_pretty(pg_total_relation_size('household_demographics')) AS "household_demographics",
pg_size_pretty(pg_total_relation_size('income_band')) AS "income_band",
pg_size_pretty(pg_total_relation_size('inventory')) AS "inventory",
pg_size_pretty(pg_total_relation_size('item')) AS "item",
pg_size_pretty(pg_total_relation_size('promotion')) AS "promotion",
pg_size_pretty(pg_total_relation_size('reason')) AS "reason",
pg_size_pretty(pg_total_relation_size('ship_mode')) AS "ship_mode",
pg_size_pretty(pg_total_relation_size('store')) AS "store",
pg_size_pretty(pg_total_relation_size('store_returns')) AS "store_returns",
pg_size_pretty(pg_total_relation_size('store_sales')) AS "store_sales",
pg_size_pretty(pg_total_relation_size('time_dim')) AS "time_dim",
pg_size_pretty(pg_total_relation_size('warehouse')) AS "warehouse",
pg_size_pretty(pg_total_relation_size('web_page')) AS "web_page",
pg_size_pretty(pg_total_relation_size('web_returns')) AS "web_returns",
pg_size_pretty(pg_total_relation_size('web_sales')) AS "web_sales",
pg_size_pretty(pg_total_relation_size('web_site')) AS "web_site";
'''