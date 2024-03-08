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
        if not key.isdigit() and not value.isdigit():
            columns.append(match)
    return columns


# 根据存储sql语句文件夹的位置，然后决定哪些文件被解析，然后调用解析函数解析sql语句，得到解析后的属性列表
def folder_sql_to_columns():
    root_path = '/home/postgres/tpc/tpch/SQL/queries'
    columns = []
    for filename in os.listdir(root_path):
        if filename not in ['db1.sql', 'db2.sql', 'db3.sql', 'db4.sql', 'db5.sql']:
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


if __name__ == '__main__':
    columns = folder_sql_to_columns()
    for column in columns:
        print(column)
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