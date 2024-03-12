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
    root_path = '/home/postgres/tpc/tpcds/queries'
    columns = []
    for filename in os.listdir(root_path):
        # if filename not in ['db21.sql', 'db22.sql']:
        #     continue
        if filename in ['query2.sql', 'query5.sql', 'query6.sql', 'query12.sql', 'query14.sql', 'query16.sql',
                            'query20.sql', 'query21.sql', 'query23.sql', 'query32.sql', 'query36.sql', 'query37.sql',
                            'query40.sql', 'query49.sql', 'query70.sql', 'query77.sql', 'query80.sql', 'query82.sql',
                            'query86.sql', 'query92.sql', 'query94.sql', 'query95.sql', 'query98.sql', 'query1.sql',
                            'query10.sql', 'query11.sql', 'query29.sql', 'query35.sql', 'query47.sql', 'query57.sql',
                            'query58.sql', 'query59.sql', 'query64.sql', 'query74.sql', 'query78.sql', 'query4.sql']:
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



if __name__ == '__main__':
    columns = folder_sql_to_columns()
    for column in columns:
        print(column)
    print(len(columns))

