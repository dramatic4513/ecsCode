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
        if filename not in ['db18.sql', 'db2.sql']:
            continue
        query_path = os.path.join(root_path, filename)
        columns += parse_sql(query_path) #获得当前sql的连接属性
    return columns

def table_columns_dict():
    table_columns = {}


if __name__ == '__main__':
    columns = folder_sql_to_columns()

    # # 使用 sqlparse 解析 SQL 语句
    # parsed = sqlparse.parse(sql)
    #
    # # 遍历解析结果
    # for statement in parsed:
    #     # 查找连接的表和属性
    #     tables = []
    #     for item in statement.tokens:
    #         if isinstance(item, sqlparse.sql.IdentifierList):
    #             for identifier in item.get_identifiers():
    #                 tables.append(identifier.get_real_name())
    #         elif isinstance(item, sqlparse.sql.Identifier):
    #             tables.append(item.get_real_name())
    #
    #     # 输出连接的表和属性
    #     print("连接的表和属性：", tables)
