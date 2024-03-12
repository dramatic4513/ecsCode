'''
表，属性的编码和解码器
'''
import re
import Graph.parseSQL
import psycopg2
import time
import subprocess
import random

host = "172.23.52.199"
port = 20004
database = "tpch100m"
user = "postgres"
password = "postgres"

'''
简单随机抽样
'''
def random_sample():
    data = [7, 3, 2, 8, 6, 4, 7, 8, 15]
    samples = []
    for k in range(1000):
        sample = []
        for i, limit in enumerate(data):
            sample.append(random.randint(0, limit))
        samples.append(sample)
    for sample in samples[:10]:
        print(sample)

#data = [0, 1, 0, 0, 4, 4, 0, 1, 10]
'''
解析采样得到的列表，生成alter语句
'''
def decode():
    tables = ['NATION', 'REGION', 'PART', 'SUPPLIER', 'PARTSUPP', 'CUSTOMER', 'ORDERS', 'LINEITEM']
    columns = [ ['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'],
        ['R_REGIONKEY', 'R_NAME', 'R_COMMENT'],
     ['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE',
                 'P_COMMENT'],
    ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'],
    ['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'],
    ['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT',
                     'C_COMMENT'],
    ['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY',
                  'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'],
    ['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE',
                     'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE',
                     'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT']
    ]
    data = [0, 1, 0, 0, 4, 4, 0, 1, 10]
    if len(data) != 9:
        print("在decode()函数中，传入的列表长度不符合")
        return False
    for i in range(len(data) - 1):
        table = tables[i]
        partition_key = columns[i][data[i]]
        alter_sql = 'ALTER TABLE ' + table + ' DISTRIBUTE BY HASH(' + partition_key + ');'
        print(alter_sql)



# ['NATION', 'REGION', 'PART', 'SUPPLIER', 'PARTSUPP', 'CUSTOMER', 'ORDERS', 'LINEITEM']
def find_table_names():  # 得到的列名列表需要再处理一下
    file_path = '/home/postgres/tpc/tpch/SQL/createTable.txt'
    str_sql = ""
    with open(file_path, 'r') as f:
        lines = f.readlines()
        for line in lines:
            str_sql = str_sql + line
    sqls = str_sql.split(';')
    table_names = []
    for sql in sqls:
        sql = sql.strip()
        words = sql.split(" ")
        if len(words) > 3:
            table_name = words[2]
            table_names.append(table_name)
            column_list = []
            strs = sql.split("distribute")[0].strip().split(',')
            strs[0] = strs[0].strip().split('(')[1]
            for s in strs:
                column = s.strip().split(" ")[0]
                column_list.append(column)
            print(table_name, column_list)
    print(table_names)


def find_table_column_dist():
    tables = ['NATION', 'REGION', 'PART', 'SUPPLIER', 'PARTSUPP', 'CUSTOMER', 'ORDERS', 'LINEITEM']
    nation_list = ['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT']
    region_list = ['R_REGIONKEY', 'R_NAME', 'R_COMMENT']
    part_list = ['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE',
                 'P_COMMENT']
    supplier_list = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']
    partsupp_list = ['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT']
    customer_list = ['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT',
                     'C_COMMENT']
    order_list = ['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY',
                  'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT']
    lineitem_list = ['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE',
                     'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE',
                     'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT']


def join_sqls():
    file_path = "/home/postgres/code/data/tpch_join_sql"
    # 根据工作负载生成连接语句,并覆盖写，写到文件里
    columns = Graph.parseSQL.folder_sql_to_columns() #解析工作负载，找到其中的连接
    table_columns_dict = {'n_': 'NATION', 'r_': 'REGION', 'p_': 'PART', 's_': 'SUPPLIER', 'ps_': 'PARTSUPP',
                          'c_': 'CUSTOMER', 'o_': 'ORDERS', 'l_': 'LINEITEM'}
    with open(file_path, 'w') as f:
        for column in columns:
            if column[0].split('_')[0] + "_" in table_columns_dict and column[1].split('_')[0] + "_" in table_columns_dict:
                table1 = table_columns_dict[column[0].split('_')[0] + "_"]
                table2 = table_columns_dict[column[1].split("_")[0] + "_"]
                if table1 in ['NATION', 'REGION'] or table2 in ['NATION', 'REGION']:
                    continue
                join_sql = "select * from " + table1+", " + table2 + " where " + column[0] + " = " + column[1] + ";\n"
                print(join_sql)
                f.write(join_sql)
        f.close()

def execute_sql_for_network():
    sql_file = "/home/postgres/code/data/tpch_join_sql"
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    print("Connection established")
    total_time = 0
    process = subprocess.Popen(['dstat', '-nt', '--output', '/home/postgres/tpc/tpch/res/network_for_ga.txt'])
    i = 0
    with open(sql_file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            cursor = connection.cursor()
            start_time = time.time()
            cursor.execute(line)
            i = i + 1
            end_time = time.time()
            total_time += (end_time - start_time) * 1000
            print(str(i) + " " + str(end_time - start_time))
            time.sleep(10)
    print(total_time)
    process.terminate()
    connection.close()

'''
分开计算文件内各个sql的网络字节数，只能计算主节点了
计算总字节数
'''
def sum_network(path):
    line_num = 1
    total_bit = 0
    pre = False
    join_num = 1
    with open(path, 'r') as f:
        for line in f:
            if line_num > 8:
                words = line.split(',')
                recv = float(words[0])
                send = float(words[1])
                if recv > 10000 and send > 10000:
                    total_bit += recv
                    total_bit += send
                    pre = True
                else:
                    if pre:
                        pre = False
                        print(join_num, total_bit)
                        join_num += 1
                        total_bit = 0
            line_num = line_num + 1
        f.close()
    # print("total_bit  ", total_bit)

'''
将tpch100m数据库按照主键作为分区键的方式，进行重分区
'''
def alter_table_primary():
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    sql_path = '/home/postgres/code/data/alter_by_primary_tpch'
    with open(sql_path, 'r') as f:
        lines = f.readlines()
        for line in lines:
            cursor = connection.cursor()
            print(line.strip())
            cursor.execute(line.strip())
            connection.commit()
    connection.close()

'''
获取总的运行时间和工作负载
'''


if __name__ == '__main__':
    join_sqls()
    # execute_sql_for_network()
    # sum_network('/home/postgres/tpc/tpch/res/network_for_ga.txt')
    # alter_table_primary()
    # decode()

