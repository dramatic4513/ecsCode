'''
首先在 parseSQL中解析sql语句获取其中的连接对
然后在 column_weight中根据属性权重获取推荐的数据分区语句
最后在本文件中执行更改分区的语句获取对应的运行时间和网络代价
'''

import psycopg2
import tpch_queries
import column_weight

host = "172.23.52.199"
port = 20004
database = "tpch100m"
user = "postgres"
password = "postgres"

def primary_key_partition(file_name_list):
    table_key = {'NATION': 'N_NATIONKEY', 'REGION': 'R_REGIONKEY', 'PART': 'P_PARTKEY', 'SUPPLIER': 'S_SUPPKEY',
                 'PARTSUPP': "PS_PARTKEY", 'CUSTOMER': 'C_CUSTKEY', 'ORDERS': 'O_ORDERKEY', 'LINEITEM': 'L_ORDERKEY'}
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    cursor = connection.cursor()
    try:
        # 按照主键进行分区
        for key, value in table_key.items():
            sql_str = "ALTER TABLE " + key + " DISTRIBUTE BY HASH(" + value + ");"
            cursor.execute(sql_str)
            connection.commit()
            # cursor.close()
        #执行sql语句，获取对应的运行时间和网络通信数量
        tpch_queries.run_query(file_name_list)
    finally:
        cursor.close()
        connection.close()

def greedy_partition(file_name_list):
    alter_table_list = column_weight.tpch_greedy_column_alter_table_sql()
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    cursor = connection.cursor()
    try:
        # 按照主键进行分区
        for alter_table_sql in alter_table_list:
            cursor.execute(alter_table_sql)
            connection.commit()
            print(alter_table_sql)
            # print(cursor.fetchall())
        # 执行sql语句，获取对应的运行时间和网络通信数量
        tpch_queries.run_query(file_name_list)
    finally:
        cursor.close()
        connection.close()




if __name__ == '__main__':
    file_name_list = ['db1.sql', 'db2.sql', 'db3.sql', 'db4.sql', 'db5.sql']  # 要执行的sql
    primary_key_partition(file_name_list)
    # greedy_partition(file_name_list)