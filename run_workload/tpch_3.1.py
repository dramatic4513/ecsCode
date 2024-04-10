import psycopg2
import time
import os

'''
alter table supplier distribute by hash(s_nationkey);
alter table customer distribute by hash(c_nationkey);
alter table orders distribute by hash(o_custkey);
'''

host = "172.23.52.199"
port = 20004
database = "tpch100m"
user = "postgres"
password = "postgres"

if __name__ == '__main__':
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    total_time = 0
    root_path = '/home/postgres/tpc/tpch/SQL/queries'
    for filename in os.listdir(root_path):
        if filename not in ['db17.sql', 'db18.sql', 'db19.sql', 'db21.sql', 'db22.sql']:
        # if filename not in ['db6.sql', 'db7.sql', 'db8.sql', 'db9.sql', 'db10.sql']:
            continue
        print(filename)
        query_path = os.path.join(root_path, filename)
        sql = ""
        with open(query_path) as f:
            for line in f:
                sql += line
        cursor = connection.cursor()
        start_time = time.time()
        cursor.execute(sql)
        end_time = time.time()
        elapsed_time = (end_time - start_time) * 1000  # SQL语句执行时间 毫秒
        print(elapsed_time)
        total_time += elapsed_time
        cursor.close()
    connection.close()
    print(total_time)
# print("Q1")
# connection = psycopg2.connect(host=host, port=port, database=database, user=user)
# cursor = connection.cursor()
# start_time = time.time()
# cursor.execute("select count(*) from orders,lineitem where o_orderkey = l_orderkey")
# end_time = time.time()
# elapsed_time = (end_time - start_time) * 1000  # SQL语句执行时间 毫秒
# print(elapsed_time)
#
# print("Q2")
# connection = psycopg2.connect(host=host, port=port, database=database, user=user)
# cursor = connection.cursor()
# start_time = time.time()
# cursor.execute("select count(*) from orders,lineitem where o_custkey = l_suppkey")
# end_time = time.time()
# elapsed_time = (end_time - start_time) * 1000  # SQL语句执行时间 毫秒
# print(elapsed_time)

# print("Q1 *")
# cursor = connection.cursor()
# start_time = time.time()
# cursor.execute("select * from orders,lineitem where o_orderkey = l_orderkey")
# end_time = time.time()
# elapsed_time = (end_time - start_time) * 1000  # SQL语句执行时间 毫秒
# print(elapsed_time)
#
#
# print("Q2 *")
# cursor = connection.cursor()
# start_time = time.time()
# cursor.execute("select * from orders,lineitem where o_custkey = l_suppkey")
# end_time = time.time()
# elapsed_time = (end_time - start_time) * 1000  # SQL语句执行时间 毫秒
# print(elapsed_time)