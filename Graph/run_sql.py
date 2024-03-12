import psycopg2
import time

host = "172.23.52.199"
port = 20004
database = "tpch100m"
user = "postgres"
password = "postgres"


'''
ALTER TABLE ORDERS DISTRIBUTE BY HASH(o_orderkey);
ALTER TABLE LINEITEM DISTRIBUTE BY HASH(l_orderkey);
ALTER TABLE ORDERS DISTRIBUTE BY HASH(o_custkey);
ALTER TABLE LINEITEM DISTRIBUTE BY HASH(l_suppkey);
'''

if __name__ == "__main__":
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    cursor = connection.cursor()
    start_time = time.time()
    line = "select * from ORDERS, LINEITEM where o_orderkey = l_orderkey;"
    cursor.execute(line)
    end_time = time.time()
    time = (end_time - start_time) * 1000
    print(time)
