import psycopg2
import time

host = "172.23.52.199"
port = 20004
database = "tpch"
user = "postgres"
password = "postgres"


'''
ALTER TABLE ORDERS DISTRIBUTE BY HASH(o_orderkey);
ALTER TABLE LINEITEM DISTRIBUTE BY HASH(l_orderkey);
ALTER TABLE ORDERS DISTRIBUTE BY HASH(o_custkey);
ALTER TABLE LINEITEM DISTRIBUTE BY HASH(l_suppkey);
ALTER TABLE ORDERS DISTRIBUTE BY HASH(O_SHIPPRIORITY);
ALTER TABLE LINEITEM DISTRIBUTE BY HASH(L_PARTKEY);
'''

if __name__ == "__main__":
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)

    cursor = connection.cursor()
    Q = "ALTER TABLE NATION DISTRIBUTE BY HASH(N_NATIONKEY);"
    start_time = time.time()
    cursor.execute(Q)
    end_time = time.time()
    print(end_time - start_time)

    cursor = connection.cursor()
    Q = "ALTER TABLE REGION DISTRIBUTE BY HASH(R_REGIONKEY);"
    start_time = time.time()
    cursor.execute(Q)
    end_time = time.time()
    print(end_time - start_time)

    cursor = connection.cursor()
    Q = "ALTER TABLE PART DISTRIBUTE BY HASH(P_PARTKEY);"
    start_time = time.time()
    cursor.execute(Q)
    end_time = time.time()
    print(end_time - start_time)

    cursor = connection.cursor()
    Q = "ALTER TABLE SUPPLIER DISTRIBUTE BY HASH(S_SUPPKEY);"
    start_time = time.time()
    cursor.execute(Q)
    end_time = time.time()
    print(end_time - start_time)

    cursor = connection.cursor()
    Q = "ALTER TABLE PARTSUPP DISTRIBUTE BY HASH(PS_PARTKEY);"
    start_time = time.time()
    cursor.execute(Q)
    end_time = time.time()
    print(end_time - start_time)

    cursor = connection.cursor()
    Q = "ALTER TABLE CUSTOMER DISTRIBUTE BY HASH(C_CUSTKEY);"
    start_time = time.time()
    cursor.execute(Q)
    end_time = time.time()
    print(end_time - start_time)

    cursor = connection.cursor()
    Q = "ALTER TABLE ORDERS DISTRIBUTE BY HASH(O_ORDERKEY);"
    start_time = time.time()
    cursor.execute(Q)
    end_time = time.time()
    print(end_time - start_time)

    cursor = connection.cursor()
    Q = "ALTER TABLE LINEITEM DISTRIBUTE BY HASH(L_ORDERKEY);"
    start_time = time.time()
    cursor.execute(Q)
    end_time = time.time()
    print(end_time - start_time)

    #
    #
    #
    #
    # cursor = connection.cursor()
    # Q = "ALTER TABLE ORDERS DISTRIBUTE BY HASH(o_custkey);"
    # # line = "select count(*) from ORDERS, LINEITEM where o_custkey = l_suppkey;"
    # start_time = time.time()
    # cursor.execute(Q)
    # end_time = time.time()
    # print(end_time - start_time)
    # Q2 = "ALTER TABLE LINEITEM DISTRIBUTE BY HASH(l_suppkey);"

