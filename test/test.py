import psycopg2
import time

host = "172.23.52.199"
port = 20004
database = "tpch100m"
user = "postgres"
password = "postgres"

if __name__ == "__main__":
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    cursor = connection.cursor()
    root_path = '/home/postgres/tpc/tpch/queries/db4.sql'
    sql = ""
    with open(root_path) as f:
        for line in f:
            sql += line
    # line = "select count(*) from nation;"
    print(sql)
    start_time = time.time()
    cursor.execute(sql)
    end_time = time.time()
    print(end_time - start_time)