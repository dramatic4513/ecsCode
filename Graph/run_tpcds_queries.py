import psycopg2
import os
import time
from datetime import datetime

host = "172.23.52.199"
port = 20004
# database = "tpcds39"
database = "tpcds"
user = "postgres"
password = "postgres"

if __name__ == "__main__":
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    root_path = '/home/postgres/tpc/tpcds/queries'
    total_time = 0
    for filename in os.listdir(root_path):
        if filename not in ['query39.sql']:
            continue
        print(filename)
        query_path = os.path.join(root_path, filename)
        sql = ""
        with open(query_path) as f:
            for line in f:
                sql += line
        cursor = connection.cursor()
        i = 0
        while i < 3:
            i = i + 1
            start_time = time.time()
            cursor.execute(sql)
            end_time = time.time()
            elapsed_time = (end_time - start_time) * 1000  # SQL语句执行时间 毫秒
            total_time += elapsed_time
            output = str(datetime.now()) + " " + filename + " " + str(elapsed_time) + "\n"
            print(output)
        cursor.close()
    connection.close()

