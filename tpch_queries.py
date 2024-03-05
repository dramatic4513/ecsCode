import psycopg2
import os
import time
from datetime import datetime

host = "172.23.52.199"
port = 20004
database = "tpch"
user = "postgres"
password = "postgres"

def run_query():
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    root_path = '/home/postgres/tpc/tpch/SQL/queries'
    res_path ='/home/postgres/tpc/res/tpch_runtime'
    total_time = 0
    with open(res_path,'a') as file:
        for filename in os.listdir(root_path):
            if filename in ['db20.sql']:
                continue
            query_path = os.path.join(root_path, filename)
            sql = ""
            with open(query_path) as f:
                for line in f:
                    sql += line
            cursor = connection.cursor()
            start_time = time.time()
            cursor.execute(sql)
            end_time = time.time()
            elapsed_time = (end_time - start_time) * 1000 #SQL语句执行时间 毫秒
            total_time += elapsed_time
            output = str(datetime.now()) + " " + filename + " " + str(elapsed_time) + "\n"
            print(output)
            file.write(str(output))
        file.write("total_time " + str(total_time) + '\n')
        file.close()
    connection.close()


if __name__ == '__main__':
    run_query()
