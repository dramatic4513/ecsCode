import psycopg2
import os
import time

host = "172.23.52.199"
port = 20004
database = "tpch"
user = "postgres"
password = "postgres"

def run_query():
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    cursor = connection.cursor()
    root_path = '/home/postgres/tpc/tpch/SQL/queries'
    for filename in os.listdir(root_path):
        if filename not in ['db20.sql']:
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
        # rows = cursor.fetchall()
        # for row in rows:
        #     print(row)
        # connection.close()
        print(filename + " " + str(end_time - start_time) + "")


if __name__ == '__main__':
    run_query()
