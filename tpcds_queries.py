import psycopg2
import os
import time
from datetime import datetime

host = "172.23.52.199"
port = 20004
database = "tpcds"
user = "postgres"
password = "postgres"

def run_query():
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    root_path = '/home/postgres/tpc/tpcds/queries'
    res_path ='/home/postgres/tpc/tpcds/res/tpcds_runtime'
    total_time = 0
    with open(res_path,'a') as file:
        for filename in os.listdir(root_path):
            #列表中的sql语句无法在数据库中运行
            if filename in ['query2.sql', 'query5.sql', 'query6.sql', 'query12.sql', 'query14.sql', 'query16.sql',
                            'query20.sql', 'query21.sql', 'query23.sql', 'query32.sql', 'query36.sql', 'query37.sql',
                            'query40.sql', 'query49.sql', 'query70.sql', 'query77.sql', 'query80.sql', 'query82.sql',
                            'query86.sql', 'query92.sql', 'query94.sql', 'query95.sql', 'query98.sql', 'query1.sql',
                            'query10.sql', 'query11.sql', 'query29.sql', 'query35.sql', 'query47.sql', 'query57.sql',
                            'query58.sql', 'query59.sql', 'query64.sql', 'query74.sql', 'query78.sql', 'query4.sql']:
                continue

            if filename in ['query18.sql', 'query28.sql', 'query91.sql', 'query17.sql', 'query81.sql', 'query38.sql',
                            'query60.sql', 'query89.sql', 'query8.sql', 'query3.sql', 'query84.sql', 'query26.sql',
                            'query33.sql', 'query96.sql', 'query93.sql', 'query15.sql', 'query13.sql', 'query66.sql',
                            'query65.sql', 'query85.sql', 'query72.sql', 'query73.sql', 'query48.sql','query52.sql',
                            'query55.sql', 'query83.sql', 'query71.sql', 'query31.sql', 'query39.sql', 'query42.sql',
                            'query91.sql', 'query25.sql', 'query61.sql', 'query90.sql', 'query43.sql', 'query79.sql',
                            'query30.sql', 'query99.sql', 'query67.sql', 'query56.sql', 'query97.sql', 'query46.sql',
                            'query27.sql', 'query44.sql', 'query9.sql', 'query62.sql']:
                continue
            # if filename not in ['db1.sql', 'db2.sql', 'db3.sql', 'db4.sql', 'db5.sql']:
            #     continue
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
            elapsed_time = (end_time - start_time) * 1000 #SQL语句执行时间 毫秒
            total_time += elapsed_time
            output = str(datetime.now()) + " " + filename + " " + str(elapsed_time) + "\n"
            print(output)
            file.write(str(output))
        file.write("total_time " + str(total_time) + '\n')
        file.close()
        print("total_time " + str(total_time))
    connection.close()
    return total_time


if __name__ == '__main__':
    run_query()
    # run_time = []
    # number = 5;
    # for i in range(number):
    #     run_time.append(run_query())
    # sum = 0
    # for i in run_time:
    #     print(i)
    #     sum += i
    # average = sum / number
    # print("average time" + str(average))

