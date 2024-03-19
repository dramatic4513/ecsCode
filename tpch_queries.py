import psycopg2
import os
import time
from datetime import datetime
import subprocess

host = "172.23.52.199"
port = 20004
# database = "tpch100m"
# database = "tpch100los"
user = "postgres"
password = "postgres"


def run_query(database, file_name_list):
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    total_time = 0
    try:
        root_path = '/home/postgres/tpc/tpch/SQL/queries'
        # res_path = '/home/postgres/tpc/res/tpch_runtime'
        res_path = '/home/postgres/tpc/tpch/res/new_tpch_runtime'
        with open(res_path, 'a') as file:
            file.write(str(database) + "\n")
            for filename in os.listdir(root_path):
                # if filename in ['db20.sql']:
                #     continue
                if filename not in file_name_list:
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
                total_time += elapsed_time
                output = str(datetime.now()) + " " + filename + " " + str(elapsed_time) + "\n"
                print(output)
                file.write(str(output))
            file.write("total_time " + str(total_time) + '\n')
            file.close()
            print("total_time " + str(total_time))
    finally:
        connection.close()
    return total_time


if __name__ == '__main__':
    runtime_path = '/home/postgres/tpc/tpch/res/tpch_runtime'
    file_name_list = [['db6.sql', 'db7.sql', 'db8.sql', 'db9.sql', 'db10.sql'],
                      ['db11.sql', 'db12.sql', 'db13.sql', 'db14.sql', 'db15.sql'],
                      ['db16.sql', 'db17.sql', 'db18.sql', 'db19.sql', 'db21.sql', 'db22.sql']]
    database_names = ["tpch100m", "tpch100nationkey", "tpch100suppkey", "tpch100los"]
    for database in database_names:
        command = '/home/postgres/tpc/tpch/res/new_net_'
        command = command + database + "_"
        for file_name in file_name_list:
            command_new = command + file_name[0]
            print(command_new)
            process = subprocess.Popen(['dstat', '-nt', '--output', command_new])
            run_query(database, file_name)
            process.terminate()

    # file_name_list = ['db1.sql', 'db2.sql', 'db3.sql', 'db4.sql', 'db5.sql']
    # file_name_list = ['db6.sql', 'db7.sql', 'db8.sql', 'db9.sql', 'db10.sql']
    # file_name_list = ['db11.sql', 'db12.sql', 'db13.sql', 'db14.sql', 'db15.sql']
    # file_name_list = ['db16.sql', 'db17.sql', 'db18.sql', 'db19.sql', 'db21.sql', 'db22.sql']



    # run_time = []
    # number = 5;
    # for i in range(number):
    #     run_time.append(run_query(file_name_list))
    # sum = 0
    # for i in run_time:
    #     print(i)
    #     sum += i
    # average = sum / number
    # print("average time" + str(average))
