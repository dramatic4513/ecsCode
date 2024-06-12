import psycopg2
import os
import time
from datetime import datetime

from Graph import tpcds_na_greedy_edges as greedy

host = "172.23.52.199"
port = 20004
# database = "tpcds39"
database = "tpcds"
user = "postgres"
password = "postgres"

def slide_window_workload():
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    root_path = '/home/postgres/tpc/tpcds/queries'
    workload = ['query7.sql', 'query8.sql', 'query27.sql', 'query65.sql']
    left = 0
    right = 0
    # filenames = [workload[left]]
    while(right < len(workload)):
        filenames = []
        i = 0
        #建立窗口
        while ( i < 3 and right < len(workload)) :
            join_pairs = greedy.find_join_pair(filenames)
            filenames.append(workload[right])
            right = right + 1
            i = i + len(join_pairs)
        #为窗口内的SQL生成数据分区方案
        join_pair_freduency = greedy.find_join_pair(filenames)
        edge_weight_list = greedy.edge_weight(join_pair_freduency)
        repartition_income, repartition_cost = greedy.probability_attribute_partition(edge_weight_list)
        #数据重分区时机判定
        print(repartition_income, repartition_cost)
        if repartition_income < repartition_cost:
            print("False ！！！ Do not repartition")
        else :
            print(" repartition ")
        for filename in filenames :
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
            output = str(datetime.now()) + " " + filename + " " + str(elapsed_time) + "\n"
            print(output)
            cursor.close()
    connection.close()

if __name__ == "__main__":
    slide_window_workload()

