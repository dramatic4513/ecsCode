import psycopg2
import os
import re
from collections import Counter
import random
import numpy as np

'''
1. 将属性名转换为表名的代码
2. 统计所有连接，并合并，得到频率
3. 计算收益
    建立字典 table_profit_dict = {t1:0, t2:0, t3:0, t4:0}
    遍历所有的连接，两端profit分别 += （两端表大小之和 * 频率）
4. 遍历字典，输出 表大小 ，profit， 代价， prfit/代价
'''
import psycopg2
import os
import re
from collections import Counter
import random
import numpy as np

host = "172.23.52.199"
port = 20004
database = "tpcdsppp"
user = "postgres"
password = "postgres"

'''
输入：属性名
输出：表名
'''
def attribute_to_table(attribute_name):
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    cur = connection.cursor()
    cur.execute(
        """ SELECT relname FROM pg_class WHERE oid IN (  SELECT attrelid FROM pg_attribute  WHERE attname = %s );""",
        (attribute_name,))
    table_name = cur.fetchone()[0]
    # print(table_name)
    cur.close()
    connection.close()
    return table_name
'''
输入：表名
输出：表大小
'''
def table_size(table_name):
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    cur = connection.cursor()
    cur.execute(f"SELECT pg_relation_size('{table_name}')")
    table_size_bytes = cur.fetchone()[0]
    # print(table_name + " " + str(table_size_bytes))  # 单位B
    cur.close()
    connection.close()
    return table_size_bytes


'''
统计查询语句中的连接对
以及连接对的频率
('sr_item_sk', 'ss_item_sk', 3)
'''
def find_join_pair(file_name_list):
    folder_path = '/home/postgres/tpc/tpcds/queries'
    join_pairs = []  # 原始连接对列表
    for filename in os.listdir(folder_path):
        if filename not in file_name_list:
            continue
        query_path = os.path.join(folder_path, filename)
        sql = ""
        with open(query_path, "r") as f:
            for line in f:
                sql = sql + line
        matches = re.findall(r'\b(\w+)\s*=\s*(\w+)\b', sql)
        for match in matches:
            key, value = match
            if not key.isdigit() and not value.isdigit():
                join_pairs.append(match)
    counter = Counter([tuple(sorted(pair)) for pair in join_pairs])
    join_pairs_frequency = [(pair[0], pair[1], count) for pair, count in counter.items()]
    for i in join_pairs_frequency:
        print(i)
    return join_pairs_frequency

'''
输入为 ('c_current_addr_sk', 'ca_address_sk', 2) 组成的列表
输出为 ['sr_ticket_number', 'ss_ticket_number', 1388249088] 组成的列表
'''


def edge_weight(join_pairs_frequency):
    join_pairs_weight = []
    for join_pair in join_pairs_frequency:
        print(join_pair)
        try:
            t1 = table_size(attribute_to_table(join_pair[0]))
            t2 = table_size(attribute_to_table(join_pair[1]))
            f = join_pair[2]
            join_weigth = f * (t1 + t2)
            join_pairs_weight.append([join_pair[0], join_pair[1], join_weigth])
        except TypeError as e:
            print(e)
    for join_pair in join_pairs_weight:
        print(join_pair)
    return join_pairs_weight

def replication(join_pairs_weight):
    table_profit_dict = dict()
    for join_pair in join_pairs_weight:
        a1 = join_pair[0]
        a2 = join_pair[1]
        t1 = attribute_to_table(a1)
        t2 = attribute_to_table(a2)
        if t1 not in table_profit_dict:
            table_profit_dict[t1] = join_pair[2]
        else:
            table_profit_dict[t1] = table_profit_dict[t1] + join_pair[2]
        if t2 not in table_profit_dict:
            table_profit_dict[t2] = join_pair[2]
        else:
            table_profit_dict[t2] = table_profit_dict[t2] + join_pair[2]
    for key,value in table_profit_dict.items():
        profit = table_size(key) * 2
        print(key, value, profit, value/profit)

if __name__ == '__main__':
    file_name_list = [ 'query8.sql']
    join_pair_freduency = find_join_pair(file_name_list)
    edge_weight_list = edge_weight(join_pair_freduency)
    replication(edge_weight_list)
