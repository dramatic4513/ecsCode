import time

import psycopg2
import os
import re
from collections import Counter
import random
import numpy as np

'''
1. 从文件夹中解析SQL，得到连接对； 解析工作负载得到连接对
2. 合并连接对，对应频率相加
3. 建立数据库表名 及 表大小的对应关系
4. 对应连接的权重
5. 
'''

host = "172.23.52.199"
port = 20004
database = "tpcdsppp"
user = "postgres"
password = "postgres"

'''
输入：属性名
输出：表名
'''

table_primary_list = [['dbgen_version', 'dv_version'], ['customer_address', 'ca_address_sk'],
                      ['customer_demographics', 'cd_demo_sk'],
                      ['date_dim', 'd_date_sk'], ['warehouse', 'w_warehouse_sk'], ['ship_mode', 'sm_ship_mode_sk'],
                      ['time_dim', 't_time_sk'], ['reason', 'r_reason_sk'], ['income_band', 'ib_income_band_sk'],
                      ['item', 'i_item_sk'], ['store', 's_store_sk'], ['call_center', 'cc_call_center_sk'],
                      ['customer', 'c_customer_sk'], ['web_site', 'web_site_sk'], ['store_returns', 'sr_item_sk'],
                      ['household_demographics', 'hd_demo_sk'], ['web_page', 'wp_web_page_sk'],
                      ['promotion', 'p_promo_sk'], ['catalog_page', 'cp_catalog_page_sk'], ['inventory', 'inv_date_sk'],
                      ['catalog_returns', 'cr_item_sk'], ['web_returns', 'wr_item_sk'], ['web_sales', 'ws_item_sk'],
                      ['catalog_sales', 'cs_item_sk'], ['store_sales', 'ss_item_sk']]

table_primary_dict = {'dbgen_version': 'dv_version', 'customer_address': 'ca_address_sk',
                      'customer_demographics': 'cd_demo_sk', 'date_dim': 'd_date_sk', 'warehouse': 'w_warehouse_sk',
                      'ship_mode': 'sm_ship_mode_sk', 'time_dim': 't_time_sk', 'reason': 'r_reason_sk',
                      'income_band': 'ib_income_band_sk', 'item': 'i_item_sk', 'store': 's_store_sk',
                      'call_center': 'cc_call_center_sk', 'customer': 'c_customer_sk', 'web_site': 'web_site_sk',
                      'store_returns': 'sr_item_sk', 'household_demographics': 'hd_demo_sk',
                      'web_page': 'wp_web_page_sk', 'promotion': 'p_promo_sk', 'catalog_page': 'cp_catalog_page_sk',
                      'inventory': 'inv_date_sk', 'catalog_returns': 'cr_item_sk', 'web_returns': 'wr_item_sk',
                      'web_sales': 'ws_item_sk', 'catalog_sales': 'cs_item_sk', 'store_sales': 'ss_item_sk'}


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


'''
输入为 ['sr_ticket_number', 'ss_ticket_number', 1388249088]
按照属性权重的贪婪算法
'''


def greedy_column_partition(edge_weight_list):
    ans_dict = {}  # 表名：属性名
    table_set = set()
    while len(edge_weight_list) > 0:
        # 根据边权重列表对属性值字典进行排序
        column_weight_dict = {}
        # 将边权重转化为属性权重
        for edge_weight in edge_weight_list:
            c = edge_weight[0]
            w = edge_weight[2]
            if c not in column_weight_dict:
                column_weight_dict[c] = w
            else:
                column_weight_dict[c] += w
        for edge_weight in edge_weight_list:
            c = edge_weight[1]
            w = edge_weight[2]
            if c not in column_weight_dict:
                column_weight_dict[c] = w
            else:
                column_weight_dict[c] += w
        sorted_dict_desc = dict(sorted(column_weight_dict.items(), key=lambda item: item[1], reverse=True))
        # for key, value in sorted_dict_desc.items():
        #     print(key, value)
        # 选择权重最大的属性，并且为所有有该属性参与连接的表分配分区键
        greedy_key = next(iter(sorted_dict_desc))
        for edge_weight in edge_weight_list:
            c1 = edge_weight[0]
            c2 = edge_weight[1]
            if c1 == greedy_key or c2 == greedy_key:
                try:
                    t1 = attribute_to_table(c1)
                    t2 = attribute_to_table(c2)
                    ans_dict[t1] = c1
                    ans_dict[t2] = c2
                    table_set.add(t1)
                    table_set.add(t2)
                except TypeError as e:
                    print(e)

        # 去除所有与边相连的表
        new_edge_wight_list = []
        for edge_weight in edge_weight_list:
            c1 = edge_weight[0]
            c2 = edge_weight[1]
            if attribute_to_table(c1) in table_set or attribute_to_table(c2) in table_set:
                continue
            else:
                new_edge_wight_list.append(edge_weight)

        edge_weight_list = new_edge_wight_list

    for key, value in ans_dict.items():
        query_template = "ALTER TABLE {table} DISTRIBUTE BY HASH({column});"
        query = query_template.format(table=key, column=value)
        print(query)
    for key, value in ans_dict.items():
        if table_primary_dict[key] != value:
            print(key + " " + table_primary_dict[key] + " -> " + value)


'''
输入为 ['sr_ticket_number', 'ss_ticket_number', 1388249088]
按照边权重的贪婪算法
'''


def gredy_edge_partition(edge_weight_list):
    partitions = []
    # k = 1
    while edge_weight_list is not None and len(edge_weight_list) > 0:
        # if k == 4:
        #     break
        # k = k + 1
        # 按照权重对边进行排序
        sorted_edge_weight = sorted(edge_weight_list, key=lambda x: x[-1], reverse=True)
        # 选出权重最大的边，分区
        cur_edge = sorted_edge_weight[0]
        t1 = attribute_to_table(cur_edge[0])
        t2 = attribute_to_table(cur_edge[1])
        partition = [t1, cur_edge[0]]
        partitions.append(partition)
        partition = [t2, cur_edge[1]]
        partitions.append(partition)
        # 去掉与选出来的表相连的所有边
        new_edge_weight = []
        for edge in sorted_edge_weight:
            if (t1 == attribute_to_table(edge[0]) or t2 == attribute_to_table(edge[0]) or t1 == attribute_to_table(
                    edge[1])
                    or t2 == attribute_to_table(edge[1])):
                continue
            else:
                new_edge_weight.append(edge)
        edge_weight_list = new_edge_weight
    for i in partitions:
        # print(i)
        query_template = "ALTER TABLE {table} DISTRIBUTE BY HASH({column});"
        table_name = i[0]
        column_name = i[1]
        query = query_template.format(table=table_name, column=column_name)
        print(query)
    for i in partitions:
        table_name = i[0]
        column_name = i[1]
        if table_primary_dict[table_name] != column_name:
            print(table_name + " " + table_primary_dict[table_name] + " -> " + column_name)



def primary_partiton():
    with open('/home/postgres/code/data/tpcds.sql', 'r') as file:
        sql_content = file.read()

    # 使用正则表达式提取每个表的定义，包括表名和分区键
    pattern = r'create table (\w+)\s*\(([\s\S]*?)\)distribute by hash\((\w+)\);'
    matches = re.findall(pattern, sql_content, re.IGNORECASE)

    table_list = []
    table_primary_dict = {}
    # 打印提取结果
    for match in matches:
        table_name, _, partition_key = match
        # print(f"表名: {table_name.strip()}")
        # print(f"分区键: {partition_key.strip()}")
        query_template = "ALTER TABLE {table} DISTRIBUTE BY HASH({column});"
        table = table_name.strip()
        table_list.append(table)
        column = partition_key.strip()
        query = query_template.format(table=table, column=column)
        print(query)
        table_primary_dict[table] = column

    table_list.sort()
    print(table_list)
    print(table_primary_dict)


'''
输入为 ['sr_ticket_number', 'ss_ticket_number', 1388249088]
按照边权重的概率算法
'''


def probability_edge_partition(edge_weight_list):
    ans_edge_list = []
    while len(edge_weight_list) > 0:
        weights_list = [item[2] for item in edge_weight_list]
        total_weight = sum(weights_list)
        normalized_weights = [weight / total_weight for weight in weights_list]
        cur_edge_index = np.random.choice(len(edge_weight_list), p=normalized_weights)
        # cur_edge = random.choice(edge_weight_list, weights=weights_list, k=1)
        # cur_edge_index = np.random.choice(len(edge_weight_list), p=weights_list)
        cur_edge = edge_weight_list[cur_edge_index]
        ans_edge_list.append(cur_edge)
        t1 = attribute_to_table(cur_edge[0])
        t2 = attribute_to_table(cur_edge[1])
        new_edge_weight_list = list()
        for edge in edge_weight_list:
            if t1 == attribute_to_table(edge[0]) or t1 == attribute_to_table(edge[1]) or t2 == attribute_to_table(
                    edge[0]) or t2 == attribute_to_table(edge[1]):
                continue
            else:
                new_edge_weight_list.append(edge)
        edge_weight_list = new_edge_weight_list
    # print("-------------")
    for edge in ans_edge_list:
        query_template = "ALTER TABLE {table} DISTRIBUTE BY HASH({column});"
        table_name = attribute_to_table(edge[0])
        column_name = edge[0]
        query = query_template.format(table=table_name, column=column_name)
        print(query)
        table_name = attribute_to_table(edge[1])
        column_name = edge[1]
        query = query_template.format(table=table_name, column=column_name)
        print(query)
    for edge in ans_edge_list:
        table_name = attribute_to_table(edge[0])
        column_name = edge[0]
        if table_primary_dict[table_name] != column_name:
            print(table_name + " " + table_primary_dict[table_name] + " -> " + column_name)
        table_name = attribute_to_table(edge[1])
        column_name = edge[1]
        if table_primary_dict[table_name] != column_name:
            print(table_name + " " + table_primary_dict[table_name] + " -> " + column_name)

def probability_attribute_partition(edge_weight_list):
    ans_dict = dict() #表：属性
    table_set = set() #已分配属性的表

    while len(edge_weight_list) > 0:
        #将边权重转化为属性权重
        attribute_weight_dict = dict()
        for edge in edge_weight_list:
            a1 = edge[0]
            a2 = edge[1]
            w = edge[2]
            if a1 in attribute_weight_dict:
                attribute_weight_dict[a1] += w
            else:
                attribute_weight_dict[a1] = w
            if a2 in attribute_weight_dict:
                attribute_weight_dict[a2] += w
            else:
                attribute_weight_dict[a2] = w
        attribute_weight_list = list()
        for key, value in attribute_weight_dict.items():
            cur_list = [key, value]
            attribute_weight_list.append(cur_list)

        #概率归一化，选择属性
        weights_list = [item[1] for item in attribute_weight_list]
        total_weight = sum(weights_list)
        normalized_weights = [weight / total_weight for weight in weights_list]
        cur_attribute_weight_index = np.random.choice(len(attribute_weight_list), p=normalized_weights)
        cur_attribute = attribute_weight_list[cur_attribute_weight_index]
        print(cur_attribute)

        #为在这个属性上连接的表分配属性
        for edge_weight in edge_weight_list:
            c1 = edge_weight[0]
            c2 = edge_weight[1]
            if c1 == cur_attribute[0] or c2 == cur_attribute[0]:
                t1 = attribute_to_table(c1)
                t2 = attribute_to_table(c2)
                ans_dict[t1] = c1
                ans_dict[t2] = c2
                table_set.add(t1)
                table_set.add(t2)

        #去除所有与边相连的表
        new_edge_wight_list = []
        for edge_weight in edge_weight_list:
            c1 = edge_weight[0]
            c2 = edge_weight[1]
            if attribute_to_table(c1) in table_set or attribute_to_table(c2) in table_set:
                continue
            else:
                new_edge_wight_list.append(edge_weight)

        edge_weight_list = new_edge_wight_list

    for key, value in ans_dict.items():
        query_template = "ALTER TABLE {table} DISTRIBUTE BY HASH({column});"
        query = query_template.format(table=key, column=value)
        print(query)
    for key, value in ans_dict.items():
        if table_primary_dict[key] != value:
            print(key + " " + table_primary_dict[key] + " -> " + value)


def replication_for_PAA(table, edge_weight_list):
    new_list = list()
    for e in edge_weight_list:
        a1 = e[0]
        a2 = e[1]
        t1 = attribute_to_table(a1)
        t2 = attribute_to_table(a2)
        if t1 == table or t2 == table:
            continue
        else:
            new_list.append(e)
    return new_list

if __name__ == '__main__':
    # 根据列找表名 和 根据表名查找表大小
    # table_name = attribute_to_table("ws_net_paid_inc_ship_tax")
    # table_size_bytes = table_size(table_name)

    # # 边贪心算法生成分区语句
    file_name_list = ['query60.sql', 'query61.sql', 'query62.sql', 'query63.sql', 'query65.sql', 'query66.sql', 'query67.sql', 'query68.sql', 'query69.sql']
    join_pair_freduency = find_join_pair(file_name_list)
    edge_weight_list = edge_weight(join_pair_freduency)

    start_time = time.time()
    gredy_edge_partition(edge_weight_list)
    end_time = time.time()
    esp = (end_time - start_time) * 1000
    print(esp)

    # 属性贪心算法生成分区语句
    # file_name_list = ['query41.sql', 'query42.sql', 'query43.sql', 'query44.sql', 'query45.sql', 'query46.sql', 'query48.sql']
    # join_pair_freduency = find_join_pair(file_name_list)
    # edge_weight_list = edge_weight(join_pair_freduency)
    # greedy_column_partition(edge_weight_list)

    # # 边概率算法生成分区语句
    # file_name_list = ['query41.sql', 'query42.sql', 'query43.sql', 'query44.sql', 'query45.sql', 'query46.sql', 'query48.sql']
    # join_pair_freduency = find_join_pair(file_name_list)
    # edge_weight_list = edge_weight(join_pair_freduency)
    # probability_edge_partition(edge_weight_list)

    # # 属性概率算法生成分区语句
    # file_name_list = ['query41.sql', 'query42.sql', 'query43.sql', 'query44.sql', 'query45.sql', 'query46.sql', 'query48.sql']
    # join_pair_freduency = find_join_pair(file_name_list)
    # edge_weight_list = edge_weight(join_pair_freduency)
    # probability_attribute_partition(edge_weight_list)

    # # 去掉全表复制表的属性概率算法生成分区语句
    # file_name_list = ['query41.sql', 'query42.sql', 'query43.sql', 'query44.sql', 'query45.sql', 'query46.sql', 'query48.sql']
    # join_pair_freduency = find_join_pair(file_name_list)
    # t = "store"
    # join_pair_weight = edge_weight(join_pair_freduency)
    # edge_weight_list = replication_for_PAA(t, join_pair_weight)
    # probability_attribute_partition(edge_weight_list)

    # 处理主键分区
    # primary_partiton()
