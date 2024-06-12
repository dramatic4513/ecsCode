import time
import matplotlib.pyplot as plt

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

pair_to_id = {('cs_sold_date_sk', 'd_date_sk'): 0, ('cs_item_sk', 'i_item_sk'): 1, ('cd1', 'cs_bill_cdemo_sk'): 2,
              ('c_customer_sk', 'cs_bill_customer_sk'): 3, ('c_current_cdemo_sk', 'cd2'): 4,
              ('c_current_addr_sk', 'ca_address_sk'): 5, ('cc_call_center_sk', 'cr_call_center_sk'): 6,
              ('cr_returned_date_sk', 'd_date_sk'): 7, ('c_customer_sk', 'cr_returning_customer_sk'): 8,
              ('c_current_cdemo_sk', 'cd_demo_sk'): 9, ('c_current_hdemo_sk', 'hd_demo_sk'): 10,
              ('d_date_sk', 'ss_sold_date_sk'): 11, ('i_item_sk', 'ss_item_sk'): 12, ('s_store_sk', 'ss_store_sk'): 13,
              ('sr_customer_sk', 'ss_customer_sk'): 14, ('sr_item_sk', 'ss_item_sk'): 15,
              ('sr_ticket_number', 'ss_ticket_number'): 16, ('d2', 'sr_returned_date_sk'): 17,
              ('cs_bill_customer_sk', 'sr_customer_sk'): 18, ('cs_item_sk', 'sr_item_sk'): 19,
              ('cs_sold_date_sk', 'd3'): 20, ('ca_address_sk', 'cr_returning_addr_sk'): 21, ('ctr2', 'ctr_state'): 22,
              ('c_customer_sk', 'ctr_customer_sk'): 23, ('date_dim', 'ss_sold_date_sk'): 24,
              ('customer', 'ss_customer_sk'): 25, ('cs_sold_date_sk', 'date_dim'): 26,
              ('cs_bill_customer_sk', 'customer'): 27, ('date_dim', 'ws_sold_date_sk'): 28,
              ('customer', 'ws_bill_customer_sk'): 29, ('ca_address_sk', 'ss_addr_sk'): 30,
              ('ca_address_sk', 'cs_bill_addr_sk'): 31, ('i_item_sk', 'ws_item_sk'): 32,
              ('d_date_sk', 'ws_sold_date_sk'): 33, ('ca_address_sk', 'ws_bill_addr_sk'): 34,
              ('d_date_sk', 'store_sales'): 35, ('item', 'ss_item_sk'): 36,
              ('hd_income_band_sk', 'ib_income_band_sk'): 37, ('cd_demo_sk', 'sr_cdemo_sk'): 38,
              ('cd_demo_sk', 'cs_bill_cdemo_sk'): 39, ('cs_promo_sk', 'p_promo_sk'): 40,
              ('ss_sold_time_sk', 'time_dim'): 41, ('household_demographics', 'ss_hdemo_sk'): 42,
              ('r_reason_sk', 'sr_reason_sk'): 43, ('hd_demo_sk', 'ss_hdemo_sk'): 44, ('cd_demo_sk', 'ss_cdemo_sk'): 45,
              ('w_warehouse_sk', 'ws_warehouse_sk'): 46, ('t_time_sk', 'ws_sold_time_sk'): 47,
              ('sm_ship_mode_sk', 'ws_ship_mode_sk'): 48, ('cs_warehouse_sk', 'w_warehouse_sk'): 49,
              ('cs_sold_time_sk', 't_time_sk'): 50, ('cs_ship_mode_sk', 'sm_ship_mode_sk'): 51,
              ('sc', 'ss_store_sk'): 52, ('s_store_sk', 'sc'): 53, ('i_item_sk', 'sc'): 54,
              ('wp_web_page_sk', 'ws_web_page_sk'): 55, ('wr_item_sk', 'ws_item_sk'): 56,
              ('wr_order_number', 'ws_order_number'): 57, ('cd_demo_sk', 'wr_refunded_cdemo_sk'): 58,
              ('cd_demo_sk', 'wr_returning_cdemo_sk'): 59, ('ca_address_sk', 'wr_refunded_addr_sk'): 60,
              ('r_reason_sk', 'wr_reason_sk'): 61, ('cd2', 'cd_marital_status'): 62, ('cd2', 'cd_education_status'): 63,
              ('cs_item_sk', 'inv_item_sk'): 64, ('inv_warehouse_sk', 'w_warehouse_sk'): 65,
              ('cs_bill_hdemo_sk', 'hd_demo_sk'): 66, ('cs_sold_date_sk', 'd1'): 67, ('d2', 'inv_date_sk'): 68,
              ('cs_ship_date_sk', 'd3'): 69, ('cr_item_sk', 'cs_item_sk'): 70,
              ('cr_order_number', 'cs_order_number'): 71, ('d2', 'd_week_seq'): 72,
              ('ss_store_sk', 'store'): 73, ('c_customer_sk', 'ss_customer_sk'): 74, ('i_item_sk', 'sr_item_sk'): 75,
              ('d_date_sk', 'sr_returned_date_sk'): 76, ('cr_item_sk', 'i_item_sk'): 77,
              ('i_item_sk', 'wr_item_sk'): 78,
              ('d_date_sk', 'wr_returned_date_sk'): 79, ('cr_items', 'item_id'): 80, ('item_id', 'wr_items'): 81,
              ('i_item_sk', 'sold_item_sk'): 82, ('t_time_sk', 'time_sk'): 83, ('ca_county', 'ss2'): 84,
              ('ca_county', 'ss3'): 85, ('ca_county', 'ws1'): 86, ('ca_county', 'ws2'): 87, ('ca_county', 'ws3'): 88,
              ('i_item_sk', 'inv_item_sk'): 89, ('d_date_sk', 'inv_date_sk'): 90, ('i_item_sk', 'inv2'): 91,
              ('inv2', 'w_warehouse_sk'): 92, ('p_promo_sk', 'ss_promo_sk'): 93, ('time_dim', 'ws_sold_time_sk'): 94,
              ('household_demographics', 'ws_ship_hdemo_sk'): 95, ('web_page', 'ws_web_page_sk'): 96,
              ('ca_address_sk', 'wr_returning_addr_sk'): 97, ('cs_ship_date_sk', 'd_date_sk'): 98,
              ('cc_call_center_sk', 'cs_call_center_sk'): 99, ('csci', 'customer_sk'): 100, ('csci', 'item_sk'): 101,
              ('customer_address', 'ss_addr_sk'): 102, ('c_current_addr_sk', 'current_addr'): 103,
              ('descending', 'rnk'): 104, ('asceding', 'i_item_sk'): 105, ('descending', 'i_item_sk'): 106,
              ('d_date_sk', 'ws_ship_date_sk'): 107, ('web_site_sk', 'ws_web_site_sk'): 108,
              ('d1', 'ss_sold_date_sk'): 109, ('i1', 'i_manufact'): 110, ('c_current_addr_sk', 'ca'): 111,
              ('c', 'cd_demo_sk'): 112, ('c_customer_sk', 'ws_bill_customer_sk'): 113,
              ('c_customer_sk', 'cs_ship_customer_sk'): 114, ('d_date_sk', 'sold_date_sk'): 115,
              ('i_item_sk', 'item_sk'): 116, ('c_customer_sk', 'cs_or_ws_sales'): 117, ('ca_county', 's_county'): 118,
              ('ca_state', 's_state'): 119, ('item_sk', 'store'): 120, ('d_date', 'store'): 121,
              ('i_brand_id', 'prev_yr'): 122, ('i_class_id', 'prev_yr'): 123, ('i_category_id', 'prev_yr'): 124,
              ('i_manufact_id', 'prev_yr'): 125, ('ca_zip', 's_zip'): 126}


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


def table_size(table_name):
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    cur = connection.cursor()
    cur.execute(f"SELECT pg_relation_size('{table_name}')")
    table_size_bytes = cur.fetchone()[0]
    # print(table_name + " " + str(table_size_bytes))  # 单位B
    cur.close()
    connection.close()
    return table_size_bytes


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
    # for i in join_pairs_frequency:
    #     print(i)
    return join_pairs_frequency


def edge_weight(join_pairs_frequency):
    join_pairs_weight = []
    for join_pair in join_pairs_frequency:
        # print(join_pair)
        try:
            t1 = table_size(attribute_to_table(join_pair[0]))
            t2 = table_size(attribute_to_table(join_pair[1]))
            f = join_pair[2]
            join_weigth = f * (t1 + t2)
            join_pairs_weight.append([join_pair[0], join_pair[1], join_weigth])
        except TypeError as e:
            print(e)
    # for join_pair in join_pairs_weight:
    #     print(join_pair)
    return join_pairs_weight


'''
将连接转换为标号
'''


def join_to_id(word_pairs):
    pair_to_id = {}
    id_counter = 0

    for pair in word_pairs:
        word_pair = (pair[0], pair[1])  # 只考虑元组中的前两个单词
        if word_pair not in pair_to_id:
            pair_to_id[word_pair] = id_counter
            id_counter += 1

    print(pair_to_id)


'''
输入：连接对及频率
输出：标号对
'''


def join_id_and_frequency(word_pairs):
    join_id_frequency_dict = dict()
    for pair in word_pairs:
        word_pair = (pair[0], pair[1])  # 只考虑元组中的前两个单词
        id = pair_to_id[word_pair]
        if id not in join_id_frequency_dict:
            join_id_frequency_dict[id] = pair[2]
        else:
            join_id_frequency_dict[id] = join_id_frequency_dict[id] + pair[2]
    return join_id_frequency_dict
    # print(join_id_frequency_dict)
    # keys = list(join_id_frequency_dict.keys())
    # values = list(join_id_frequency_dict.values())
    #
    # # plt.figure(figsize=(8,6))
    # plt.figure()
    # plt.scatter(keys, values, color='blue')
    #
    # # 添加标题和标签
    # plt.title('7')
    # plt.xlabel('Keys')
    # plt.ylabel('Values')
    #
    # # 显示图形
    # plt.show()


def draw_dot(join_id_frequency_dict, file_name):
    print(file_name, join_id_frequency_dict)
    keys = list(join_id_frequency_dict.keys())
    values = list(join_id_frequency_dict.values())

    # plt.figure(figsize=(8,6))
    plt.figure()
    plt.scatter(keys, values, color='blue')

    # plt.xlim(0, 127)
    # plt.ylim(0, 40)

    # 添加标题和标签
    plt.title(file_name)
    plt.xlabel('Keys')
    plt.ylabel('Values')

    # 显示图形
    plt.show()


if __name__ == '__main__':
    # file_name_list = [
    #     'query3.sql', 'query7.sql', 'query8.sql', 'query9.sql',
    #     'query13.sql', 'query15.sql', 'query17.sql', 'query18.sql', 'query19.sql',
    #     'query22.sql', 'query24.sql', 'query25.sql', 'query26.sql', 'query27.sql', 'query28.sql',
    #     'query30.sql', 'query31.sql', 'query33.sql', 'query34.sql', 'query38.sql', 'query39.sql',
    #     'query41.sql', 'query42.sql', 'query43.sql', 'query44.sql', 'query45.sql', 'query46.sql', 'query48.sql',
    #     'query50.sql', 'query51.sql', 'query52.sql', 'query53.sql', 'query54.sql', 'query55.sql',
    #     'query56.sql',
    #     'query60.sql', 'query61.sql', 'query62.sql', 'query63.sql', 'query65.sql', 'query66.sql',
    #     'query67.sql', 'query68.sql', 'query69.sql',
    #     'query71.sql', 'query72.sql', 'query73.sql', 'query75.sql', 'query76.sql', 'query79.sql',
    #     'query81.sql', 'query83.sql', 'query84.sql', 'query85.sql', 'query87.sql', 'query88.sql',
    #     'query89.sql',
    #     'query90.sql', 'query91.sql', 'query91.sql', 'query93.sql', 'query96.sql', 'query97.sql',
    #     'query99.sql'
    # ]
    file_name_list = ['query8.sql', 'query15.sql', 'query17.sql', 'query18.sql', 'query19.sql', 'query25.sql',
                      'query43.sql', 'query53.sql', 'query55.sql', 'query63.sql', 'query67.sql', 'query89.sql', 'query91',
                      'query22.sql', 'query39.sql', 'query90.sql']
    # file_name_list = ['query3.sql', 'query38.sql', 'query87.sql']
    # file_name_list = ['query65.sql', 'query66.sql', 'query85.sql']

    # file_name_list = ['query72.sql', 'query83.sql']
    # file_name_list = ['query22.sql', 'query39.sql', 'query90.sql']
    # file_name_list = ['query41.sql', 'query44.sql']
    join_pair_freduency = find_join_pair(file_name_list)
    draw_dot(join_id_and_frequency(join_pair_freduency), "ALL")
    # for file in file_name_list:
    #     join_pair_freduency = find_join_pair(file)
    #     draw_dot(join_id_and_frequency(join_pair_freduency), file)

