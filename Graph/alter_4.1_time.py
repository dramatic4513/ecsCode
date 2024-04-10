import time
import matplotlib.pyplot as plt
import psycopg2
import random

host = "172.23.52.199"
port = 20004
database = "tpcdspb"
user = "postgres"
password = "postgres"
#'dbgen_version': 'dv_version',
table_primary_dict = {'customer_address': 'ca_address_sk',
                      'customer_demographics': 'cd_demo_sk', 'date_dim': 'd_date_sk', 'warehouse': 'w_warehouse_sk',
                      'ship_mode': 'sm_ship_mode_sk', 'time_dim': 't_time_sk', 'reason': 'r_reason_sk',
                      'income_band': 'ib_income_band_sk', 'item': 'i_item_sk', 'store': 's_store_sk',
                      'call_center': 'cc_call_center_sk', 'customer': 'c_customer_sk', 'web_site': 'web_site_sk',
                      'store_returns': 'sr_item_sk', 'household_demographics': 'hd_demo_sk',
                      'web_page': 'wp_web_page_sk', 'promotion': 'p_promo_sk', 'catalog_page': 'cp_catalog_page_sk',
                      'inventory': 'inv_date_sk', 'catalog_returns': 'cr_item_sk', 'web_returns': 'wr_item_sk',
                      'web_sales': 'ws_item_sk', 'catalog_sales': 'cs_item_sk', 'store_sales': 'ss_item_sk'}

def choice_attribute(table):
    # 连接到 PostgreSQL-XL 数据库
    conn = psycopg2.connect(host=host, port=port, database=database, user=user)
    # 创建游标
    cur = conn.cursor()
    # 执行 SQL 查询获取表的属性信息
    cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", (table,))
    # 获取查询结果
    rows = cur.fetchall()
    attributes = list()
    # 打印表的属性信息
    for row in rows:
        if row[1] == 'integer':
            attributes.append(row)
    # 关闭游标和连接
    cur.close()
    conn.close()
    # a = random.choice(attributes)
    # # print(type(a))
    # # print(a[0])
    # return a[0]
    return attributes

def table_size(table_name):
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    cur = connection.cursor()
    cur.execute(f"SELECT pg_relation_size('{table_name}')")
    table_size_bytes = cur.fetchone()[0]
    # print(table_name + " " + str(table_size_bytes))  # 单位B
    cur.close()
    connection.close()
    return table_size_bytes

def alter_table():
    at = list()  #alter time
    ts = list() #table size
    size_time = list()
    conn = psycopg2.connect(host=host, port=port, database=database, user=user)
    for key, value in table_primary_dict.items():
        cur = conn.cursor()
        attributes = choice_attribute(key)
        attribute = random.choice(attributes)
        a = attribute[0]
        while a == value and len(attributes) > 1:
            attribute = random.choice(attributes)
            a = attribute[0]
        sql = "ALTER TABLE " + key + " distribute by hash(" + a + ");"
        print(sql)
        print(value + " -> " + a)
        if (a == value) :
            print(" =================== ")
        size_t = table_size(key) / 1024
        print(size_t)
        ts.append(size_t)
        start_time = time.time()
        cur.execute(sql)
        end_time = time.time()
        epl_time = end_time - start_time
        at.append(epl_time)
        size_time.append((size_t, epl_time))
        print(epl_time)
        cur.close()
    conn.close()
    print(ts)
    print(at)
    print(size_time)


if __name__ == "__main__":
    alter_table()