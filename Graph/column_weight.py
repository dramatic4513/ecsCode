'''
输入 连接对
遍历所有的连接，得到一个连接中包含的所有属性的set
然后遍历这个set建立以属性特征为key，value为0的字典 a
建立以属性特征“n_ l_ ps_”为key，表大小为value的字典，表大小即为权重
遍历所有的连接对，根据连接两端的特征，为字典a中的属性赋值
对字典中的属性进行排序，找到最大的属性，将这个属性作为分区键，所有和这个属性连接的表都以这个属性进行分区
然后去掉连接对中所有已经被分配了属性的表参与了的连接
输出分区键

最后如果有表
没有被分配则默认按照主键进行分区

和原来的主键进行对比，显示出需要更改的分区键，并且生成SQL语句
'''

# ALTER TABLE SUPPLIER DISTRIBUTE BY HASH(s_nationkey);

import parseSQL

table_columns_dict = {'n_': 'NATION', 'r_': 'REGION', 'p_': 'PART', 's_': 'SUPPLIER', 'ps_': 'PARTSUPP',
                      'c_': 'CUSTOMER', 'o_': 'ORDERS', 'l_': 'LINEITEM'}
table_key = {'NATION': 'N_NATIONKEY', 'REGION': 'R_REGIONKEY', 'PART': 'P_PARTKEY', 'SUPPLIER': 'S_SUPPKEY',
             'PARTSUPP': "PS_PARTKEY", 'CUSTOMER': 'C_CUSTKEY', 'ORDERS': 'O_ORDERKEY', 'LINEITEM': 'L_ORDERKEY'}


def find_column_as_partition_key(join_pairs):
    """
    :type join_pairs: list
    :rtype: tuple
    """
    res_sql = [] #存储生成的分区语句
    column_weight_dict = {}
    for join_pair in join_pairs:
        key = join_pair[0]
        value = 0
        if key not in column_weight_dict:
            column_weight_dict[key] = value
        key = join_pair[1]
        if key not in column_weight_dict:
            column_weight_dict[key] = value
    # 将表大小与该表的属性特征对应起来
    table_size_dict = {'n_': 16, 'r_': 16, 'p_': 3328, 's_': 232, 'ps_': 14000, 'c_': 2928, 'o_': 20000, 'l_': 88000}
    # 遍历连接对，计算属性的权重值
    for join_pair in join_pairs:
        key1 = join_pair[0].split("_")[0] + "_"
        key2 = join_pair[1].split("_")[0] + "_"
        if key1 not in table_size_dict:
            continue
        if key2 not in table_size_dict:
            continue
        value = table_size_dict[key1] + table_size_dict[key2]
        column_weight_dict[join_pair[0]] = column_weight_dict[join_pair[0]] + value
        column_weight_dict[join_pair[1]] = column_weight_dict[join_pair[1]] + value
    # 将属性按照权重值大小进行排序，找到最大的权重值作为对应表的分区键
    sorted_keys = sorted(column_weight_dict, key=column_weight_dict.get, reverse=True)
    partition_key = sorted_keys[0]
    table_name = table_columns_dict[partition_key.split('_')[0] + "_"]
    sql_str = "ALTER TABLE " + table_name + " DISTRIBUTE BY HASH(" + partition_key + ");"
    # print(table_name, partition_key)
    if table_key[table_name.upper()] != partition_key.upper():
        print(table_name + " " + table_key[table_name.upper()] + " -> " + partition_key)
    print(sql_str)
    res_sql.append(sql_str)
    table_partition_key_dict = {table_columns_dict[partition_key.split('_')[0] + "_"]: partition_key}
    connection_table_column_pre = []
    connection_table_column_pre.append(partition_key.split('_')[0] + "_")
    # 找到这个分区键参与的连接对，以及对应的表，所有参与连接的表，都已该属性作为分区键，将这些连接从连接对列表中去除
    join_pairs_new = []
    for join_pair in join_pairs:
        if join_pair[0] == partition_key:
            key = join_pair[1].split("_")[0] + "_"
            connection_table_column_pre.append(key)
            table = table_columns_dict[key]
            value = key + partition_key.split("_")[1]
            if table not in table_partition_key_dict:
                # print("\n\n!!! Waring : " + str(table) + " is already partition key " + partition_key + "\n\n")
                table_partition_key_dict[table] = value
                sql_str = "ALTER TABLE " + table + " DISTRIBUTE BY HASH(" + value + ");"
                # print(table + " " + value)
                print(sql_str)
                res_sql.append(sql_str)
                if table_key[table.upper()] != value.upper():
                    print(table + " " + table_key[table.upper()] + " -> " + value)
        elif join_pair[1] == partition_key:
            key = join_pair[0].split("_")[0] + "_"
            connection_table_column_pre.append(key)
            table = table_columns_dict[key]
            value = key + partition_key.split("_")[1]
            if table not in table_partition_key_dict:
                # print("\n\n!!! Waring : " + str(table) + " is already partition key " + partition_key + "\n\n")
                table_partition_key_dict[table] = value
                sql_str = "ALTER TABLE " + table + " DISTRIBUTE BY HASH(" + value + ");"
                # print(table + " " + value)
                print(sql_str)
                res_sql.append(sql_str)
                if table_key[table.upper()] != value.upper():
                    print(table + " " + table_key[table.upper()] + " -> " + value)
        else:
            join_pairs_new.append(join_pair)
    # 只是去掉上述连接还不够，还需要去除参与连接的表的其他所有连接
    join_pairs_result = []
    for join_pair in join_pairs_new:
        if connection_table_column_pre.__contains__(join_pair[0].split("_")[0] + "_"):
            continue
        elif connection_table_column_pre.__contains__(join_pair[1].split("_")[0] + "_"):
            continue
        else:
            join_pairs_result.append(join_pair)
    return table_partition_key_dict, join_pairs_result, res_sql

def tpch_greedy_column_alter_table_sql():
    # filename_list = ['db1.sql', 'db2.sql', 'db3.sql', 'db4.sql', 'db5.sql']
    # filename_list = ['db6.sql', 'db7.sql', 'db8.sql', 'db9.sql', 'db10.sql']
    # filename_list = ['db11.sql', 'db12.sql', 'db13.sql', 'db14.sql', 'db15.sql']
    filename_list = ['db16.sql', 'db17.sql', 'db18.sql', 'db19.sql', 'db21.sql', 'db22.sql']
    join_pairs_list = parseSQL.folder_sql_to_columns(filename_list)  # 获取这个文件夹内的SQL中的所有连接
    for join_pair in join_pairs_list:
        print(join_pair)
    table_partiton_key_dict = {}
    res_list = []  # 生成的分区语句
    while join_pairs_list.__len__() > 0:
        res_dict, join_pairs_list, alter_table_sql = find_column_as_partition_key(join_pairs_list)
        for sql in alter_table_sql:
            res_list.append(sql)
    return res_list


if __name__ == '__main__':
    a = tpch_greedy_column_alter_table_sql()
    print("-----------------------------")
    for i in a:
        print(i)




# 主键分区
'''
ALTER TABLE NATION DISTRIBUTE BY HASH(N_NATIONKEY);
ALTER TABLE REGION DISTRIBUTE BY HASH(R_REGIONKEY);
ALTER TABLE PART DISTRIBUTE BY HASH(P_PARTKEY);
ALTER TABLE SUPPLIER DISTRIBUTE BY HASH(S_SUPPKEY);
ALTER TABLE PARTSUPP DISTRIBUTE BY HASH(PS_PARTKEY);
ALTER TABLE CUSTOMER DISTRIBUTE BY HASH(C_CUSTKEY);
ALTER TABLE ORDERS DISTRIBUTE BY HASH(O_ORDERKEY);
ALTER TABLE LINEITEM DISTRIBUTE BY HASH(L_ORDERKEY);
'''



