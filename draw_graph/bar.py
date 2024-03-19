import os

import matplotlib.pyplot as plt

'''
在不同的数据分区下，执行SQL时间
'''

def tpch_greedy_net_bit_count_3_2():
    folder_path = '/home/postgres/tpc/tpch/res'
    file_list = os.listdir(folder_path)
    for file_name in file_list:
        if file_name.startswith('new_net_'):
            file_path = os.path.join(folder_path, file_name)
            i = 1
            total_bit = 0
            with open(file_path, 'r') as f:
                for line in f:
                    if i > 8:
                        words = line.split(',')
                        recv = float(words[0])
                        send = float(words[1])
                        if recv > 10000 and send > 10000:
                            total_bit += recv
                            total_bit += send
                    i = i + 1
                f.close()
            print(file_name + " total_bit  " + str(total_bit))

def tpch_greedy_16_22_runtime_3_2():
    name_list = ['sql16', 'sql17', 'sql18', 'sql19', 'sql21', 'sql22']
    primary_y = [83, 13023, 489, 12832, 295066, 113]
    greedy_y = [70, 13551, 3241, 13633, 90810, 34]
    x = list(range(len(primary_y)))
    width = 0.4
    plt.bar(x, primary_y, width=width, tick_label=name_list, align='center')
    for i in range(len(x)):
        x[i] = x[i] + width
    plt.bar(x, greedy_y, width=width, tick_label=name_list, align='center')
    plt.title("TPCH Greedy")
    plt.show()
    #因为展示效果不明显，所以我们将sql16， sql22的运行时间乘以10，sql21的运行时间除以10
    plt.title("16*10 22*10 21/10")
    primary_y = [830, 13023, 489, 12832, 29506, 1130]
    greedy_y = [700, 13551, 3241, 13633, 9081, 340]
    x = list(range(len(primary_y)))
    width = 0.4
    plt.bar(x, primary_y, width=width, tick_label=name_list, align='center')
    for i in range(len(x)):
        x[i] = x[i] + width
    plt.bar(x, greedy_y, width=width, tick_label=name_list, align='center')
    plt.show()
def tpch_greedy_total_time_3_2():
    #贪心算法得到不同的数据分区，然后不同的工作负载在不同的数据分区上的执行总时间
    #6-10 s_nationkey c_nationkey
    #11-15 ps_suppkey
    #16-22 l_partkey o_custkey s_nationkey
    name_list = ["primary", "6-10", "11-15", "16-22"]
    total_6_y = [317002, 197723, 191891, 52223]
    total_11_y = [681, 798, 645,667]
    total_15_y = [321609,325194,320093,121342]
    x = list(range(len(total_6_y)))
    width = 0.8
    plt.bar(x, total_6_y, width = width, align='center', tick_label = name_list)
    plt.title("TPCH 6-10 SQL")
    plt.show()
    plt.bar(x, total_11_y, width = width, align='center', tick_label = name_list)
    plt.title("TPCH 11-15 SQL")
    plt.show()
    plt.bar(x, total_15_y, width = width, align='center', tick_label = name_list)
    plt.title("TPCH 16-22 SQL")
    plt.show()
    # for i in range(len(x)):
    #     x[i] = x[i] + width
    # plt.bar(x, total_11_y, width = width, align='center')
    # for i in range(len(x)):
    #     x[i] = x[i] + width
    # plt.bar(x, total_15_y, width = width, align='center')
    # plt.show()
    # tpch100m_y = [317002, 681, 321609]
    # tpch100nationkey_y = [197723, 798, 325194]
    # tpch100suppkey_y = [191891, 645, 320093]
    # tpch100loss_y = [52223, 667, 121342]
    # x = list(range(len(tpch100m_y)))
    # total_width, n = 0.6, len(tpch100m_y)
    # width = total_width / n
    # plt.bar(x, tpch100m_y, width=width)
    # for i in range(len(x)):
    #     x[i] = x[i] + width
    # plt.bar(x, tpch100nationkey_y, width=width)
    # for i in range(len(x)):
    #     x[i] = x[i] + width
    # plt.bar(x, tpch100suppkey_y, width=width)
    # for i in range(len(x)):
    #     x[i] = x[i] + width
    # plt.bar(x, tpch100loss_y, width=width)
    # plt.show()



'''
第三章，在不同分区上执行同样的连接，时间不同,网络代价也不同
'''
def tpch100m_3_1():
    name_list = ["orderkey,l_orderkey", "o_custkey,l_suppkey"]
    y1 = [223, 1140]  # o_orderkey = l_orderkey
    y2 = [1823, 570]  # o_custkey = l_suppkey
    x = list(range(len(y1)))
    total_width, n = 0.2, 2
    width = total_width / n
    # plt.bar(x, y1, hatch='xxx', width=width, label="SQL1")
    plt.bar(x, y1,  width=width, label="SQL1")
    # for i, v in enumerate(y1):
    for i in range(len(x)):
        plt.text(x[i], y1[i]+0.5, "Q1", ha="center")
    for i in range(len(x)):
        x[i] = x[i] + width
    # plt.bar(x, y2, hatch='+++', width=width, label="SQL2", tick_label=name_list)
    plt.bar(x, y2, width=width, label="SQL2", tick_label=name_list)
    # for i, v in enumerate(y1):
    for i in range(len(x)):
        plt.text(x[i], y2[i]+0.5, "Q2", ha="center")
    plt.title("TPCH")
    plt.xlabel("partition key")
    plt.ylabel("run time/ms")

    plt.show()


if __name__ == '__main__':
    # tpch100m_3_1()
    # tpch_greedy_net_bit_count_3_2()
    tpch_greedy_total_time_3_2()
    # tpch_greedy_16_22_runtime_3_2()
