import matplotlib.pyplot as plt
import pandas as pd


def cycle():
    # # df_primary = pd.read_csv('/home/postgres/tpc/res/net_3_five/tpcds17primaryGAA')
    # df_primary = pd.read_csv('/home/postgres/tpc/res/net_3_five/tpcds17primary*4')
    # df_replication = pd.read_csv('/home/postgres/tpc/res/net_3_five/tpcds17GEA')
    # # df_replication = pd.read_csv('/home/postgres/tpc/res/net_3_five/tpcds17primary*4')

    # y1 = df_primary['recv']
    # y2 = df_replication['recv']
    y1 = [31873, 34004, 29688, 38513, 31322, 32864, 32059, 29859, 7360, 414, 7196, 9501, 7485, 1226, 812, 33608, 34244, 35831, 31827, 31322, 32864, 32059, 29859, 7360, 414, 7196, 9501, 7485, 1226, 812, 31873, 34004, 29688, 38513, 31606, 33967, 23247, 23104, 6293, 341, 7948, 11449, 2589, 1029, 728]
    y2 = [9391, 8993, 8956, 9199, 9121, 8766, 8306, 8652, 5924, 601, 5968, 5779, 2501, 1442, 826]

    x1 = range(1, len(y1) + 1)
    x2 = range(len(y1), len(y1) + len(y2))

    plt.plot(x1, y1)
    plt.plot(x2, y2)

    plt.axvline(len(y1) , color='red', linestyle='--')

    # plt.xlabel('Q17', fontsize=14)  # 设置 x 轴标签
    plt.ylabel('Runtime', fontsize=14)  # 设置 y 轴标签
    #     plt.title('tpch_WD16_network')  # 设置标题
    #     # plt.grid(True)  # 添加网格线
    plt.legend(['before optimize', 'after optimize'], loc='upper center', bbox_to_anchor=(0.5, 1.15), fontsize=12,
               ncol=2)
    plt.show()  # 显示图形


def normal():
    # 数据
    # x = [1, 2, 3, 4, 5, 6]
    y1 = [7913, 41611, 5704, 31606, 15119, 52997, 4596, 10413, 4670, 9391, 414, 7196, 9501, 7485, 1226, 812, 13030]
    y2 = [4701, 35261, 3043, 8306, 13342, 2304, 2179, 9366, 3077, 8766, 407, 5883, 6396, 2069, 970, 775, 9803]
    x = range(1, len(y1) + 1)

    # 创建折线图
    plt.plot(x, y1, label='before optimize', markersize=8)
    plt.plot(x, y2, label='after optimize', markersize=8)


    # # 设置x轴刻度标签
    # plt.xticks(x, ['1', '2', '3', '4', '5', '6'])

    # 添加标题和标签
    plt.xlabel('time', fontsize=14)
    plt.ylabel('Runtime', fontsize=14)

    # 显示图例
    plt.legend()

    # 显示图形
    plt.show()

def hit_ration():
    y1 = [0.25, 0.75, 0.5, 0.33, 0.66, 0.5] #命中率
    y2 = [0.034,  0.43, 0.111, 0.1598, 0.4413, 0.258] #加速
    x = range(1, len(y1) + 1)

    # 创建折线图
    plt.plot(x, y1, label='Hint ration', marker='o', markersize=8)
    plt.plot(x, y2, label='Speed up ration', marker='^', markersize=8)

    # 添加标题和标签
    plt.xlabel('Q', fontsize=14)
    plt.ylabel('ratio', fontsize=14)

    # 显示图例
    plt.legend()

    # 显示图形
    plt.show()

def alter():
    table_size = [8704.0, 174624.0, 11248.0, 16.0, 16.0, 11176.0, 16.0, 16.0, 9832.0, 16.0, 16.0, 22136.0, 16.0, 38328.0, 432.0,
     16.0, 64.0, 1936.0, 507904.0, 23024.0, 10616.0, 150136.0, 299240.0, 413576.0]
    alter_time = [0.02107381820678711, 7.695866823196411, 0.3258786201477051, 0.013656854629516602, 0.03949475288391113, 0.37360095977783203, 0.002386808395385742, 0.013092994689941406, 0.18182873725891113, 0.09487628936767578, 0.019603729248046875, 1.055877923965454, 0.019099950790405273, 1.6947431564331055, 0.055861473083496094, 0.014699220657348633, 0.03711962699890137, 0.04049038887023926, 42.802507162094116, 0.778648853302002, 0.3796236515045166, 8.525384902954102, 16.237228393554688, 25.08694624900818]
    y1 = []
    y2 = []
    for i in table_size:
        y1.append(i)
    for i in alter_time:
        y2.append(i * 1500)
    # size_time = [(8704.0, 0.02107381820678711), (174624.0, 7.695866823196411),
    #              (11248.0, 0.3258786201477051), (16.0, 0.013656854629516602), (16.0, 0.03949475288391113),
    #              (11176.0, 0.37360095977783203), (16.0, 0.002386808395385742), (16.0, 0.013092994689941406),
    #              (9832.0, 0.18182873725891113), (16.0, 0.09487628936767578), (16.0, 0.019603729248046875),
    #              (22136.0, 1.055877923965454), (16.0, 0.019099950790405273), (38328.0, 1.6947431564331055),
    #              (432.0, 0.055861473083496094), (16.0, 0.014699220657348633), (64.0, 0.03711962699890137),
    #              (1936.0, 0.04049038887023926), (507904.0, 42.802507162094116), (23024.0, 0.778648853302002),
    #              (10616.0, 0.3796236515045166), (150136.0, 8.525384902954102), (299240.0, 16.237228393554688),
    #              (413576.0, 25.08694624900818)]

    x = range(1, len(y1) + 1)

    # 创建折线图
    plt.plot(x, y1, label='table_size', marker='o', markersize=8)
    plt.plot(x, y2, label='alter_time', marker='^', markersize=8)

    # 添加标题和标签
    # plt.xlabel('Q', fontsize=14)
    # plt.ylabel('ratio', fontsize=14)

    # 显示图例
    plt.legend()

    # 显示图形
    plt.show()


if __name__ == '__main__':
    # cycle()
    # normal()
    # hit_ration()
    alter()
