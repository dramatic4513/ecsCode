import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


def sum_reve():
    print("query39")
    df_primary = pd.read_csv('/home/postgres/code/data/net/net_3_five/tpcds39primarykey')
    df_replication = pd.read_csv('/home/postgres/code/data/net/net_3_five/tpcds39replication')

    y1 = df_primary['recv']
    y2 = df_replication['recv']
    s1 = sum(y1)
    s2 = sum(y2)
    print(s1, s2, s1 / s2, s2 / s1)

    print("query27")
    df_primary = pd.read_csv('/home/postgres/code/data/net/tpcds27primary')
    df_replication = pd.read_csv('/home/postgres/code/data/net/query27replication')
    df_GEA_GAA = pd.read_csv('/home/postgres/code/data/net/tpcds27_GEA_GAA')
    df_PEA_PAA = pd.read_csv('/home/postgres/code/data/net/tpcds27_PEA_PAA')

    y1 = df_primary['recv']
    y2 = df_replication['recv']
    y3 = df_GEA_GAA['recv']
    y4 = df_PEA_PAA['recv']

    s1 = sum(y1)
    s2 = sum(y2)
    s3 = sum(y3)
    s4 = sum(y4)
    print(s1, s2, s3, s4, s1 / s2, s1 / s2, s1 / s4, s4 / s1)

    print("query65")
    df_primary = pd.read_csv('/home/postgres/code/data/net/tpcds65primary')
    df_replication = pd.read_csv('/home/postgres/code/data/net/tpcds65GPEA')

    y1 = df_primary['recv']
    y2 = df_replication['recv']
    s1 = sum(y1)
    s2 = sum(y2)
    print(s1, s2, s1 / s2, s2 / s1)

    print("query8")
    df_primary = pd.read_csv('/home/postgres/code/data/net/tpcds8primary')
    df_replication = pd.read_csv('/home/postgres/code/data/net/tpcds8GPG')

    y1 = df_primary['recv']
    y2 = df_replication['recv']
    s1 = sum(y1)
    s2 = sum(y2)
    print(s1, s2, s1 / s2, s2 / s1)

    print("query17")
    df_primary = pd.read_csv('/home/postgres/tpc/res/net_3_five/tpcds17primary*4')
    df_replication = pd.read_csv('/home/postgres/tpc/res/net_3_five/tpcds17GEA')
    y1 = df_primary['recv']
    y2 = df_replication['recv']
    s1 = sum(y1)
    s2 = sum(y2)
    print(s1, s2, s1 / s2, s2 / s1)

    print("wd4")
    df_primary = pd.read_csv('/home/postgres/tpc/res/net_3_five/wd4primary')
    df_replication = pd.read_csv('/home/postgres/tpc/res/net_3_five/wd4PAA')
    y1 = df_primary['recv']
    y2 = df_replication['recv']
    s1 = sum(y1)
    s2 = sum(y2)
    print(s1, s2, s1 / s2, s2 / s1)


def net_bar():
    x = [1, 2, 3, 4, 5, 6]
    # y_primary = [202330792, 265972170, 1139598551, 116743683, 178948623, 926436243]
    # y_after = [175104531, 255780161, 265953986, 11351030, 170267765, 548131893]
    y_primary = [202, 265, 1139, 116, 178, 926]
    y_after = [175, 255, 265, 11, 170, 548]
    x_lables = ['Q39', 'Q27', 'Q65', 'Q8', 'Q17', 'Q4']

    # 数据
    x = np.arange(len(x_lables))
    width = 0.35  # 柱子宽度

    # 创建柱状图
    fig, ax = plt.subplots()
    bars1 = ax.bar(x - width / 2, y_primary, width, label='before optimize')
    bars2 = ax.bar(x + width / 2, y_after, width, label='after optimize')

    # 添加标签、标题和图例
    ax.set_ylabel('Total network cost/MB')
    # ax.set_title('Comparison between Primary and After')
    ax.set_xticks(x)
    ax.set_xticklabels(x_lables)
    ax.legend()

    plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=3)
    # 显示图形
    plt.show()


def run_time_vs_net_work():
    x = [1, 2, 3, 4, 5, 6]
    y_time = [0.12, 0.42, 0.73, 0.38, 0.47, 0.37]
    y_net = [0.14, 0.04, 0.77, 0.9, 0.05, 0.41]

    plt.plot(x, y_time, marker='o', label='Runtime_ratio', markersize=8)
    plt.plot(x, y_net, marker='^', label='Network_ratio', markersize=8)

    plt.xticks(x, ['1', '2', '3', '4', '5', '6'])

    plt.legend()

    plt.show()


if __name__ == '__main__':
    # sum_reve()
    # net_bar()
    run_time_vs_net_work()
