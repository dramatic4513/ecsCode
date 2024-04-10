import matplotlib.pyplot as plt
import pandas as pd

def query_39():
    df_primary = pd.read_csv('/home/postgres/code/data/net/net_3_five/tpcds39primarykey')
    df_replication = pd.read_csv('/home/postgres/code/data/net/net_3_five/tpcds39replication')

    y1 = df_primary['recv']
    y2 = df_replication['recv']

    x1 = range(1, len(y1) + 1)
    x2 = range(len(y1) + 1, len(y1) + len(y2) + 1)


    plt.plot(x1, y1, marker='o')
    plt.plot(x2, y2, marker='o')

    plt.axvline(len(y1) + 0.5, color='red', linestyle='--')


    plt.xlabel('Q39', fontsize=14)  # 设置 x 轴标签
    plt.ylabel('Network Cost/B', fontsize=14)  # 设置 y 轴标签
    #     plt.title('tpch_WD16_network')  # 设置标题
    #     # plt.grid(True)  # 添加网格线
    plt.legend(['before optimize', 'after optimize'],loc='upper center', bbox_to_anchor=(0.5, 1.15), fontsize = 12, ncol = 2)
    plt.show()  # 显示图形


def query_27():
    df_primary = pd.read_csv('/home/postgres/code/data/net/tpcds27primary')
    df_replication = pd.read_csv('/home/postgres/code/data/net/query27replication')
    df_GEA_GAA = pd.read_csv('/home/postgres/code/data/net/tpcds27_GEA_GAA')
    df_PEA_PAA = pd.read_csv('/home/postgres/code/data/net/tpcds27_PEA_PAA')

    y1 = df_primary['recv']
    y2 = df_replication['recv']
    y3 = df_GEA_GAA['recv']
    y4 = df_PEA_PAA['recv']

    x1 = range(1, len(y1) + 1)
    x2 = range(len(y1) + 1, len(y1) + len(y2) + 1)
    x3 = range(len(y1) + 1, len(y1)+ len(y3) + 1)
    x4 = range(len(y1) + 1, len(y1) +len(y4)+ 1)

    plt.plot(x1, y1, marker='o')
    # plt.plot(x2, y2, marker='o')
    plt.plot(x3, y3, marker='o')
    # plt.plot(x4, y4, marker='o')

    plt.axvline(len(y1) + 0.5, color='red', linestyle='--')

    plt.xlabel('Q27', fontsize = 14)  # 设置 x 轴标签
    plt.ylabel('Network Cost/B', fontsize = 14)  # 设置 y 轴标签
    plt.legend(['before optimize', 'after optimize'],loc='upper center', bbox_to_anchor=(0.5, 1.15), fontsize = 12, ncol = 2)
    plt.show()  # 显示图形


def query_65():
    df_primary = pd.read_csv('/home/postgres/code/data/net/tpcds65primary')
    df_replication = pd.read_csv('/home/postgres/code/data/net/tpcds65GPEA')

    y1 = df_primary['recv']
    y2 = df_replication['recv']

    x1 = range(1, len(y1) + 1)
    x2 = range(len(y1) + 1, len(y1) + len(y2) + 1)


    plt.plot(x1, y1, marker='o')
    plt.plot(x2, y2, marker='o')

    plt.axvline(len(y1) + 0.5, color='red', linestyle='--')


    plt.xlabel('Q65', fontsize=14)  # 设置 x 轴标签
    plt.ylabel('Network Cost/B', fontsize=14)  # 设置 y 轴标签
    #     plt.title('tpch_WD16_network')  # 设置标题
    #     # plt.grid(True)  # 添加网格线
    plt.legend(['before optimize', 'after optimize'],loc='upper center', bbox_to_anchor=(0.5, 1.15), fontsize = 12, ncol = 2)
    plt.show()  # 显示图形

def query_8():
    df_primary = pd.read_csv('/home/postgres/code/data/net/tpcds8primary')
    df_replication = pd.read_csv('/home/postgres/code/data/net/tpcds8GPG')

    y1 = df_primary['recv']
    y2 = df_replication['recv']

    x1 = range(1, len(y1) + 1)
    x2 = range(len(y1) + 1, len(y1) + len(y2) + 1)


    plt.plot(x1, y1, marker='o')
    plt.plot(x2, y2, marker='o')

    plt.axvline(len(y1) + 0.5, color='red', linestyle='--')


    plt.xlabel('Q8', fontsize=14)  # 设置 x 轴标签
    plt.ylabel('Network Cost/B', fontsize=14)  # 设置 y 轴标签
    #     plt.title('tpch_WD16_network')  # 设置标题
    #     # plt.grid(True)  # 添加网格线
    plt.legend(['before optimize', 'after optimize'],loc='upper center', bbox_to_anchor=(0.5, 1.15), fontsize = 12, ncol = 2)
    plt.show()  # 显示图形

def query_17():
    # df_primary = pd.read_csv('/home/postgres/tpc/res/net_3_five/tpcds17primaryGAA')
    df_primary = pd.read_csv('/home/postgres/tpc/res/net_3_five/tpcds17primary*4')
    df_replication = pd.read_csv('/home/postgres/tpc/res/net_3_five/tpcds17GEA')
    # df_replication = pd.read_csv('/home/postgres/tpc/res/net_3_five/tpcds17primary*4')

    y1 = df_primary['recv']
    y2 = df_replication['recv']

    x1 = range(1, len(y1) + 1)
    x2 = range(len(y1) + 1, len(y1) + len(y2) + 1)

    plt.plot(x1, y1, marker='o')
    plt.plot(x2, y2, marker='o')

    plt.axvline(len(y1) + 0.5, color='red', linestyle='--')

    plt.xlabel('Q17', fontsize=14)  # 设置 x 轴标签
    plt.ylabel('Network Cost/B', fontsize=14)  # 设置 y 轴标签
    #     plt.title('tpch_WD16_network')  # 设置标题
    #     # plt.grid(True)  # 添加网格线
    plt.legend(['before optimize', 'after optimize'], loc='upper center', bbox_to_anchor=(0.5, 1.15), fontsize=12,
               ncol=2)
    plt.show()  # 显示图形

def wd4():
    # df_primary = pd.read_csv('/home/postgres/tpc/res/net_3_five/tpcds17primaryGAA')
    df_primary = pd.read_csv('/home/postgres/tpc/res/net_3_five/wd4primary')
    df_replication = pd.read_csv('/home/postgres/tpc/res/net_3_five/wd4PAA')
    # df_replication = pd.read_csv('/home/postgres/tpc/res/net_3_five/tpcds17primary*4')

    y1 = df_primary['recv']
    y2 = df_replication['recv']

    x1 = range(1, len(y1) + 1)
    x2 = range(len(y1) + 1, len(y1) + len(y2) + 1)

    plt.plot(x1, y1, marker='o')
    plt.plot(x2, y2, marker='o')

    plt.axvline(len(y1) + 0.5, color='red', linestyle='--')

    plt.xlabel('Q4', fontsize=14)  # 设置 x 轴标签
    plt.ylabel('Network Cost/B', fontsize=14)  # 设置 y 轴标签
    #     plt.title('tpch_WD16_network')  # 设置标题
    #     # plt.grid(True)  # 添加网格线
    plt.legend(['before optimize', 'after optimize'], loc='upper center', bbox_to_anchor=(0.5, 1.15), fontsize=12,
               ncol=2)
    plt.show()  # 显示图形

if __name__ == '__main__':
    # query_27()
    # query_39()
    # query_65()
    # query_8()
    # query_17()
    wd4()