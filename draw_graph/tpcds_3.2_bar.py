import matplotlib.pyplot as plt

def tpcds_27():
    # 数据
    x = [1, 2, 3, 4, 5, 6]
    y = [7913, 5137, 4915, 4581, 4866, 4701]

    # 标签
    labels = ['primary', 'GEA', 'PEA', 'GAA', 'PAA', 'replication']
    colors = ['grey', '#4169E1', 'green', 'purple', 'orange', 'skyblue']
    # hatchs = ['', '-  -', '.', '+', 'x', '-.']

    # 创建柱状图，并指定颜色
    # plt.bar(x, y, color=colors, label=labels, hatch=hatchs)
    plt.bar(x, y, color=colors, label=labels)

    # # 添加标签
    # for i in range(len(x)):
    #     plt.text(x[i], y[i] + 100, labels[i], ha='center')

    # 添加标题和标签
    # plt.title('Bar Chart Example')
    plt.xlabel('Q27', fontsize = 14)
    plt.ylabel('Runtime(milliseconds)', fontsize = 14)

    # 显示图例
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=3)
    # plt.legend(ncol=3, fontsize = 12)

    # 显示图形
    plt.show()

def tpcds_39():
    # 数据
    x = [1, 2, 3, 4, 5, 6]
    y = [41611, 41109, 41077, 40780, 39437, 35261]

    # 标签
    labels = ['primary', 'GEA', 'PEA', 'GAA', 'PAA', 'replication']
    colors = ['grey', '#4169E1', 'green', 'purple', 'orange', 'skyblue']
    # hatchs = ['', '-  -', '.', '+', 'x', '-.']

    # 创建柱状图，并指定颜色
    # plt.bar(x, y, color=colors, label=labels, hatch=hatchs)
    plt.bar(x, y, color=colors, label=labels)

    # # 添加标签
    # for i in range(len(x)):
    #     plt.text(x[i], y[i] + 100, labels[i], ha='center')

    # 添加标题和标签
    # plt.title('Bar Chart Example')
    plt.xlabel('Q39', fontsize = 14)
    plt.ylabel('Runtime(milliseconds)', fontsize = 14)

    # 显示图例
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=3)
    # plt.legend(ncol=3)

    # 显示图形
    plt.show()

def tpcds_17():
    # 数据
    x = [1, 2, 3, 4, 5, 6]
    y = [5704, 3503, 3072, 4670, 3077, 3043]

    # 标签
    labels = ['primary', 'GEA', 'PEA', 'GAA', 'PAA', 'replication']
    colors = ['grey', '#4169E1', 'green', 'purple', 'orange', 'skyblue']
    # hatchs = ['', '-  -', '.', '+', 'x', '-.']

    # 创建柱状图，并指定颜色
    # plt.bar(x, y, color=colors, label=labels, hatch=hatchs)
    plt.bar(x, y, color=colors, label=labels)

    # # 添加标签
    # for i in range(len(x)):
    #     plt.text(x[i], y[i] + 100, labels[i], ha='center')

    # 添加标题和标签
    # plt.title('Bar Chart Example')
    plt.xlabel('Q17', fontsize = 14)
    plt.ylabel('Runtime(milliseconds)', fontsize = 14)

    # 显示图例
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=3)
    # plt.legend(ncol=3)

    # 显示图形
    plt.show()

def tpcds_65():
    # 数据
    x = [1, 2, 3, 4, 5, 6]
    y = [31606, 9391, 8993, 9192, 8766, 8306]

    # 标签
    labels = ['primary', 'GEA', 'PEA', 'GAA', 'PAA', 'replication']
    colors = ['grey', '#4169E1', 'green', 'purple', 'orange', 'skyblue']
    # hatchs = ['', '-  -', '.', '+', 'x', '-.']

    # 创建柱状图，并指定颜色
    # plt.bar(x, y, color=colors, label=labels, hatch=hatchs)
    plt.bar(x, y, color=colors, label=labels)

    # # 添加标签
    # for i in range(len(x)):
    #     plt.text(x[i], y[i] + 100, labels[i], ha='center')

    # 添加标题和标签
    # plt.title('Bar Chart Example')
    plt.xlabel('Q65', fontsize = 14)
    plt.ylabel('Runtime(milliseconds)', fontsize = 14)

    # 显示图例
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=3)
    # plt.legend(ncol=3)

    # 显示图形
    plt.show()
def tpcds_8():
    # 数据
    x = [1, 2, 3, 4, 5, 6]
    y = [15119, 10413, 10130, 9866, 9366, 13342]

    # 标签
    labels = ['primary', 'GEA', 'PEA', 'GAA', 'PAA', 'replication']
    colors = ['grey', '#4169E1', 'green', 'purple', 'orange', 'skyblue']
    # hatchs = ['', '-  -', '.', '+', 'x', '-.']

    # 创建柱状图，并指定颜色
    # plt.bar(x, y, color=colors, label=labels, hatch=hatchs)
    plt.bar(x, y, color=colors, label=labels)

    # # 添加标签
    # for i in range(len(x)):
    #     plt.text(x[i], y[i] + 100, labels[i], ha='center')

    # 添加标题和标签
    # plt.title('Bar Chart Example')
    plt.xlabel('Q8', fontsize = 14)
    plt.ylabel('Runtime(milliseconds)', fontsize = 14)

    # 显示图例
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=3)
    # plt.legend(ncol=3)

    # 显示图形
    plt.show()

def wd4():
    # 数据
    x = [1, 2, 3, 4, 5, 6]
    y = [52977, 45962, 33377, 30380, 21799, 23044]

    # 标签
    labels = ['primary', 'GEA', 'PEA', 'GAA', 'PAA', 'replication']
    colors = ['grey', '#4169E1', 'green', 'purple', 'orange', 'skyblue']
    # hatchs = ['', '-  -', '.', '+', 'x', '-.']

    # 创建柱状图，并指定颜色
    # plt.bar(x, y, color=colors, label=labels, hatch=hatchs)
    plt.bar(x, y, color=colors, label=labels)

    # # 添加标签
    # for i in range(len(x)):
    #     plt.text(x[i], y[i] + 100, labels[i], ha='center')

    # 添加标题和标签
    # plt.title('Bar Chart Example')
    plt.xlabel('Q4', fontsize = 14)
    plt.ylabel('Runtime(milliseconds)', fontsize = 14)

    # 显示图例
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=3)
    # plt.legend(ncol=3)

    # 显示图形
    plt.show()

if __name__ == '__main__':
    # tpcds_27()
    # tpcds_39()
    # tpcds_17()
    # tpcds_65()
    # tpcds_8()
    wd4()