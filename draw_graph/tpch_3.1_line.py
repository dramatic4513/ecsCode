import matplotlib.pyplot as plt

# 数据
x = [1, 2, 3, 4, 5]
# y1 = [471, 580, 505, 581, 640]
# y2 = [879, 487, 716, 485, 891]
# y1 = [79866,  85065, 76866, 125747, 174066]
# y2 = [187299, 158449, 187299, 125768, 333210]
# y1 = [186,  85, 76, 125, 174]
# y2 = [334, 158, 187, 125, 333]
y1 = [186, 174, 85, 76, 125]
y2 = [334, 333, 158, 187, 125]

# 创建折线图
plt.plot(x, y1, marker='o', label='Q1', markersize=8)
plt.plot(x, y2, marker='^', label='Q2', markersize=8)
# plt.text(3, 600, r'$\mu=100$', fontsize=15)  # 在坐标 (1, 4) 处添加 LaTeX 文本

# 设置y轴范围从300开始
# plt.ylim(300, 1000)

# 设置x轴刻度标签
plt.xticks(x, ['1', '2', '3', '4', '5'])

# 添加标题和标签
# plt.title('TPC-H')
plt.xlabel('data partitions', fontsize=14)
plt.ylabel('Runtime(second)',fontsize=14)

# 显示图例
plt.legend()

#
# # 在右上角标注展示 Q1、Q2
# plt.text(4, 1000, r'$Q1$', fontsize=12, verticalalignment='top', horizontalalignment='right')
# plt.text(4, 930, r'$Q2$', fontsize=12, verticalalignment='top', horizontalalignment='right')

# 显示图形
plt.show()
#
# import matplotlib.pyplot as plt
#
# # 数据
# x = [1, 2, 3, 4]
# y1 = [471, 580, 505, 581]
# y2 = [879, 487, 716, 485]
#
# # 创建折线图
# plt.plot(x, y1, marker='o', markersize=8, label='Q1')
# plt.plot(x, y2, marker='^', markersize=8, label='Q2')
#
# # 设置y轴范围从300开始
# plt.ylim(300, 1000)
#
# # 设置x轴刻度标签
# plt.xticks(x, ['DP1', 'DP2', 'DP3', 'DP4'])
#
# # 添加标题和标签
# plt.ylabel('Runtime(millisecond)')
#
# # 在右上角添加标注
# # plt.text(3.8, 950, r'$Q1$', fontsize=12)
# # plt.text(3.8, 920, r'$Q2$', fontsize=12)
#
# # 显示图形
# plt.show()



