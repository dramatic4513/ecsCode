import matplotlib.pyplot as plt
import pandas as pd

df_primary = pd.read_csv('/home/postgres/code/data/net_tpch100_db6.sql')
df_los = pd.read_csv('/home/postgres/code/data/net_tpch100los_db6.sql')
df_d1_primary_16 = pd.read_csv('/home/postgres/tpc/tpch/res/tpch_net/primary16.csv')
df_d1_los_16 = pd.read_csv('/home/postgres/tpc/tpch/res/tpch_net/los16.csv')
y1 = df_primary['recv']
y2 = df_los['recv']
y3 = df_d1_primary_16['recv'] + df_d1_primary_16['send']
y4 = df_d1_los_16['recv'] + df_d1_los_16['send']
x1 = range(1, len(y1) + 1)
x2 = range(1, len(y2) + 1)
x3 = range(1, len(y3) + 1)
x4 = range(1, len(y4) + 1)

# # 绘制折线图
# plt.plot(x1, y1)
# plt.plot(x2, y2)
plt.plot(x3, y3)
plt.plot(x4, y4)
plt.xlabel('time/s')  # 设置 x 轴标签
plt.ylabel('network/B')  # 设置 y 轴标签
plt.title('tpch_WD16_network')  # 设置标题
# plt.grid(True)  # 添加网格线
plt.show()  # 显示图形
