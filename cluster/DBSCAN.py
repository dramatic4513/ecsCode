import matplotlib.pyplot as plt
import numpy as np
from sklearn.cluster import DBSCAN

id_num_dict = {0: 2, 1: 1, 2: 1, 3: 2, 4: 1, 5: 4, 11: 10, 12: 8, 13: 9, 14: 2, 15: 2, 16: 2, 17: 2, 18: 2, 19: 2, 20: 2, 89: 3, 65: 2, 90: 3, 91: 2, 92: 2, 94: 2, 95: 2, 96: 2, 74: 1}
num_list = []
for key,value in id_num_dict.items():
    item_list = [key, value]
    num_list.append(item_list)
data = np.array(num_list)

# 定义要聚类的数据
# data = np.array([[11, 2, 12, 1], [7, 1, 11, 1], [18, 3, 12, 1], [18, 3, 12, 1], [18, 1, 7, 1], [3, 16, 12, 1], [3, 1, 7, 1], [23, 4, 12, 1], [23, 1, 7, 1], [3, 1, 7, 1], [3, 16, 12, 1], [3, 4, 5, 1], [5, 5, 4, 1], [2, 12, 0, 1], [2, 1, 7, 1], [2, 8, 5, 1], [6, 1, 5, 3], [9, 1, 5, 4], [4, 1, 5, 5], [7, 1, 18, 1], [12, 1, 18, 3], [16, 1, 18, 8], [18, 4, 17, 4], [18, 3, 17, 3], [18, 10, 17, 10], [17, 4, 3, 4], [17, 3, 3, 16], [2, 1, 7, 1], [2, 11, 4, 1], [4, 1, 5, 5], [18, 3, 12, 1], [18, 1, 7, 1], [18, 7, 4, 1], [3, 16, 12, 1], [3, 1, 7, 1], [3, 7, 4, 1], [23, 4, 12, 1], [23, 1, 7, 1], [23, 8, 4, 1], [18, 3, 12, 1], [18, 1, 7, 1], [18, 8, 16, 1], [4, 1, 5, 5], [18, 8, 16, 1], [18, 1, 7, 1], [23, 3, 7, 1], [23, 12, 4, 1], [23, 14, 24, 1], [18, 1, 7, 1], [3, 1, 7, 1], [23, 1, 7, 1], [18, 3, 12, 1], [18, 1, 7, 1], [3, 16, 12, 1], [3, 1, 7, 1], [23, 4, 12, 1], [23, 1, 7, 1], [18, 1, 7, 1], [3, 1, 7, 1], [23, 1, 7, 1], [18, 3, 12, 1], [18, 1, 7, 1], [18, 3, 12, 1], [18, 1, 7, 1], [18, 3, 12, 1], [18, 1, 7, 1], [18, 8, 16, 1], [18, 1, 7, 1], [18, 3, 12, 1], [18, 4, 5, 1], [18, 1, 7, 1], [18, 4, 5, 1], [3, 1, 7, 1], [23, 1, 7, 1], [18, 1, 7, 1], [18, 3, 12, 1], [18, 4, 5, 1], [18, 1, 7, 1], [18, 4, 5, 1], [3, 1, 7, 1], [3, 4, 5, 1], [23, 1, 7, 1], [23, 5, 5, 1], [5, 5, 4, 1], [10, 1, 9, 2], [6, 1, 5, 3], [9, 1, 5, 4], [17, 5, 6, 1], [7, 1, 18, 1], [18, 8, 16, 1], [18, 8, 16, 1], [3, 1, 7, 1], [3, 16, 12, 1], [3, 5, 6, 1], [3, 17, 13, 1], [18, 3, 12, 1], [18, 1, 7, 1], [18, 7, 4, 1], [3, 16, 12, 1], [3, 1, 7, 1], [3, 7, 4, 1], [23, 4, 12, 1], [23, 1, 7, 1], [23, 8, 4, 1], [18, 8, 16, 1], [23, 3, 7, 1], [23, 12, 4, 1], [23, 14, 24, 1], [17, 3, 18, 3], [17, 10, 18, 10], [17, 9, 14, 1], [5, 1, 18, 4], [18, 1, 7, 1], [5, 1, 23, 5], [23, 1, 7, 1], [5, 1, 3, 8], [3, 1, 7, 1], [3, 4, 5, 1], [5, 5, 4, 1], [3, 1, 7, 1], [18, 1, 7, 1], [18, 8, 16, 1], [17, 1, 7, 1], [17, 8, 16, 1], [3, 1, 7, 1], [2, 1, 7, 1], [23, 1, 7, 1], [23, 13, 21, 1], [22, 1, 7, 1], [22, 12, 21, 1], [16, 1, 18, 8], [18, 1, 7, 1], [18, 6, 9, 1], [6, 1, 18, 5], [18, 6, 9, 1], [6, 1, 18, 5], [18, 6, 9, 1], [6, 1, 18, 5], [18, 7, 4, 1], [18, 7, 4, 1], [18, 7, 4, 1], [23, 16, 20, 1], [23, 1, 7, 1], [23, 2, 19, 1], [23, 15, 15, 1], [3, 15, 20, 1], [3, 1, 7, 1], [3, 2, 19, 1], [3, 14, 15, 1], [18, 1, 7, 1], [18, 1, 7, 1], [23, 4, 12, 1], [23, 1, 7, 1], [3, 3, 7, 1], [3, 11, 4, 1], [3, 12, 0, 1], [23, 13, 21, 1], [23, 4, 22, 3], [23, 18, 22, 14], [23, 1, 7, 1], [6, 1, 22, 5], [6, 1, 22, 9], [4, 1, 22, 7], [14, 1, 22, 13], [7, 1, 18, 1], [12, 1, 18, 3], [16, 1, 18, 8], [18, 4, 17, 4], [18, 3, 17, 3], [18, 10, 17, 10], [17, 4, 3, 4], [17, 3, 3, 16], [3, 16, 12, 1], [3, 1, 7, 1], [0, 1, 3, 12], [12, 1, 11, 2], [11, 3, 20, 1], [11, 1, 7, 1], [18, 3, 17, 3], [18, 10, 17, 10], [18, 1, 7, 1], [18, 8, 16, 1], [18, 3, 12, 1], [18, 9, 13, 1], [3, 16, 2, 3], [3, 18, 2, 17], [3, 1, 7, 1], [3, 13, 1, 1], [3, 16, 12, 1], [3, 17, 13, 1], [23, 4, 22, 3], [23, 18, 22, 14], [23, 1, 7, 1], [23, 14, 24, 1], [23, 4, 12, 1], [23, 17, 13, 1], [18, 3, 12, 1], [18, 1, 7, 1], [3, 16, 11, 2], [20, 1, 11, 3], [12, 1, 3, 16], [3, 5, 6, 1], [3, 6, 9, 1], [3, 17, 13, 1], [2, 3, 3, 16], [2, 17, 3, 18], [3, 16, 12, 1], [3, 1, 7, 1], [18, 4, 5, 1], [22, 3, 23, 4], [22, 14, 23, 18], [12, 1, 3, 16], [7, 1, 3, 1], [3, 16, 12, 1], [7, 1, 3, 1], [16, 1, 18, 8], [18, 1, 7, 1], [6, 1, 18, 5], [6, 1, 18, 5], [6, 1, 18, 5], [18, 7, 4, 1], [18, 7, 4, 1], [18, 7, 4, 1], [7, 1, 18, 1], [18, 3, 12, 1], [17, 3, 12, 1], [17, 1, 7, 1], [2, 3, 12, 1], [2, 1, 7, 1], [22, 3, 12, 1], [22, 1, 7, 1], [5, 1, 18, 4], [18, 1, 7, 1], [5, 1, 23, 5], [23, 1, 7, 1], [7, 1, 23, 1], [7, 1, 3, 1], [7, 1, 18, 1], [7, 1, 23, 1], [12, 1, 23, 4], [18, 1, 7, 1], [18, 7, 4, 1], [23, 1, 7, 1], [23, 8, 4, 1], [11, 2, 12, 1], [11, 3, 20, 1], [11, 1, 7, 1], [11, 2, 12, 1], [11, 3, 20, 1], [11, 1, 7, 1], [3, 16, 2, 3], [3, 18, 2, 17], [18, 8, 16, 1], [18, 4, 5, 1], [18, 3, 12, 1], [18, 3, 17, 3], [18, 10, 17, 10], [18, 9, 13, 1], [5, 1, 18, 4], [18, 1, 7, 1], [5, 1, 23, 5], [23, 1, 7, 1], [5, 1, 3, 8], [3, 1, 7, 1], [5, 1, 18, 4], [18, 1, 7, 1], [5, 1, 3, 4], [3, 1, 7, 1], [5, 1, 23, 5], [23, 1, 7, 1], [7, 1, 18, 1], [12, 1, 18, 3], [16, 1, 18, 8], [18, 4, 17, 4], [18, 3, 17, 3], [18, 10, 17, 10], [17, 4, 3, 4], [17, 3, 3, 16], [18, 1, 7, 1], [18, 8, 16, 1], [18, 9, 13, 1], [18, 4, 5, 1], [4, 1, 5, 5], [18, 3, 12, 1], [18, 1, 7, 1], [18, 8, 16, 1], [18, 4, 5, 1], [4, 1, 5, 5], [18, 3, 12, 1], [11, 2, 12, 1], [7, 1, 11, 1], [3, 16, 12, 1], [7, 1, 18, 1], [16, 1, 18, 8], [18, 4, 5, 1], [22, 1, 7, 1], [22, 11, 4, 1], [4, 1, 5, 5], [3, 3, 7, 1], [3, 15, 20, 1], [3, 14, 15, 1], [3, 12, 0, 1], [18, 1, 7, 1], [18, 3, 12, 1], [18, 8, 16, 1], [18, 3, 12, 1], [18, 1, 7, 1], [18, 7, 4, 1], [3, 16, 12, 1], [3, 1, 7, 1], [3, 7, 4, 1], [23, 4, 12, 1], [23, 1, 7, 1], [23, 8, 4, 1], [18, 1, 7, 1], [3, 1, 7, 1], [18, 4, 5, 1], [18, 1, 7, 1], [18, 3, 12, 1], [18, 8, 16, 1], [18, 5, 6, 1], [7, 1, 18, 1], [12, 1, 18, 3], [16, 1, 18, 8], [5, 1, 18, 4], [18, 1, 7, 1], [5, 1, 23, 5], [23, 1, 7, 1], [23, 3, 7, 1], [23, 16, 20, 1], [23, 15, 15, 1], [23, 14, 24, 1], [23, 1, 7, 1], [3, 1, 7, 1], [18, 1, 7, 1], [11, 1, 7, 1], [11, 2, 12, 1], [17, 1, 7, 1], [18, 10, 17, 10], [18, 3, 17, 3], [18, 4, 17, 4], [18, 8, 16, 1], [18, 3, 12, 1], [18, 1, 7, 1], [18, 8, 16, 1], [7, 1, 18, 1], [18, 3, 12, 1], [18, 4, 5, 1], [5, 5, 4, 1], [18, 8, 16, 1], [18, 1, 7, 1], [18, 3, 12, 1], [18, 5, 6, 1], [18, 9, 13, 1], [7, 1, 18, 1], [16, 1, 18, 8], [7, 1, 18, 1], [16, 1, 18, 8], [18, 1, 7, 1], [18, 3, 12, 1], [23, 1, 7, 1], [23, 4, 12, 1], [3, 1, 7, 1], [3, 16, 12, 1], [12, 1, 23, 4], [7, 1, 23, 1], [23, 4, 12, 1], [7, 1, 23, 1], [18, 3, 12, 1], [18, 1, 7, 1], [18, 8, 16, 1], [5, 1, 18, 4], [18, 1, 7, 1], [5, 1, 23, 5], [23, 1, 7, 1], [5, 1, 3, 8], [3, 1, 7, 1], [5, 5, 4, 1], [4, 8, 16, 24], [4, 9, 16, 25], [18, 1, 7, 1], [5, 1, 18, 4], [23, 5, 5, 1], [5, 5, 4, 1], [23, 4, 12, 1], [23, 1, 7, 1], [3, 18, 2, 17], [3, 16, 2, 3], [12, 1, 3, 16], [3, 15, 20, 1], [3, 1, 7, 1], [18, 8, 16, 1], [18, 8, 16, 1], [18, 8, 16, 1], [18, 8, 16, 1], [18, 8, 16, 1], [18, 8, 16, 1], [18, 8, 16, 1], [18, 8, 16, 1], [18, 4, 5, 1], [23, 1, 7, 1], [18, 1, 7, 1], [22, 14, 23, 18], [23, 4, 22, 3], [23, 1, 7, 1], [2, 17, 3, 18], [3, 16, 2, 3], [3, 1, 7, 1], [17, 10, 18, 10], [18, 3, 17, 3], [18, 1, 7, 1], [23, 4, 18, 3], [3, 16, 18, 3], [12, 1, 3, 16], [7, 1, 3, 1], [3, 18, 2, 17], [3, 16, 2, 3], [12, 1, 18, 3], [7, 1, 18, 1], [18, 10, 17, 10], [18, 3, 17, 3], [12, 1, 23, 4], [7, 1, 23, 1], [23, 18, 22, 14], [23, 4, 22, 3], [18, 4, 5, 1], [18, 10, 17, 10], [18, 3, 17, 3], [18, 4, 5, 1], [18, 3, 12, 1], [18, 8, 16, 1], [5, 5, 4, 1], [16, 26, 4, 10], [18, 10, 17, 10], [18, 3, 17, 3], [18, 4, 5, 1], [18, 3, 12, 1], [18, 8, 16, 1], [5, 5, 4, 1], [16, 26, 4, 10]])
# data = np.array(num_list)

# 创建DBSCAN对象
dbscan = DBSCAN(eps=5, min_samples=3)  # eps表示最大距离，min_samples表示核心点最少包含的样本数

# 对数据进行聚类
labels = dbscan.fit_predict(data)

# 获取不同类别的数据
unique_labels = np.unique(labels)

# 创建颜色映射
colors = plt.cm.rainbow(np.linspace(0, 1, len(unique_labels)))

# 绘制聚类结果
for i, label in enumerate(unique_labels):
    if label == -1:
        # 噪声点用黑色表示
        color = 'black'
    else:
        color = colors[i]
    # 绘制当前类别的数据
    cluster = data[labels == label]
    plt.scatter(cluster[:, 0], cluster[:, 1], c=[color], label=label)

plt.xlabel('Join type')
plt.ylabel('Count of join')
plt.title('DBSCAN Clustering')
plt.legend()
plt.show()
