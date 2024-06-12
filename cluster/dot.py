import matplotlib.pyplot as plt

join_id_frequency_dict = {0: 2, 1: 1, 2: 1, 3: 2, 4: 1, 5: 4, 11: 10, 12: 8, 13: 9, 14: 2, 15: 2, 16: 2, 17: 2, 18: 2, 19: 2, 20: 2}
    # {89: 3, 65: 2, 90: 3, 91: 2, 92: 2, 94: 2, 95: 2, 96: 2, 74: 1}
# {0: 2, 1: 1, 2: 1, 3: 2, 4: 1, 5: 4, 11: 10, 12: 8, 13: 9, 14: 2, 15: 2, 16: 2, 17: 2, 18: 2, 19: 2, 20: 2}
keys = list(join_id_frequency_dict.keys())
values = list(join_id_frequency_dict.values())

# plt.figure(figsize=(8,6))
plt.figure()
plt.scatter(keys, values, color='blue')

plt.xlim(0, 100)
plt.ylim(0, 12)

# 添加标题和标签
# plt.title()
plt.xlabel('Join type')
plt.ylabel('Count of join')

# 显示图形
plt.show()
