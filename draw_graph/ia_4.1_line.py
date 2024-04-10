import matplotlib.pyplot as plt
import csv

'''
Actual : 实际功率输出
DA: 日前预报
HA4: 提前4小时预报
光伏类型
UPV: 公用事业规模光伏
DPV: 分布式光伏
'''
# # 文件路径
file_paths = [
    '/home/postgres/code/data/ia-pv-2006/DA_40.95_-94.35_2006_UPV_10MW_60_Min.csv',
    # '/home/postgres/code/data/ia-pv-2006/DA_41.05_-94.65_2006_UPV_5MW_60_Min.csv',
    # '/home/postgres/code/data/ia-pv-2006/DA_41.15_-94.85_2006_UPV_38MW_60_Min.csv',
    # '/home/postgres/code/data/ia-pv-2006/DA_41.55_-93.55_2006_DPV_32MW_60_Min.csv',
    # '/home/postgres/code/data/ia-pv-2006/DA_41.65_-90.65_2006_DPV_31MW_60_Min.csv'
    # '/home/postgres/code/data/ia-pv-2006/Actual_40.95_-94.35_2006_UPV_10MW_5_Min.csv',
    # '/home/postgres/code/data/ia-pv-2006/Actual_41.05_-94.65_2006_UPV_5MW_5_Min.csv',
    '/home/postgres/code/data/ia-pv-2006/DA_41.15_-94.85_2006_UPV_38MW_60_Min.csv',
    # '/home/postgres/code/data/ia-pv-2006/DA_41.55_-93.55_2006_DPV_32MW_60_Min.csv',
    # '/home/postgres/code/data/ia-pv-2006/DA_41.65_-90.65_2006_DPV_31MW_60_Min.csv'
]

# 存储从每个文件中提取的数据
powers = []

# 读取并提取数据
for file_path in file_paths:
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # 跳过标题行
        data = [float(row[1]) for row in reader]
        if file_path == '/home/postgres/code/data/ia-pv-2006/DA_41.15_-94.85_2006_UPV_38MW_60_Min.csv' :
            powers.append(data[10:510])
        else:
            powers.append(data[:500])

# 绘制折线图
plt.figure(figsize=(10, 6))
for i, power in enumerate(powers, start=1):
    plt.plot(power, label=f'type {i}')

plt.xlabel('Time', fontsize = 14)
plt.ylabel('Count of SQL', fontsize = 14)
# plt.title('Power Consumption Over Time')
plt.xticks([0, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500 ], ['0', '50', '100', '150', '200', '250', '300', '350', '400', '450', '500'])
plt.tight_layout()
plt.legend(fontsize = 14)
plt.show()

# import matplotlib.pyplot as plt
# import numpy as np
# import csv
#
# # 文件路径
# file_paths = [
#     '/home/postgres/code/data/ia-pv-2006/Actual_40.95_-94.35_2006_UPV_10MW_5_Min.csv',
#     '/home/postgres/code/data/ia-pv-2006/Actual_41.05_-94.65_2006_UPV_5MW_5_Min.csv',
#     '/home/postgres/code/data/ia-pv-2006/DA_41.15_-94.85_2006_UPV_38MW_60_Min.csv',
# ]
#
# # 存储从每个文件中提取的数据
# powers = []
#
# # 读取并提取数据
# for file_path in file_paths:
#     with open(file_path, 'r') as file:
#         reader = csv.reader(file)
#         next(reader)  # 跳过标题行
#         data = [float(row[1]) for row in reader]
#         powers.append(data[:500])
#
# # 绘制折线图
# plt.figure(figsize=(10, 6))
# for i, power in enumerate(powers, start=1):
#     plt.plot(power, label=f'join {i}')
#
# plt.xlabel('Time/hour', fontsize = 14)
# plt.ylabel('Count of join', fontsize = 14)
# plt.xticks([0, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500], ['0', '50', '100', '150', '200', '250', '300', '350', '400', '450', '500'])
# plt.tight_layout()
# plt.legend(fontsize = 14)
# plt.show()
#
# # 傅里叶变换
# plt.figure(figsize=(10, 6))
# for i, power in enumerate(powers, start=1):
#     fft_vals = np.fft.fft(power)
#     fft_freq = np.fft.fftfreq(len(power))
#     plt.plot(fft_freq[:len(fft_freq)//2], np.abs(fft_vals)[:len(fft_freq)//2], label=f'join {i}')
#
# plt.xlabel('Frequency', fontsize=14)
# plt.ylabel('Magnitude', fontsize=14)
# plt.title('Fourier Transform', fontsize=14)
# plt.legend(fontsize=14)
# plt.show()
