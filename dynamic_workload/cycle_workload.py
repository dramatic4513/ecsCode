import matplotlib.pyplot as plt
import csv
import numpy as np
import psycopg2
import os
import time
from datetime import datetime

host = "172.23.52.199"
port = 20004
database = "tpcds"
user = "postgres"
password = "postgres"

#文件地址：
#sql 地址
# 通过自相关函数设置阈值的方式判断周期性
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

def len_cycle(file_paths):
    powers = []

    # 读取并提取数据
    for file_path in file_paths:
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # 跳过标题行
            data = [float(row[1]) for row in reader]
            if file_path == '/home/postgres/code/data/ia-pv-2006/DA_41.15_-94.85_2006_UPV_38MW_60_Min.csv':
                powers.append(data[10:26])
            else:
                powers.append(data[:24])

    power = [x + 10 for x in powers[1]]

    # 将列表转换为numpy数组
    power = np.array(power)

    # 计算傅里叶变换
    fft_result = np.fft.fft(power)
    fft_freqs = np.fft.fftfreq(len(power))

    # 计算功率谱密度
    power_spectrum = np.abs(fft_result) ** 2

    # 找到主要频率成分的位置
    main_freq_index = np.argmax(power_spectrum)
    main_freq = fft_freqs[main_freq_index]
    main_freq = 1/len(power)

    # 计算周期长度
    period_length = int(1 / np.abs(main_freq))
    print(period_length)


def autocorrelation(file_paths):
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


    power = [x + 10 for x in powers[0]]
    # 将列表转换为numpy数组
    power = np.array(power)

    # 确保它是一维数组
    if power.ndim != 1:
        raise ValueError("power 应该是一维数组")

    # 计算自相关值
    n = len(power)
    mean = np.mean(power)
    variance = np.var(power)
    autocorr = np.correlate(power - mean, power - mean, mode='full')[n - 1:] / (variance * (np.arange(n, 0, -1)))

    # mean_autocorr = np.mean(np.abs(autocorr))
    mean_autocorr = np.max(np.abs(autocorr))
    # 打印自相关值
    # print(f"Autocorrelation values for the data sequence:", mean_autocorr)
    print(f"Autocorrelation values for the data sequence:", autocorr)
    if mean_autocorr > 0.7 or mean_autocorr < -0.7 :
        # print(mean_autocorr)
        print("True")

def run_cycle() :
    data = []
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    root_path = '/home/postgres/tpc/tpcds/queries'
    i = 0
    while i < 8:
        i = i + 1
        for filename in os.listdir(root_path):
            if filename not in ['query65.sql']:
                continue
            print(filename)
            query_path = os.path.join(root_path, filename)
            sql = ""
            with open(query_path) as f:
                for line in f:
                    sql += line
            cursor = connection.cursor()
            start_time = time.time()
            cursor.execute(sql)
            end_time = time.time()
            elapsed_time = (end_time - start_time) * 1000  # SQL语句执行时间 毫秒
            data.append(elapsed_time)
            output = str(datetime.now()) + " " + filename + " " + str(elapsed_time) + "\n"
            print(output)
            cursor.close()

    for filename in os.listdir(root_path):
        if filename not in ['query27.sql']:
            continue
        print(filename)
        query_path = os.path.join(root_path, filename)
        sql = ""
        with open(query_path) as f:
            for line in f:
                sql += line
        cursor = connection.cursor()
        start_time = time.time()
        cursor.execute(sql)
        end_time = time.time()
        elapsed_time = (end_time - start_time) * 1000  # SQL语句执行时间 毫秒
        data.append(elapsed_time)
        output = str(datetime.now()) + " " + filename + " " + str(elapsed_time) + "\n"
        print(output)
        cursor.close()

    for filename in os.listdir(root_path):
        if filename not in ['query84.sql']:
            continue
        print(filename)
        query_path = os.path.join(root_path, filename)
        sql = ""
        with open(query_path) as f:
            for line in f:
                sql += line
        cursor = connection.cursor()
        start_time = time.time()
        cursor.execute(sql)
        end_time = time.time()
        elapsed_time = (end_time - start_time) * 1000  # SQL语句执行时间 毫秒
        data.append(elapsed_time)
        output = str(datetime.now()) + " " + filename + " " + str(elapsed_time) + "\n"
        print(output)
        cursor.close()

    i = 0
    while i < 3:
        i = i + 1
        for filename in os.listdir(root_path):
            if filename not in ['query96.sql']:
                continue
            print(filename)
            query_path = os.path.join(root_path, filename)
            sql = ""
            with open(query_path) as f:
                for line in f:
                    sql += line
            cursor = connection.cursor()
            start_time = time.time()
            cursor.execute(sql)
            end_time = time.time()
            elapsed_time = (end_time - start_time) * 1000  # SQL语句执行时间 毫秒
            data.append(elapsed_time)
            output = str(datetime.now()) + " " + filename + " " + str(elapsed_time) + "\n"
            cursor.close()
            print(output)


    for filename in os.listdir(root_path):
        if filename not in ['query41.sql']:
            continue
        print(filename)
        query_path = os.path.join(root_path, filename)
        sql = ""
        with open(query_path) as f:
            for line in f:
                sql += line
        cursor = connection.cursor()
        start_time = time.time()
        cursor.execute(sql)
        end_time = time.time()
        elapsed_time = (end_time - start_time) * 1000  # SQL语句执行时间 毫秒
        data.append(elapsed_time)
        output = str(datetime.now()) + " " + filename + " " + str(elapsed_time) + "\n"
        cursor.close()
        print(output)

    for filename in os.listdir(root_path):
        if filename not in ['query45.sql']:
            continue
        print(filename)
        query_path = os.path.join(root_path, filename)
        sql = ""
        with open(query_path) as f:
            for line in f:
                sql += line
        cursor = connection.cursor()
        start_time = time.time()
        cursor.execute(sql)
        end_time = time.time()
        elapsed_time = (end_time - start_time) * 1000  # SQL语句执行时间 毫秒
        data.append(elapsed_time)
        output = str(datetime.now()) + " " + filename + " " + str(elapsed_time) + "\n"
        cursor.close()
        print(output)
    connection.close()

    for d in data:
        print(d)

    x1 = range(1, len(data) + 1)
    plt.plot(x1, data)
    plt.show()
    return data



def new_run_cycle():
    data = run_cycle()
    connection = psycopg2.connect(host=host, port=port, database="tpcdscycle65", user=user)
    root_path = '/home/postgres/tpc/tpcds/queries'
    i = 0
    while i < 8:
        i = i + 1
        for filename in os.listdir(root_path):
            if filename not in ['query65.sql']:
                continue
            print(filename)
            query_path = os.path.join(root_path, filename)
            sql = ""
            with open(query_path) as f:
                for line in f:
                    sql += line
            cursor = connection.cursor()
            start_time = time.time()
            cursor.execute(sql)
            end_time = time.time()
            elapsed_time = (end_time - start_time) * 1000  # SQL语句执行时间 毫秒
            data.append(elapsed_time)
            output = str(datetime.now()) + " " + filename + " " + str(elapsed_time) + "\n"
            print(output)
            cursor.close()
    connection.close()

    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    for filename in os.listdir(root_path):
        if filename not in ['query27.sql']:
            continue
        print(filename)
        query_path = os.path.join(root_path, filename)
        sql = ""
        with open(query_path) as f:
            for line in f:
                sql += line
        cursor = connection.cursor()
        start_time = time.time()
        cursor.execute(sql)
        end_time = time.time()
        elapsed_time = (end_time - start_time) * 1000  # SQL语句执行时间 毫秒
        data.append(elapsed_time)
        output = str(datetime.now()) + " " + filename + " " + str(elapsed_time) + "\n"
        print(output)
        cursor.close()

    for filename in os.listdir(root_path):
        if filename not in ['query84.sql']:
            continue
        print(filename)
        query_path = os.path.join(root_path, filename)
        sql = ""
        with open(query_path) as f:
            for line in f:
                sql += line
        cursor = connection.cursor()
        start_time = time.time()
        cursor.execute(sql)
        end_time = time.time()
        elapsed_time = (end_time - start_time) * 1000  # SQL语句执行时间 毫秒
        data.append(elapsed_time)
        output = str(datetime.now()) + " " + filename + " " + str(elapsed_time) + "\n"
        print(output)
        cursor.close()

    i = 0
    while i < 3:
        i = i + 1
        for filename in os.listdir(root_path):
            if filename not in ['query96.sql']:
                continue
            print(filename)
            query_path = os.path.join(root_path, filename)
            sql = ""
            with open(query_path) as f:
                for line in f:
                    sql += line
            cursor = connection.cursor()
            start_time = time.time()
            cursor.execute(sql)
            end_time = time.time()
            elapsed_time = (end_time - start_time) * 1000  # SQL语句执行时间 毫秒
            data.append(elapsed_time)
            output = str(datetime.now()) + " " + filename + " " + str(elapsed_time) + "\n"
            cursor.close()
            print(output)

    for filename in os.listdir(root_path):
        if filename not in ['query41.sql']:
            continue
        print(filename)
        query_path = os.path.join(root_path, filename)
        sql = ""
        with open(query_path) as f:
            for line in f:
                sql += line
        cursor = connection.cursor()
        start_time = time.time()
        cursor.execute(sql)
        end_time = time.time()
        elapsed_time = (end_time - start_time) * 1000  # SQL语句执行时间 毫秒
        data.append(elapsed_time)
        output = str(datetime.now()) + " " + filename + " " + str(elapsed_time) + "\n"
        cursor.close()
        print(output)

    for filename in os.listdir(root_path):
        if filename not in ['query45.sql']:
            continue
        print(filename)
        query_path = os.path.join(root_path, filename)
        sql = ""
        with open(query_path) as f:
            for line in f:
                sql += line
        cursor = connection.cursor()
        start_time = time.time()
        cursor.execute(sql)
        end_time = time.time()
        elapsed_time = (end_time - start_time) * 1000  # SQL语句执行时间 毫秒
        data.append(elapsed_time)
        output = str(datetime.now()) + " " + filename + " " + str(elapsed_time) + "\n"
        cursor.close()
        print(output)
    connection.close()

    for d in data:
        print(d)

    x1 = range(1, len(data) + 1)
    plt.plot(x1, data)
    plt.show()

def draw_line(file_path):
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

if __name__ == '__main__':
    # draw_line(file_paths)
    # autocorrelation(file_paths)
    # len_cycle(file_paths)
    # run_cycle()
    new_run_cycle()

