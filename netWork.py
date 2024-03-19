import os
def net_work_bit(path):
#统计文件中超过10000的字节数
#文件的前7行为固定格式没有用
    # path = '/home/postgres/tpc/res/net/tpch5before.txt'
    i = 1
    total_bit = 0
    with open(path, 'r') as f:
        for line in f:
            # print(str(i) + " --- " + line)
            if i > 8:
                words = line.split(',')
                recv = float(words[0])
                send = float(words[1])
                if recv > 10000 and send > 10000:
                    total_bit += recv
                    total_bit += send
            i = i + 1
        f.close()
    print("total_bit  ", total_bit)
    return total_bit

if __name__ == '__main__':
    folder_path = '/home/postgres/tpc/tpch/res'
    file_list = os.listdir(folder_path)
    for file_name in file_list:
        if file_name.startswith('new_net_') and os.path.isfile(os.path.join(folder_path, file_name)):
            print(os.path.join(folder_path, file_name))
            net_work_bit(os.path.join(folder_path, file_name))
