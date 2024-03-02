import os

#生成将数据导入到tpcds中的命令，在数据库中执行生成的命令
# \copy web_site from '/home/postgres/tpcds/1G/handled/web_site.dat' with delimiter as '|' NULL '';

root_path = '/home/postgres/tpc/tpcds/data/1G/handled/'
for filename in os.listdir(root_path):
    filenames = filename.split('.')[:-1]
    pre_filename = filenames[0]
    str = '\copy ' + pre_filename + ' from ' + '\'' + root_path + filename + '\'' +  ' with delimiter as \'|\' NULL \'\';'
    print(str)
