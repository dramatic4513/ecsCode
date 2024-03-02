import psycopg2
import os

host = "172.23.52.199"
port = 20004
database = "tpch"
user = "postgres"
password = "postgres"

def connect_to_db():
    #连接数据库
    try:
        connection = psycopg2.connect(host = host, port = port, database = database, user=user)
        #执行sql语句
        cursor = connection.cursor()
        sqls = create_table_sqls()
        sqls = sqls[:-1]
        for sql in sqls:
            sql = sql + ';'
            cursor.execute(sql)
            rows = cursor.fetchall()
            for row in rows:
                print(row)
            print("Success !!!")
        connection.close()

    except psycopg2.Error as e:
        print("连接失败 !!!")
        print(e)



def create_table_sqls():
     file_path = '/home/postgres/tpc/tpch/SQL/createTable.txt'
     file_content = ""
     with open(file_path, 'r') as f:
         for s in f:
             file_content = file_content + s
    #sqls中的sql不带; 所以用到的时候需要添加;
     file_content.strip()
     sqls = file_content.split(';')
     return sqls



if __name__ == '__main__':
    connect_to_db()
