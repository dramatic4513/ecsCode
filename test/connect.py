import psycopg2

host = "172.23.52.199"
port = 20004
database = "tpch"
user = "postgres"
password = "postgres"

try:
    connection = psycopg2.connect(host = host, port = port, database = database, user=user)
    print("Connection established")
    cursor = connection.cursor()
    sql = "SELECT count(*) FROM nation"
    # cursor.execute("CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL, N_NAME       CHAR(25) NOT NULL, N_REGIONKEY  INTEGER NOT NULL, N_COMMENT    VARCHAR(152)) distribute by hash(N_NATIONKEY); ")
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    connection.close()

except psycopg2.Error as e:
    print("Error !!!")
    connection.close()

finally:
    if connection:
        connection.close()
        print("PostgreSQL XL connection closed.")