import subprocess
import datetime
import time
# str = 'dstat -nt --output /home/postgres/tpc/res/net/' + str(time.time()) + '.txt'
str_sql = 'psql -h 172.23.52.199 -p 20004 -d tpch100m -U postgres -c "ALTER TABLE CUSTOMER DISTRIBUTE BY HASH(C_CUSTKEY);"'
print(str_sql)
command = str_sql
result = subprocess.run(command, shell=True, capture_output=True, text=True)

print(result.stdout)