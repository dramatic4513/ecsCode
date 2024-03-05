import subprocess
import datetime
import time
str = 'dstat -nt --output /home/postgres/tpc/res/net/' + str(time.time()) + '.txt'
print(str)
command = str
result = subprocess.run(command, shell=True, capture_output=True, text=True)

print(result.stdout)