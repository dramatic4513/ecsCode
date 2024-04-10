#快速傅里叶变换
import numpy as np
import matplotlib.pyplot as plt

# 生成正弦波信号的时域数据
fs = 1000  # 采样频率
t = np.arange(0, 1, 1/fs)  # 时间向量，从0到1秒
f = 10  # 正弦波频率为10Hz
x = np.sin(2*np.pi*f*t)  # 正弦波信号

# 绘制时域波形
plt.figure()
plt.subplot(2, 1, 1)
plt.plot(t, x)
plt.title('time domain signal')  #时域信号
plt.xlabel('time (s)')
plt.ylabel('amplitude') #幅度

# 应用傅里叶变换，将时域信号转换为频域信号
X = np.fft.fft(x)  # 进行傅里叶变换
freqs = np.fft.fftfreq(len(x), 1/fs)  # 计算频率轴

# 绘制频域波形
plt.subplot(2, 1, 2)
plt.plot(freqs[:len(freqs)//2], np.abs(X)[:len(freqs)//2])  # 只绘制正频率部分
plt.title('frequency domain signal') #频域信号
plt.xlabel('frequency (Hz)')
plt.ylabel('amplitude')

plt.tight_layout()

plt.show()
