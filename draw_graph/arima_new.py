import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import matplotlib.pyplot as plt

# 读取数据
file_path = '/home/postgres/code/data/ia-pv-2006/HA4_41.65_-93.45_2006_DPV_32MW_60_Min.csv'
data = pd.read_csv(file_path)

# 准备数据为ARIMA所需的格式
data = data.rename(columns={'LocalTime': 'ds', 'Power(MW)': 'y'})
data['ds'] = pd.to_datetime(data['ds'])

# 将数据分为训练集和测试集
train_size = int(len(data) * 0.80)
train_data, test_data = data.iloc[:train_size], data.iloc[train_size:]

# 自动确定差分阶数
d = 1  # 可以根据实际情况调整

# 通过循环迭代找到最佳的ARIMA模型参数
best_rmse = float('inf')
best_model = None
for p in range(5):
    for q in range(5):
        try:
            model = ARIMA(train_data['y'], order=(p, d, q))
            model_fit = model.fit()
            forecast = model_fit.forecast(steps=len(test_data))
            rmse = np.sqrt(mean_squared_error(test_data['y'], forecast))
            if rmse < best_rmse:
                best_rmse = rmse
                best_model = model_fit
        except:
            pass

# 使用最佳模型进行预测
forecast = best_model.forecast(steps=len(test_data))

# 计算均方根误差（RMSE）和平均绝对误差（MAE）
rmse = np.sqrt(mean_squared_error(test_data['y'], forecast))
mae = mean_absolute_error(test_data['y'], forecast)
mse = mean_squared_error(test_data['y'], forecast)
r2 = r2_score(test_data['y'], forecast)
print(f"Root Mean Squared Error (RMSE): {rmse}")
print(f"Mean Absolute Error (MAE): {mae}")
print(f"Mean Squared Error (MSE): {mse}")
print(f"R-squared (R^2): {r2}")

# 绘制预测数据和测试集的折线图
plt.figure(figsize=(12, 6))
plt.plot(test_data['ds'], test_data['y'], label='Test')
plt.plot(test_data['ds'], forecast, label='Predicted')
plt.xlabel('LocalTime')
plt.ylabel('Power (MW)')
plt.title('Power Prediction using ARIMA')
plt.legend()
plt.show()
