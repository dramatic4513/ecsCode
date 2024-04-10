import pandas as pd
import numpy as np
from prophet import Prophet
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import matplotlib.pyplot as plt
# 读取三个文件的数据
file_path1 = '/home/postgres/code/data/ia-pv-2006/HA4_41.65_-93.45_2006_DPV_32MW_60_Min.csv'
file_path2 = '/home/postgres/code/data/ia-pv-2006/HA4_41.65_-93.55_2006_DPV_32MW_60_Min.csv'
file_path3 = '/home/postgres/code/data/ia-pv-2006/HA4_41.65_-93.65_2006_DPV_32MW_60_Min.csv'
data1 = pd.read_csv(file_path1)
data2 = pd.read_csv(file_path2)
data3 = pd.read_csv(file_path3)

# 合并三个数据集
data = pd.concat([data1, data2, data3], ignore_index=True)

# 准备数据为Prophet所需的格式
data = data.rename(columns={'LocalTime': 'ds', 'Power(MW)': 'y'})
data['ds'] = pd.to_datetime(data['ds'])

# 将数据分为训练集和测试集
train_size = int(len(data) * 0.80)
train_data, test_data = data.iloc[:train_size], data.iloc[train_size:]

# 创建Prophet模型并拟合数据
model = Prophet()
model.fit(train_data)

# 创建未来日期的数据框架并进行预测
future = model.make_future_dataframe(periods=len(test_data), freq='60T')
forecast = model.predict(future)

# 提取预测结果中的测试集部分
predicted_data = forecast[['ds', 'yhat']].iloc[train_size:]

# 计算均方根误差（RMSE）和平均绝对误差（MAE）
rmse = np.sqrt(mean_squared_error(test_data['y'], predicted_data['yhat']))
mae = mean_absolute_error(test_data['y'], predicted_data['yhat'])
mse = mean_squared_error(test_data['y'], predicted_data['yhat'])
r2 = r2_score(test_data['y'], predicted_data['yhat'])
print(f"Root Mean Squared Error (RMSE): {rmse}")
print(f"Mean Absolute Error (MAE): {mae}")
print(f"Mean Squared Error (MSE): {mse}")
print(f"R-squared (R^2): {r2}")

# 绘制预测数据和测试集的折线图
plt.figure(figsize=(12, 6))
plt.plot(test_data['ds'], test_data['y'], label='Test')
plt.plot(predicted_data['ds'], predicted_data['yhat'], label='Predicted')
plt.xlabel('LocalTime')
plt.ylabel('Power (MW)')
plt.title('Power Prediction using Prophet')
plt.legend()
plt.show()
