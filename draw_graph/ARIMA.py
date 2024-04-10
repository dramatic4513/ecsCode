import pandas as pd
import numpy as np
import prophet
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import matplotlib.pyplot as plt

# 读取数据
# file_path = '/home/postgres/code/data/ia-pv-2006/DA_41.15_-94.85_2006_UPV_38MW_60_Min.csv'
# file_path = '/home/postgres/code/data/ia-pv-2006/Actual_40.95_-94.35_2006_UPV_10MW_5_Min.csv'
file_path = '/home/postgres/code/data/ia-pv-2006/HA4_41.65_-93.45_2006_DPV_32MW_60_Min.csv'
data = pd.read_csv(file_path)

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
# 计算均方根误差（RMSE）、平均绝对误差（MAE）、均方误差（MSE）和R-squared（R^2）
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


# import pandas as pd
# import numpy as np
# from prophet import Prophet
# import matplotlib.pyplot as plt
#
# # 读取数据
# file_path = '/home/postgres/code/data/ia-pv-2006/DA_41.15_-94.85_2006_UPV_38MW_60_Min.csv'
# data = pd.read_csv(file_path)
#
# # 准备数据为Prophet所需的格式
# data = data.rename(columns={'LocalTime': 'ds', 'Power(MW)': 'y'})
# data['ds'] = pd.to_datetime(data['ds'])
#
# # 将数据分为训练集和测试集
# train_size = int(len(data) * 0.8)
# train_data, test_data = data.iloc[:train_size], data.iloc[train_size:]
#
# # 创建Prophet模型并拟合数据
# model = Prophet()
# model.fit(train_data)
#
# # 创建未来日期的数据框架并进行预测
# future = model.make_future_dataframe(periods=len(test_data), freq='60T')
# forecast = model.predict(future)
#
# # 提取预测结果中的测试集部分
# predicted_data = forecast[['ds', 'yhat']].iloc[train_size:]
#
# # 绘制预测数据和测试集的折线图
# plt.figure(figsize=(12, 6))
# plt.plot(test_data['ds'], test_data['y'], label='Test')
# plt.plot(predicted_data['ds'], predicted_data['yhat'], label='Predicted')
# plt.xlabel('LocalTime')
# plt.ylabel('Power (MW)')
# plt.title('Power Prediction using Prophet')
# plt.legend()
# plt.show()





# import pandas as pd
# import numpy as np
# from sklearn.model_selection import train_test_split
# from sklearn.ensemble import RandomForestRegressor
# import matplotlib.pyplot as plt
#
# # 读取数据
# file_path = '/home/postgres/code/data/ia-pv-2006/DA_41.15_-94.85_2006_UPV_38MW_60_Min.csv'
# data = pd.read_csv(file_path)
#
# # 提取第二列作为预测目标
# y = data.iloc[:, 1].values.reshape(-1, 1)
#
# # 创建特征，这里使用时间序列的索引作为特征
# X = np.arange(len(y)).reshape(-1, 1)
#
# # 将数据分为训练集和测试集
# train_size = int(len(X) * 0.8)
# X_train, X_test, y_train, y_test = X[:train_size], X[train_size:], y[:train_size], y[train_size:]
#
# # 使用随机森林模型进行训练和预测
# # model = RandomForestRegressor()
# model = RandomForestRegressor(n_estimators=100, max_depth=10, min_samples_split=2, min_samples_leaf=1, max_features='sqrt')
# model.fit(X_train, y_train)
# y_pred = model.predict(X_test)
#
# # 计算均方根误差
# rmse = np.sqrt(np.mean((y_test - y_pred) ** 2))
# print(f"Root Mean Squared Error: {rmse}")
#
# # 绘制训练数据、测试数据和预测结果
# plt.figure(figsize=(12, 6))
# plt.plot(X_train, y_train, label='Train')
# # plt.plot(X_test, y_test, label='Test')
# plt.plot(X_test, y_pred, label='Predicted')
# plt.xlabel('Index')
# plt.ylabel('Power (MW)')
# plt.title('Power Prediction using Random Forest')
# plt.legend()
# plt.show()
