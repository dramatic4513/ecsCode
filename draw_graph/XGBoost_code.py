# import pandas as pd
# import numpy as np
# from xgboost import XGBRegressor
# from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
# import matplotlib.pyplot as plt
#
# # 读取数据
# file_path = '/home/postgres/code/data/ia-pv-2006/HA4_41.65_-93.45_2006_DPV_32MW_60_Min.csv'
# data = pd.read_csv(file_path)
#
# # 准备数据
# data = data.rename(columns={'LocalTime': 'ds', 'Power(MW)': 'y'})
# data['ds'] = pd.to_datetime(data['ds'])
#
# # 将数据分为训练集和测试集
# train_size = int(len(data) * 0.80)
# train_data, test_data = data.iloc[:train_size], data.iloc[train_size:]
#
# # 使用XGBoost模型
# model = XGBRegressor()
# model.fit(np.arange(len(train_data)).reshape(-1, 1), train_data['y'])
# forecast = model.predict(np.arange(len(train_data), len(data)).reshape(-1, 1))
#
# # 计算均方根误差（RMSE）、平均绝对误差（MAE）、均方误差（MSE）和R-squared（R^2）
# rmse = np.sqrt(mean_squared_error(test_data['y'], forecast))
# mae = mean_absolute_error(test_data['y'], forecast)
# mse = mean_squared_error(test_data['y'], forecast)
# r2 = r2_score(test_data['y'], forecast)
# print(f"Root Mean Squared Error (RMSE): {rmse}")
# print(f"Mean Absolute Error (MAE): {mae}")
# print(f"Mean Squared Error (MSE): {mse}")
# print(f"R-squared (R^2): {r2}")
#
# # 绘制预测数据和测试集的折线图
# plt.figure(figsize=(12, 6))
# plt.plot(test_data['ds'], test_data['y'], label='Test')
# plt.plot(test_data['ds'], forecast, label='Predicted')
# plt.xlabel('LocalTime')
# plt.ylabel('Power (MW)')
# plt.title('Power Prediction using XGBoost')
# plt.legend()
# plt.show()


import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import matplotlib.pyplot as plt

# 读取数据
file_path = '/home/postgres/code/data/ia-pv-2006/HA4_41.65_-93.45_2006_DPV_32MW_60_Min.csv'
data = pd.read_csv(file_path)

# 准备数据
data = data.rename(columns={'LocalTime': 'ds', 'Power(MW)': 'y'})
data['ds'] = pd.to_datetime(data['ds'])

# 将数据分为训练集和测试集
train_size = int(len(data) * 0.80)
train_data, test_data = data.iloc[:train_size], data.iloc[train_size:]

# 定义XGBoost模型
model = XGBRegressor()

# 定义参数网格
param_grid = {
    'n_estimators': [50, 100, 150],
    'max_depth': [3, 6, 9],
    'learning_rate': [0.1, 0.01, 0.001]
}

# 使用时间序列交叉验证进行参数调优
tscv = TimeSeriesSplit(n_splits=3)
grid_search = GridSearchCV(estimator=model, param_grid=param_grid, cv=tscv, scoring='neg_mean_squared_error')
grid_search.fit(np.arange(len(train_data)).reshape(-1, 1), train_data['y'])

# 输出最佳参数
print("Best parameters found: ", grid_search.best_params_)

# 使用最佳参数重新训练模型
best_model = grid_search.best_estimator_
best_model.fit(np.arange(len(train_data)).reshape(-1, 1), train_data['y'])
forecast = best_model.predict(np.arange(len(train_data), len(data)).reshape(-1, 1))

# 计算均方根误差（RMSE）、平均绝对误差（MAE）、均方误差（MSE）和R-squared（R^2）
rmse = np.sqrt(mean_squared_error(test_data['y'], forecast))
mae = mean_absolute_error(test_data['y'], forecast)
mse = mean_squared_error(test_data['y'], forecast)
r2 = r2_score(test_data['y'], forecast)
print(f"Root Mean Squared Error (RMSE): {rmse}")
print(f"Mean Absolute Error (MAE): {mae}")
print(f"Mean Squared Error (MSE): {mse}")

# 绘制预测数据和测试集的折线图
plt.figure(figsize=(12, 6))
plt.plot(test_data['ds'], test_data['y'], label='Test')
plt.plot(test_data['ds'], forecast, label='Predicted')
plt.xlabel('LocalTime')
plt.ylabel('Power (MW)')
plt.title('Power Prediction using XGBoost')
plt.legend()
plt.show()