import pandas as pd
import numpy as np
from arch import arch_model
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import matplotlib.pyplot as plt


file_path = '/home/postgres/code/data/ia-pv-2006/HA4_41.65_-93.45_2006_DPV_32MW_60_Min.csv'
data = pd.read_csv(file_path)


data = data.rename(columns={'LocalTime': 'ds', 'Power(MW)': 'y'})
data['ds'] = pd.to_datetime(data['ds'])
data = data['y']


train_size = int(len(data) * 0.80)
train_data, test_data = data.iloc[:train_size], data.iloc[train_size:]


model = arch_model(train_data, vol='Garch', p=1, q=1, dist='Normal')
model_fit = model.fit()


forecast = model_fit.forecast(start=len(train_data), horizon=len(test_data))

print(type(forecast))
# 查看forecast的属性
print("Forecast properties:")
print("Mean:")
print(forecast.mean)
print("Variance:")
print(forecast.variance)
print("Standard deviation:")
print(forecast.stddev)
print("Residual variance:")
print(forecast.residual_variance)

# 查看forecast的值
print("Forecast values:")
print("Mean values:")
print(forecast.mean['h.1'].values)
print("Variance values:")
print(forecast.variance['h.1'].values)
print("Standard deviation values:")
print(forecast.stddev['h.1'].values)
print("Residual variance values:")
print(forecast.residual_variance.values)


# rmse = np.sqrt(mean_squared_error(test_data, forecast.mean['h.1']))
# mae = mean_absolute_error(test_data, forecast.mean['h.1'])
# mse = mean_squared_error(test_data, forecast.mean['h.1'])
# r2 = r2_score(test_data, forecast.mean['h.1'])
# print('RMSE: ', rmse)
# print('MAE: ', mae)
# print('MSE: ', mse)
# print('R2: ', r2)


# plt.figure(figsize=(12, 6))
# plt.plot(test_data.index, test_data, label='Test')
# plt.plot(test_data.index, forecast.mean['h.1'], label='Predicted')
# plt.xlabel('Index')
# plt.ylabel('Power (MW)')
# plt.title('Power Prediction using GARCH')
# plt.legend()
# plt.show()
