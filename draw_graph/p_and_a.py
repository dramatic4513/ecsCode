import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
from prophet import Prophet
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import matplotlib.pyplot as plt

# 模型一
file_path = '/home/postgres/code/data/ia-pv-2006/HA4_41.65_-93.45_2006_DPV_32MW_60_Min.csv'
data = pd.read_csv(file_path)
data = data.rename(columns={'LocalTime': 'ds', 'Power(MW)': 'y'})
data['ds'] = pd.to_datetime(data['ds'])
train_size = int(len(data) * 0.80)
train_data, test_data = data.iloc[:train_size], data.iloc[train_size:]
d = 1

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

forecast_arima = best_model.forecast(steps=len(test_data))

# 模型二
data = pd.read_csv(file_path)
data = data.rename(columns={'LocalTime': 'ds', 'Power(MW)': 'y'})
data['ds'] = pd.to_datetime(data['ds'])
train_size = int(len(data) * 0.80)
train_data, test_data = data.iloc[:train_size], data.iloc[train_size:]

model = Prophet()
model.fit(train_data)
future = model.make_future_dataframe(periods=len(test_data), freq='60T')
forecast_prophet = model.predict(future)
predicted_data_prophet = forecast_prophet[['ds', 'yhat']].iloc[train_size:]

# 合并预测结果并取平均值
forecast_combined = (forecast_arima.values/2) + predicted_data_prophet['yhat'].values

# 计算均方根误差（RMSE）和平均绝对误差（MAE）
rmse = np.sqrt(mean_squared_error(test_data['y'], forecast_combined))
mae = mean_absolute_error(test_data['y'], forecast_combined)
mse = mean_squared_error(test_data['y'], forecast_combined)
r2 = r2_score(test_data['y'], forecast_combined)
print(f"Root Mean Squared Error (RMSE): {rmse}")
print(f"Mean Absolute Error (MAE): {mae}")
print(f"Mean Squared Error (MSE): {mse}")
print(f"R-squared (R^2): {r2}")

# 绘制预测数据和测试集的折线图
plt.figure(figsize=(12, 6))
plt.plot(test_data['ds'], test_data['y'], label='Test')
plt.plot(test_data['ds'], forecast_combined, label='Predicted')
plt.xlabel('LocalTime')
plt.ylabel('Power (MW)')
plt.title('Power Prediction using Combined Models (Average)')
plt.legend()
plt.show()
