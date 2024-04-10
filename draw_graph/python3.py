import pandas as pd
import numpy as np
from arch import arch_model
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import matplotlib.pyplot as plt


file_path = '/home/postgres/code/data/ia-pv-2006/HA4_41.65_-93.45_2006_DPV_32MW_60_Min.csv'
data = pd.read_csv(file_path)


data = data.rename(columns={'LocalTime': 'ds', 'Power(MW)': 'y'})
data['ds'] = pd.to_datetime(data['ds'])


train_size = int(len(data) * 0.80)
train_data, test_data = data.iloc[:train_size], data.iloc[train_size:]


model = arch_model(train_data['y'], vol='Garch', p=1, q=1, dist='Normal')
model_fit = model.fit()


forecast = model_fit.forecast(start=len(train_data), horizon=len(test_data))


rmse = np.sqrt(mean_squared_error(test_data['y'], forecast.mean['h.1']))
mae = mean_absolute_error(test_data['y'], forecast.mean['h.1'])
mse = mean_squared_error(test_data['y'], forecast.mean['h.1'])
r2 = r2_score(test_data['y'], forecast.mean['h.1'])
print(f"Root Mean Squared Error (RMSE): {rmse}")
print(f"Mean Absolute Error (MAE): {mae}")
print(f"Mean Squared Error (MSE): {mse}")
print(f"R-squared (R^2): {r2}")


plt.figure(figsize=(12, 6))
plt.plot(test_data['ds'], test_data['y'], label='Test')
plt.plot(test_data['ds'], forecast.mean['h.1'], label='Predicted')
plt.xlabel('LocalTime')
plt.ylabel('Power (MW)')
plt.title('Power Prediction using GARCH')
plt.legend()
plt.show()