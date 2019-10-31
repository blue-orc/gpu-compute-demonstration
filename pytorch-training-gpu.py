import torch
import numpy as np
from torch.autograd import Variable
from torch.nn import functional as F
import cx_Oracle


def selectDischargeCyclesByBatteryName(db, batteryName):
    cur = db.cursor()
    statement = """
        SELECT 
            battery_discharge_data.current_load,
            battery_discharge_data.current_measured,
            battery_discharge_data.temperature_measured,
            battery_discharge_data.voltage_load,
            battery_discharge_data.voltage_measured,
            battery_discharge_data.m_time,
            battery_discharge_data.m_capacity,
            battery_cycle.pct_rul
        FROM battery_battery 
        LEFT JOIN battery_cycle ON battery_battery.battery_id = battery_cycle.battery_id
        LEFT JOIN battery_discharge_data ON battery_discharge_data.cycle_id = battery_cycle.cycle_id
        WHERE
            m_name = :batteryName
    """
    cur.execute(statement, 
        batteryName = batteryName)
    res = cur.fetchall()
    if len(res) == 0:
        return []

    npRes = np.array(res).astype(np.float32)
    #this line clears out nan values, need to fix in DB
    npRes =  npRes[~np.isnan(npRes).any(axis=1)]
    x_data = npRes[:, :7].astype(np.float32)
    y_data = npRes[:,7].astype(np.float32)
    return x_data, y_data

db = cx_Oracle.connect(user="ADMIN", password="Oracle12345!", dsn="burlmigration_high")
print("Connected to Oracle ADW")

x_data, y_data = selectDischargeCyclesByBatteryName(db, 'B0005')
x_norm = x_data / x_data.max(axis=0)
y_norm = y_data / y_data.max(axis=0)
x_test = x_norm[32988]
y_test = y_norm[32988]
print('ok')

print("Loading data and model on to GPU")
device = torch.device("cuda:0")

class LinearRegression(torch.nn.Module):

    def __init__(self, input_dim, output_dim):

        super(LinearRegression, self).__init__() 
        # Calling Super Class's constructor
        self.linear = torch.nn.Linear(input_dim, output_dim)
        # nn.linear is defined in nn.Module

    def forward(self, x):
        # Here the forward pass is simply a linear function

        out = torch.sigmoid(self.linear(x))
        return out

input_dim = 7
output_dim = 1

x_tensor = torch.Tensor(x_norm).to(device)
y_tensor = torch.Tensor(y_norm).to(device)
y_ok = y_tensor.unsqueeze(1)
x_test_tensor = torch.Tensor(x_test)

model = LinearRegression(input_dim,output_dim)
model = torch.nn.DataParallel(model)
model.to(device)
criterion = torch.nn.MSELoss()# Mean Squared Loss
l_rate = 0.5
optimizer = torch.optim.SGD(model.parameters(), lr = l_rate) #Stochastic Gradient Descent

epochs = 5000
for epoch in range(epochs):
    pctComplete = epoch / epochs * 100
    print ("{:.2f}".format(pctComplete)+"%", end="\r")
    model.train()
    optimizer.zero_grad()
    y_pred = model(x_tensor)
    loss = criterion(y_pred, y_ok)
    loss.backward()
    optimizer.step()
    print(loss.item())

predicted = model(x_test_tensor)
print(predicted.item())
print('fin')