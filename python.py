import scipy
import scipy.io as sio
import pandas as pd
import csv

def writeToCsv(filename, rowData):
    with open(filename, mode='a') as csvFile:
        csv_writer = csv.writer(csvFile, delimiter=',', quotechar='"', quoting = csv.QUOTE_MINIMAL)
        csv_writer.writerow(rowData)

mill = sio.loadmat('B0018.mat', squeeze_me=True)
Mill = mill['B0018']
MillDF = pd.DataFrame(Mill, index=[0])
print(list(MillDF))
d1 = pd.DataFrame(MillDF['cycle'][0])

chargeCycle = 0
dischargeCycle = 0
for index, row in d1.iterrows():
    out = []
    out.append(row['type'])
    out.append(str(row['ambient_temperature']))
    data = pd.DataFrame(row.loc['data'], index=[0])
    print(str(index))
    for index2, row2 in data.iterrows():
        pd.to_numeric(row2, errors='coerce').fillna(0)
        if (row['type'] == 'charge'):
            for i in range(len(row2['Voltage_measured'])):
                out2 = []
                out2.append(row['type'])
                out2.append(str(row['ambient_temperature']))
                out2.append(str(chargeCycle))
                out2.append(row2['Voltage_measured'][i])
                out2.append(row2['Current_measured'][i]) 
                out2.append(row2['Temperature_measured'][i])
                out2.append(row2['Current_charge'][i])
                out2.append(row2['Voltage_charge'][i])
                out2.append(row2['Time'][i])
                writeToCsv("charge.csv", out2)
            chargeCycle = chargeCycle + 1
        elif (row['type'] == 'discharge'):
            for i in range(len(row2['Current_load'])):
                out2 = []
                out2.append(row['type'])
                out2.append(str(row['ambient_temperature']))
                out2.append(str(dischargeCycle))
                out2.append(row2['Current_load'][i])
                out2.append(row2['Current_measured'][i])
                out2.append(row2['Temperature_measured'][i])
                out2.append(row2['Voltage_load'][i])
                out2.append(row2['Voltage_measured'][i])
                out2.append(row2['Time'][i])
                out2.append(row2['Capacity'])
                writeToCsv("discharge.csv", out2)
            dischargeCycle = dischargeCycle + 1
        else:
            print(row['type'])

