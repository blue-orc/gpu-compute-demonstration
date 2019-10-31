import csv
from concurrent.futures import ThreadPoolExecutor
import cx_Oracle

def readCsv(filename):
    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count=0
        data = []
        for row in csv_reader:
            data.append(row)
        return data

def insertBattery(battery_name):
    cur = db.cursor()
    cur.execute("""
            INSERT INTO battery_battery
                (m_name)
            VALUES
                (:name)  
        """,
        name = battery_name
    )
    cur.close()

def selectBatteryID(battery_name):
    statement = "SELECT battery_id FROM battery_battery WHERE m_name = :name"
    cur = db.cursor()
    cur.execute(statement, name=battery_name)
    res = cur.fetchall()
    cur.close()
    if (len(res)==0):
        return 0

    return res[0][0]

class BatteryCycle(object):
    CycleID = 0
    BatteryID = 0
    BatteryCycleNumber = 0
    AmbientTemperature = 0
    CycleType = ""
    PctRUL = 0.0


def insertBatteryCycle(batteryCycle: BatteryCycle):
    cur = db.cursor()
    statement = """
        INSERT INTO battery_cycle (
            battery_id,
            battery_cycle_number,
            ambient_temperature,
            cycle_type,
            pct_rul
        )
        VALUES (
            :batteryID,
            :batteryCycleNumber,
            :ambientTemperature,
            :cycleType,
            :pctRUL
        )
    """
    cur.execute(
        statement, 
        batteryID = batteryCycle.BatteryID, 
        batteryCycleNumber = batteryCycle.BatteryCycleNumber, 
        ambientTemperature = batteryCycle.AmbientTemperature, 
        cycleType = batteryCycle.CycleType,
        pctRUL = batteryCycle.PctRUL
    )
    cur.close()



def selectCycleIDByBatteryIDAndCycleNumberAndType(battery_id, cycle_number, cycle_type):
    statement = """
        SELECT cycle_id FROM battery_cycle 
        WHERE 
            battery_id = :batteryID 
            AND battery_cycle_number = :batteryCycleNumber 
            AND cycle_type = :cycleType
    """
    cur = db.cursor()
    cur.execute(statement, 
        batteryID = battery_id,
        batteryCycleNumber = cycle_number, 
        cycleType = cycle_type)
    res = cur.fetchall()
    cur.close()
    if len(res) == 0:
        return 0
    return res[0][0]

class BatteryDischargeData(object):
    CycleID = 0
    CurrentLoad = 0.0
    CurrentMeasured = 0.0
    TemperatureMeasured = 0.0
    VoltageLoad = 0.0
    VoltageMeasured = 0.0
    Time = 0.0
    Capacity = 0.0

def insertBatteryDischargeData(dischargeData: BatteryDischargeData):
    statement = """
        INSERT INTO battery_discharge_data (
            cycle_id,
            current_load,
            current_measured,
            temperature_measured,
            voltage_load,
            voltage_measured,
            m_time,
            m_capacity
        ) VALUES (
            :cycleID,
            :currentLoad,
            :currentMeasured,
            :temperatureMeasured,
            :voltageLoad,
            :voltageMeasured,
            :time,
            :capacity
        )
    """
    cur = db.cursor()
    cur.execute(
        statement,
        cycleID = dischargeData.CycleID,
        currentLoad = dischargeData.CurrentLoad,
        currentMeasured = dischargeData.CurrentMeasured,
        temperatureMeasured = dischargeData.TemperatureMeasured,
        voltageLoad = dischargeData.VoltageLoad,
        voltageMeasured = dischargeData.VoltageMeasured,
        time = dischargeData.Time,
        capacity = dischargeData.Capacity
    )
    cur.close()


class BatteryChargeData(object):
    CycleID = 0
    CurrentCharge = 0.0
    CurrentMeasured = 0.0
    TemperatureMeasured = 0.0
    VoltageCharge = 0.0
    VoltageMeasured = 0.0
    Time = 0.0

def insertBatteryChargeData(chargeData: BatteryChargeData):
    statement = """
        INSERT INTO battery_charge_data (
            cycle_id,
            current_charge,
            current_measured,
            temperature_measured,
            voltage_charge,
            voltage_measured,
            m_time
        ) VALUES (
            :cycleID,
            :currentCharge,
            :currentMeasured,
            :temperatureMeasured,
            :voltageCharge,
            :voltageMeasured,
            :time
        )
    """
    cur = db.cursor()
    cur.execute(
        statement,
        cycleID = chargeData.CycleID,
        currentCharge = chargeData.CurrentCharge,
        currentMeasured = chargeData.CurrentMeasured,
        temperatureMeasured = chargeData.TemperatureMeasured,
        voltageCharge = chargeData.VoltageCharge,
        voltageMeasured = chargeData.VoltageMeasured,
        time = chargeData.Time
    )
    cur.close()



db = cx_Oracle.connect(user="ADMIN", password="Oracle12345!", dsn="burlmigration_high", threaded=True)
db.autocommit = True
print("Connected to Oracle ADW")

data = readCsv('charge-b0005.csv')

try:
    insertBattery("B0005")
except cx_Oracle.IntegrityError as e:
    errorObj, = e.args
    print("Exception: " + errorObj.message)

batteryID = selectBatteryID("B0005")
if batteryID == 0:
    print("invalid battery id")
    exit

cycle = BatteryCycle()
cycle.CycleID = selectCycleIDByBatteryIDAndCycleNumberAndType(batteryID, data[0][2], data[0][0])

currentCycleNumber = cycle.BatteryCycleNumber
with ThreadPoolExecutor(max_workers=5) as executor:
    for i in range(len(data)):
        if i < 0:
            continue  
        cycle.BatteryID = batteryID
        cycle.CycleType = data[i][0]
        cycle.AmbientTemperature = data[i][1]
        cycle.BatteryCycleNumber = data[i][2]
        if cycle.CycleType == "charge":
            cycle.PctRUL = data[i][9]
        elif cycle.CycleType == "discharge":
            cycle.PctRUL = data[i][10]
        if cycle.CycleID == 0 or cycle.BatteryCycleNumber != currentCycleNumber:
            try:
                insertBatteryCycle(cycle)
            except cx_Oracle.IntegrityError as e:
                errorObj, = e.args
                print(str(i) + " exception: " + errorObj.message)
            print('Battery ' + str(batteryID) + ' cycle ' + str(cycle.CycleID) + ' inserted')
            cycle.CycleID = selectCycleIDByBatteryIDAndCycleNumberAndType(batteryID, data[i][2], data[i][0])
            currentCycleNumber = cycle.BatteryCycleNumber      
        if cycle.CycleID > 0:
            if cycle.CycleType == "discharge":
                discharge = BatteryDischargeData()
                discharge.CycleID = cycle.CycleID
                discharge.CurrentLoad = abs(float(data[i][3]))
                discharge.CurrentMeasured = abs(float(data[i][4]))
                discharge.TemperatureMeasured = abs(float(data[i][5]))
                discharge.VoltageLoad = abs(float(data[i][6]))
                discharge.VoltageMeasured = abs(float(data[i][7]))
                discharge.Time = abs(float(data[i][8]))
                discharge.Capacity = abs(float(data[i][9]))
                if discharge.CurrentLoad > 1:
                    try:
                        executor.submit(insertBatteryDischargeData, discharge)
                    except cx_Oracle.IntegrityError as e:
                        errorObj, = e.args
                        print(str(i) + " exception: " + errorObj.message)
            if cycle.CycleType == "charge":
                charge = BatteryChargeData()
                charge.CycleID = cycle.CycleID
                charge.VoltageMeasured = abs(float(data[i][3]))
                charge.CurrentMeasured = abs(float(data[i][4]))
                charge.TemperatureMeasured = abs(float(data[i][5]))
                charge.CurrentCharge = abs(float(data[i][6]))
                charge.VoltageCharge = abs(float(data[i][7]))
                charge.Time = abs(float(data[i][8]))
                if charge.VoltageCharge > 2:
                    try:
                        executor.submit(insertBatteryChargeData, charge)
                    except cx_Oracle.IntegrityError as e:
                            errorObj, = e.args
                            print(str(i) + " exception: " + errorObj.message)
                    
print('finished')