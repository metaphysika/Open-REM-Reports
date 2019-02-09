
import sqlite3
import pandas as pd
import openpyxl
import py
# from datetime import datetime
import atexit
from time import time, strftime, localtime
from datetime import timedelta
import datetime
import os, sys

# This will help display run time for script
def secondsToStr(elapsed=None):
    if elapsed is None:
        return strftime("%Y-%m-%d %H:%M:%S", localtime())
    else:
        return str(timedelta(seconds=elapsed))

def log(s, elapsed=None):
    line = "="*40
    print(line)
    print(secondsToStr(), '-', s)
    if elapsed:
        print("Elapsed time:", elapsed)
    print(line)
    print()

def endlog():
    end = time()
    elapsed = end-start
    log("End Program", secondsToStr(elapsed))

# calls run time functions for script
start = time()
atexit.register(endlog)
log("Start Program")




# List of protocols to exclude
exclude = ['Topogram', 'PreMonitoring', 'Monitoring', 'SCOUT', 'Test Bolus', 'monitoring', 'Localizer',
           'Specials^DAILY_QC (Adult)', 'SANFORD QC(Adult)', 'Private^DAILY_QC (Adult)', 'DAILY QC PHANTOM/Head',
           'DAILY QA/QA', 'AXIAL QC', 'HELICAL QC', 'i-Sequence', 'Topogram PA', 'Topogram LAT', 'LEAD INTEGRITY/Abdomen']

# For blank accession numbers
exclude2 = ['', ' ']

# gets user input for date range.  If enter wrong date format, asks to input again.
while True:
    begindate = input("please enter begin date as yyyy-mm-dd: ")
    try:
        datetime.datetime.strptime(begindate, '%Y-%m-%d')
    except ValueError:
        print("Incorrect data format, should be YYYY-MM-DD")
    else:
        break


while True:
    enddate = input("please enter end date as yyyy-mm-dd: ")
    try:
        datetime.datetime.strptime(enddate, '%Y-%m-%d')
    except ValueError:
        print("Incorrect data format, should be YYYY-MM-DD")
    else:
        break

# path for local database
fileDb = py.path.local(r"C:\Users\clahn\Desktop\openrem.db")


# make a copy of databse file on my computer.
# This script will then perform operations on that file.
if fileDb.isfile():
    fileDb.remove()
py.path.local(r'W:\SHARE8 Physics\Software\python\data\openrem\openrem081.db').copy(fileDb)

# Connect to the database. Need .strpath to work.
db = sqlite3.connect(fileDb.strpath)
#  Creates indexes for speeding up query.  Not necessary with new query of all data in just single query.
# cursor = db.cursor()
# sql = ("""CREATE INDEX dose_index ON remapp_ctirradiationeventdata(acquisition_protocol,
#           ct_radiation_dose_id, irradiation_event_uid, mean_ctdivol);""")
# cursor.execute(sql)
# idx1 = ("""CREATE INDEX index1 ON remapp_ctradiationdose(start_of_xray_irradiation, general_study_module_attributes_id);""")
# cursor.execute(idx1)
# idx2 = ("""CREATE INDEX index2 ON remapp_generalequipmentmoduleattr(station_name, institution_name);""")
# cursor.execute(idx2)
# idx3 = ("""CREATE INDEX index3 ON remapp_patientstudymoduleattr(patient_age_decimal);""")
# cursor.execute(idx3)
# idx4 = ("""CREATE INDEX index4 ON remapp_generalstudymoduleattr(accession_number, study_description);""")
# cursor.execute(idx4)

# This is all the data in a single query.  No jumping in through pandas to get extra data.
# joins 3 tables.  Filters by user inputted date range.  Excludes blank ctdi values.
queries = ("""SELECT remapp_ctradiationdose.start_of_xray_irradiation as day, acquisition_protocol as protocol,
               mean_ctdivol as ctdi, remapp_ctirradiationeventdata.dlp as dlp,
               remapp_generalstudymoduleattr.accession_number as acc,
              remapp_generalstudymoduleattr.study_description as study, 
              remapp_generalequipmentmoduleattr.institution_name as site, 
              remapp_generalequipmentmoduleattr.station_name as station, 
              remapp_patientstudymoduleattr.patient_age_decimal as ptage,
              remapp_generalequipmentmoduleattr.manufacturer as brand,
              remapp_generalequipmentmoduleattr.manufacturer_model_name as model
              FROM remapp_ctradiationdose, remapp_ctirradiationeventdata, remapp_generalstudymoduleattr, 
              remapp_generalequipmentmoduleattr, remapp_patientstudymoduleattr
             WHERE remapp_ctradiationdose.id = remapp_ctirradiationeventdata.ct_radiation_dose_id
              AND remapp_ctradiationdose.general_study_module_attributes_id = remapp_generalstudymoduleattr.id
              AND remapp_generalequipmentmoduleattr.id = remapp_ctradiationdose.general_study_module_attributes_id
              AND remapp_patientstudymoduleattr.general_study_module_attributes_id = remapp_ctradiationdose.general_study_module_attributes_id
             AND Date(start_of_xray_irradiation) BETWEEN ? AND ? AND mean_ctdivol != ''""")


# pulls in queries sql call with begindate and endate as user input to filter date range
df = pd.read_sql(queries, db, params=(begindate, enddate))
# pd.set_option('display.max_columns', 7)
df = df[(~df['protocol'].isin(exclude)) & (~df['study'].isin(exclude)) & (~df['acc'].isin(exclude2))]
# print(df.head(40))




def create_report():
    # filepath = (r"W:\SHARE8 Physics\Software\python\scripts\clahn\Open REM Reports\Open REM Reports2.xlsx")
    #  snippet to become date in file name below.
    todaydate2 = strftime("%Y-%m-%d %H.%M.%S")
    # # naming of daily report for archival. The "/" was necessary to set the file path along with the todaydate function.
    reportname = (r"W:\SHARE8 Physics\Software\python\scripts\clahn\Open REM Reports\Open REM Reports" +
                        todaydate2 + ".xlsx")
    wb = openpyxl.Workbook()
    sheet = wb.active
    #sheet = wb['Sheet1']
    # header row titles
    header = ["Protocol", "ctdi", "dlp", "Study Description", "Patient Age", "Accession #", "Study Date",
              "Site", "Station name", "Brand", "Model"]
    sheet.append(header)
    for idx, row in df.iterrows():
        # list for adding data to spreadsheet for tracking notifications.
        nt = []
        protocol = str(row.at["protocol"])
        nt.append(protocol)
        ctdi = (row.at['ctdi'])
        nt.append(ctdi)
        dlp = (row.at['dlp'])
        nt.append(dlp)
        study = str(row.at['study'])
        nt.append(study)
        ptage = (row.at['ptage'])
        nt.append(ptage)
        acc = str(row.at['acc'])
        nt.append(acc)
        day = str(row.at["day"])
        nt.append(day)
        site = str(row.at["site"])
        nt.append(site)
        station = str(row.at["station"])
        nt.append(station)
        brand = str(row.at["brand"])
        nt.append(brand)
        model = str(row.at["model"])
        nt.append(model)
        #append full nt list to workbook
        sheet.append(nt)
    # this will overwrite the old file with same name.
    wb.save(reportname)
    wb.close()

create_report()
db.close()