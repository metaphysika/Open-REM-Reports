
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


#setup django/OpenREM
import openrem
from openrem import remapp
from openrem.remapp.extractors import rdsr
# from itertools import chain
# from remapp.models import UniqueEquipmentNames
from remapp.models import CtIrradiationEventData
# from remapp.models import CtRadiationDose
# GeneralStudyModuleAttr # has study_date and accession_number


enddate = datetime.datetime.now() - datetime.timedelta(days=30)
# https://stackoverflow.com/questions/2425603/how-do-i-select-from-multiple-tables-in-one-query-with-django

# Create query to get data from models.  The double __ will allow you to access data by foreign key on different models.
data = CtIrradiationEventData.objects.values('acquisition_protocol', 'mean_ctdivol', 'dlp', 'irradiation_event_uid',
                                                'ct_radiation_dose__start_of_xray_irradiation', 
                                                'ct_radiation_dose__general_study_module_attributes__accession_number',
                                                'ct_radiation_dose__general_study_module_attributes__study_description',
                                                'ct_radiation_dose__general_study_module_attributes__generalequipmentmoduleattr__institution_name',
                                                'ct_radiation_dose__general_study_module_attributes__generalequipmentmoduleattr__station_name',
                                                'ct_radiation_dose__general_study_module_attributes__patientstudymoduleattr__patient_age_decimal',
                                                'ct_radiation_dose__general_study_module_attributes__generalequipmentmoduleattr__manufacturer',
                                                'ct_radiation_dose__general_study_module_attributes__generalequipmentmoduleattr__manufacturer_model_name').filter( 
                                                ct_radiation_dose__start_of_xray_irradiation__range=(begindate, enddate)
                                                    )


# Generate a data series 
df = pd.DataFrame(data)

# rename columns
df.rename(columns = {'ct_radiation_dose__start_of_xray_irradiation': 'day', 
                    'acquisition_protocol': 'protocol', 'mean_ctdivol': 'ctdi', 'dlp': 'dlp', 
                    'ct_radiation_dose__general_study_module_attributes__accession_number': 'acc',
                    'ct_radiation_dose__general_study_module_attributes__study_description': 'study', 
                    'ct_radiation_dose__general_study_module_attributes__generalequipmentmoduleattr__institution_name': 'site',
                    'ct_radiation_dose__general_study_module_attributes__generalequipmentmoduleattr__station_name': 'station',
                    'ct_radiation_dose__general_study_module_attributes__patientstudymoduleattr__patient_age_decimal': 'ptage',
                    'ct_radiation_dose__general_study_module_attributes__generalequipmentmoduleattr__manufacturer': 'brand',
                    'ct_radiation_dose__general_study_module_attributes__generalequipmentmoduleattr__manufacturer_model_name': 'model'}, inplace=True)

# drop nan/blank ctdi rows
df= df.dropna(subset=['ctdi'])
# pd.set_option('display.max_columns', 7)
df = df[(~df['protocol'].isin(exclude)) & (~df['study'].isin(exclude)) & (~df['acc'].isin(exclude2))]
# print(df.head(40))




def create_report():
    # filepath = (r"W:\SHARE8 Physics\Software\python\scripts\clahn\Open REM Reports\Open REM Reports2.xlsx")
    #  snippet to become date in file name below.
    todaydate2 = strftime("%Y-%m-%d %H.%M.%S")
    # # naming of daily report for archival. The "/" was necessary to set the file path along with the todaydate function.
    reportname = (r"/var/openrem/media/xray_reports/" +
                        todaydate2 + "ct_report.xlsx") 
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
