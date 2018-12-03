
import sqlite3
import pandas as pd
import openpyxl
import py
from datetime import datetime

# List of protocols to exclude
exclude = ['Topogram', 'PreMonitoring', 'Monitoring', 'SCOUT', 'Test Bolus', 'monitoring', 'Localizer']

# path for local database
fileDb = py.path.local(r"C:\Users\clahn\Desktop\openrem.db")


# make a copy of databse file on my computer.
# This script will then perform operations on that file.
if fileDb.isfile():
    fileDb.remove()
py.path.local(r'W:\SHARE8 Physics\Software\python\data\openrem\openrem081.db').copy(fileDb)

# Connect to the database. Need .strpath to work.
db = sqlite3.connect(fileDb.strpath)

# selects data from database.  LIMIT will  limit results to specified number.
queries = ("""SELECT acquisition_protocol as protocol, mean_ctdivol as ctdi, irradiation_event_uid as uid,
              start_of_xray_irradiation as day FROM remapp_ctirradiationeventdata, remapp_ctradiationdose
              WHERE remapp_ctirradiationeventdata.ct_radiation_dose_id = remapp_ctradiationdose.id""")

# pandas dataframe
# pd.set_option('display.max_columns', 5)
df = pd.read_sql_query(queries, db)
df['protocol'] = df['protocol'].astype(str)
# trying to set dat column to datetime format
df['day'] = pd.to_datetime(df['day'], format="%Y/%m/%d")


begindate = input("please enter begin date as yyyy-mm-dd: ")
enddate = input("please enter end date as yyyy-mm-dd: ")
begindate = datetime.strptime(begindate, '%Y-%m-%d')
enddate = datetime.strptime(enddate, '%Y-%m-%d')
# create mask of just date specified by user
mask = (df['day'] > begindate) & (df['day'] <= enddate)
df = df.loc[mask]
# mask dataframe to exclude protocols in list exclude
df = df[~df['protocol'].isin(exclude)]

#print(df.head(40))



# function that takes the uid and finds exam accession number.


def get_accession(uid):
    uidrow = db.cursor().execute(f"SELECT ct_radiation_dose_id "
                                 f"FROM remapp_ctirradiationeventdata "
                                 f"WHERE irradiation_event_uid=?", (uid,)).fetchone()[0]
    ctdoseid = db.cursor().execute(f"SELECT general_study_module_attributes_id "
                                   f"FROM remapp_ctradiationdose "
                                   f"WHERE id=?", (uidrow,)).fetchone()[0]
    accnum = db.cursor().execute(f"SELECT accession_number "
                                 f"FROM remapp_generalstudymoduleattr "
                                 f"WHERE id=?", (ctdoseid,)).fetchone()[0]
    return accnum

def get_studydescription(uid):
    uidrow = db.cursor().execute(f"SELECT ct_radiation_dose_id "
                                 f"FROM remapp_ctirradiationeventdata "
                                 f"WHERE irradiation_event_uid=?", (uid,)).fetchone()[0]
    ctdoseid = db.cursor().execute(f"SELECT general_study_module_attributes_id "
                                   f"FROM remapp_ctradiationdose "
                                   f"WHERE id=?", (uidrow,)).fetchone()[0]
    studydescription = db.cursor().execute(f"SELECT study_description "
                                 f"FROM remapp_generalstudymoduleattr "
                                 f"WHERE id=?", (ctdoseid,)).fetchone()[0]
    return studydescription

def get_examdate(uid):
    uidrow = db.cursor().execute(f"SELECT ct_radiation_dose_id "
                                 f"FROM remapp_ctirradiationeventdata "
                                 f"WHERE irradiation_event_uid=?", (uid,)).fetchone()[0]
    raddate = db.cursor().execute(f"SELECT start_of_xray_irradiation "
                                  f"FROM remapp_ctradiationdose "
                                  f"WHERE id=?", (uidrow,)).fetchone()[0]
    return raddate

# function that takes the uid and finds site location.


def get_site(uid):
    uidrow = db.cursor().execute(f"SELECT ct_radiation_dose_id "
                                 f"FROM remapp_ctirradiationeventdata "
                                 f"WHERE irradiation_event_uid=?", (uid,)).fetchone()[0]
    ctdoseid = db.cursor().execute(f"SELECT general_study_module_attributes_id "
                                   f"FROM remapp_ctradiationdose "
                                   f"WHERE id=?", (uidrow,)).fetchone()[0]
    site = db.cursor().execute(f"SELECT institution_name "
                               f"FROM remapp_generalequipmentmoduleattr "
                               f"WHERE general_study_module_attributes_id=?", (ctdoseid,)).fetchone()[0]
    return site

# function that takes the uid and finds station name.


def get_station(uid):
    uidrow = db.cursor().execute(f"SELECT ct_radiation_dose_id "
                                 f"FROM remapp_ctirradiationeventdata "
                                 f"WHERE irradiation_event_uid=?", (uid,)).fetchone()[0]
    ctdoseid = db.cursor().execute(f"SELECT general_study_module_attributes_id "
                                   f"FROM remapp_ctradiationdose "
                                   f"WHERE id=?", (uidrow,)).fetchone()[0]
    station = db.cursor().execute(f"SELECT station_name "
                                  f"FROM remapp_generalequipmentmoduleattr "
                                  f"WHERE general_study_module_attributes_id=?", (ctdoseid,)).fetchone()[0]
    return station


def scanner_alert_limit(uid):
    try:
        uidrow = db.cursor().execute(f"SELECT id "
                                     f"FROM remapp_ctirradiationeventdata "
                                     f"WHERE irradiation_event_uid=?", (uid,)).fetchone()[0]
        scanalert = db.cursor().execute(f"SELECT ctdivol_notification_value "
                                        f"FROM remapp_ctdosecheckdetails "
                                        f"WHERE ct_irradiation_event_data_id=?", (uidrow,)).fetchone()[0]
        return scanalert
    # was getting a type error.  wasn't able to grab [0] on some rows????
    except TypeError:
        return "Unknown"

def get_ptage(uid):
    uidrow = db.cursor().execute(f"SELECT ct_radiation_dose_id "
                                 f"FROM remapp_ctirradiationeventdata "
                                 f"WHERE irradiation_event_uid=?", (uid,)).fetchone()[0]
    ctdoseid = db.cursor().execute(f"SELECT general_study_module_attributes_id "
                                   f"FROM remapp_ctradiationdose "
                                   f"WHERE id=?", (uidrow,)).fetchone()[0]
    ptage = db.cursor().execute(f"SELECT patient_age_decimal "
                                  f"FROM remapp_patientstudymoduleattr "
                                  f"WHERE general_study_module_attributes_id=?", (ctdoseid,)).fetchone()[0]
    return ptage


def create_report():
    filepath = (r"W:\SHARE8 Physics\Software\python\scripts\clahn\Open REM Reports\Open REM Reports.xlsx")
    wb = openpyxl.Workbook()
    sheet = wb.active
    # header row titles
    header = ["Protocol", "uid", "Study Description", "ctdi", "Patient Age", "scan alert", "Accession #", "Study Date",
              "Site", "Station name"]
    sheet.append(header)
    #sheet = wb['Sheet1']
    for idx, row in df.iterrows():
        # list for adding data to spreadsheet for tracking notifications.
        nt = []
        protocol = str(row.at["protocol"])
        nt.append(protocol)
        uid = str(row.at['uid'])
        nt.append(uid)
        studydescription = get_studydescription(uid)
        nt.append(studydescription)
        ctdi = str(row.at['ctdi'])
        nt.append(ctdi)
        ptage = get_ptage(uid)
        nt.append(ptage)
        # alert_limit = str(limit)
        # nt.append(alert_limit)
        scanalert = scanner_alert_limit(uid)
        nt.append(scanalert)
        # calls function that matches up uid with accession # in database.
        acc = get_accession(uid)
        nt.append(acc)
        # calls function that matches up uid with beginning of radiation event (study date) in database.
        studydate = get_examdate(uid)
        nt.append(studydate)
        # calls function that matches up uid with Site name in database.
        siteadd = get_site(uid)
        nt.append(siteadd)
        # calls function that matches up uid with station name in database.
        stationname = get_station(uid)
        nt.append(stationname)
        sheet.append(nt)
    # this will overwrite the old file with same name.
    wb.save(filepath)
    wb.close()

create_report()
db.close()
