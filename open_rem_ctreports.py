
import sqlite3
import pandas as pd
import openpyxl
import py
from datetime import datetime
import atexit
from time import time, strftime, localtime
from datetime import timedelta
import datetime

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

start = time()
atexit.register(endlog)
log("Start Program")


# List of protocols to exclude
exclude = ['Topogram', 'PreMonitoring', 'Monitoring', 'SCOUT', 'Test Bolus', 'monitoring', 'Localizer',
           'Specials^DAILY_QC (Adult)', 'SANFORD QC(Adult)', 'Private^DAILY_QC (Adult)', 'DAILY QC PHANTOM/Head',
           'DAILY QA/QA', 'AXIAL QC', 'HELICAL QC', 'i-Sequence', 'Topogram PA', 'Topogram LAT', 'LEAD INTEGRITY/Abdomen']

# begindate = input("please enter begin date as yyyy-mm-dd: ")
# enddate = input("please enter end date as yyyy-mm-dd: ")

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

#begindate = datetime.strptime(begindate, '%Y-%m-%d')
#enddate = datetime.strptime(enddate, '%Y-%m-%d')
#print(type(begindate))
#print(type(enddate))

# path for local database
fileDb = py.path.local(r"C:\Users\clahn\Desktop\openrem.db")


# make a copy of databse file on my computer.
# This script will then perform operations on that file.
if fileDb.isfile():
    fileDb.remove()
py.path.local(r'W:\SHARE8 Physics\Software\python\data\openrem\openrem081.db').copy(fileDb)

# Connect to the database. Need .strpath to work.
db = sqlite3.connect(fileDb.strpath)
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


# queries = ("""SELECT acquisition_protocol as protocol, mean_ctdivol as ctdi, irradiation_event_uid as uid,
#              remapp_ctradiationdose.start_of_xray_irradiation as day,
#              remapp_generalstudymoduleattr.accession_number as acc,
#              remapp_generalstudymoduleattr.study_description as study
#              FROM remapp_ctirradiationeventdata, remapp_ctradiationdose, remapp_generalstudymoduleattr
#              WHERE remapp_ctirradiationeventdata.ct_radiation_dose_id = remapp_ctradiationdose.id
#              AND Date(start_of_xray_irradiation) >= ? AND Date(start_of_xray_irradiation) <= ? """)

# really crazy idea to try and study description in query so I can check against exclude list and ignore those rows.
# can't get it to work.  Doesn't like the ? for the list or passing a list to the pandas call.
# queries = ("""SELECT acquisition_protocol as protocol, mean_ctdivol as ctdi, irradiation_event_uid as uid,
#              remapp_ctradiationdose.start_of_xray_irradiation as day,
#              remapp_generalstudymoduleattr.accession_number as acc,
#              remapp_generalstudymoduleattr.study_description as study
#              FROM remapp_ctirradiationeventdata, remapp_ctradiationdose, remapp_generalstudymoduleattr
#              WHERE remapp_ctirradiationeventdata.ct_radiation_dose_id = remapp_ctradiationdose.id
#              AND remapp_ctradiationdose.general_study_module_attributes_id = remapp_generalstudymoduleattr.id
#              AND Date(start_of_xray_irradiation) >= ? AND Date(start_of_xray_irradiation) <= ? AND mean_ctdivol != ''
#              AND remapp_generalstudymoduleattr.study_description NOT IN ?""")

# strftime('%Y-%m-%d', date)
# This works.  I needed to use params in the pandas connection below.  the ? is the place holder for the
# Variable that gets passed in the pandas connection.
# queries = ("""SELECT acquisition_protocol as protocol, mean_ctdivol as ctdi, irradiation_event_uid as uid,
#              remapp_ctradiationdose.start_of_xray_irradiation as day,
#              remapp_generalstudymoduleattr.accession_number as acc,
#              remapp_generalstudymoduleattr.study_description as study
#              FROM remapp_ctirradiationeventdata, remapp_ctradiationdose, remapp_generalstudymoduleattr
#              WHERE remapp_ctirradiationeventdata.ct_radiation_dose_id = remapp_ctradiationdose.id
#              AND Date(start_of_xray_irradiation) >= ? AND Date(start_of_xray_irradiation) <= ? AND mean_ctdivol != ''""")
# ValueError: operation parameter must be str
#Added the mean_ctdivol != '' to exlcude duplicate entires with nan values for ctdi
# queries = ("""SELECT acquisition_protocol as protocol, mean_ctdivol as ctdi, irradiation_event_uid as uid,
#              remapp_ctradiationdose.start_of_xray_irradiation as day
#              FROM remapp_ctirradiationeventdata, remapp_ctradiationdose
#              WHERE remapp_ctirradiationeventdata.ct_radiation_dose_id = remapp_ctradiationdose.id
#              AND Date(start_of_xray_irradiation) >= ? AND Date(start_of_xray_irradiation) <= ? AND mean_ctdivol != ''""")

# look at ct radiationdose table first and join on ctirradiationeventdata.  first has 53,000 entries and second 173,000.
# queries = ("""SELECT remapp_ctradiationdose.start_of_xray_irradiation as day, acquisition_protocol as protocol,
#                mean_ctdivol as ctdi, irradiation_event_uid as uid
#               FROM remapp_ctradiationdose, remapp_ctirradiationeventdata
#              WHERE remapp_ctradiationdose.id = remapp_ctirradiationeventdata.ct_radiation_dose_id
#              AND Date(start_of_xray_irradiation) BETWEEN ? AND ? AND mean_ctdivol != ''""")

# look at ct radiationdose table first and join on ctirradiationeventdata.  first has 53,000 entries and second 173,000.
# queries = ("""SELECT remapp_ctradiationdose.start_of_xray_irradiation as day, acquisition_protocol as protocol,
#                mean_ctdivol as ctdi, irradiation_event_uid as uid, remapp_generalstudymoduleattr.accession_number as acc,
#               remapp_generalstudymoduleattr.study_description as study
#               FROM remapp_ctradiationdose, remapp_ctirradiationeventdata, remapp_generalstudymoduleattr
#              WHERE remapp_ctradiationdose.id = remapp_ctirradiationeventdata.ct_radiation_dose_id
#               AND remapp_ctradiationdose.general_study_module_attributes_id = remapp_generalstudymoduleattr.id
#              AND Date(start_of_xray_irradiation) BETWEEN ? AND ? AND mean_ctdivol != ''""")

# This is all the data in a single query.  No jumping in through pandas to get extra data.
queries = ("""SELECT remapp_ctradiationdose.start_of_xray_irradiation as day, acquisition_protocol as protocol,
               mean_ctdivol as ctdi, irradiation_event_uid as uid, 
               remapp_generalstudymoduleattr.accession_number as acc,
              remapp_generalstudymoduleattr.study_description as study, 
              remapp_generalequipmentmoduleattr.institution_name as site, 
              remapp_generalequipmentmoduleattr.station_name as station, 
              remapp_patientstudymoduleattr.patient_age_decimal as ptage
              FROM remapp_ctradiationdose, remapp_ctirradiationeventdata, remapp_generalstudymoduleattr, 
              remapp_generalequipmentmoduleattr, remapp_patientstudymoduleattr
             WHERE remapp_ctradiationdose.id = remapp_ctirradiationeventdata.ct_radiation_dose_id
              AND remapp_ctradiationdose.general_study_module_attributes_id = remapp_generalstudymoduleattr.id
              AND remapp_generalequipmentmoduleattr.id = remapp_ctradiationdose.general_study_module_attributes_id
              AND remapp_patientstudymoduleattr.id = remapp_ctradiationdose.general_study_module_attributes_id
             AND Date(start_of_xray_irradiation) BETWEEN ? AND ? AND mean_ctdivol != ''""")

# AND (SELECT remapp_generalstudymoduleattr.study_description as study FROM remapp_generalstudymoduleattr
#              WHERE remapp_ctradiationdose.general_study_module_attributes_id = remapp_generalstudymoduleattr.id
#              AND remapp_generalstudymoduleattr.study_description NOT IN ?)

# SELECT remapp_generalstudymoduleattr.study_description as study FROM remapp_generalstudymoduleattr
# WHERE remapp_ctradiationdose.general_study_module_attributes_id = remapp_generalstudymoduleattr.id
# AND remapp_generalstudymoduleattr.study_description NOT IN ?

# Old version that just grabbed 3 things  from database and funcitons below grabbed other items.
#queries = ("""SELECT acquisition_protocol as protocol, mean_ctdivol as ctdi, irradiation_event_uid as uid,
             # remapp_ctradiationdose.start_of_xray_irradiation as day
             # FROM remapp_ctirradiationeventdata, remapp_ctradiationdose
             # WHERE remapp_ctirradiationeventdata.ct_radiation_dose_id = remapp_ctradiationdose.id
             # AND Date(start_of_xray_irradiation) >= ? AND Date(start_of_xray_irradiation) <= ? """)


#queries = ("""SELECT start_of_xray_irradiation as day
              #FROM remapp_ctradiationdose
              #WHERE Date(start_of_xray_irradiation) BETWEEN ? AND ?""")
# AttributeError: 'tuple' object has no attribute 'cursor'


# THis works for just pulling a query from SQLite.
#queries = ("""SELECT start_of_xray_irradiation as day
              #FROM remapp_ctradiationdose
             # WHERE Date(start_of_xray_irradiation) BETWEEN '2018-10-06' AND '2018-11-07'""")

#, (begindate, enddate,)

# This works for printing off just the SQLite database.  The str error must be happening with pandas.
# This can pull data real quickly
#conn = sqlite3.connect(fileDb.strpath)
#cur = conn.cursor()
#cur.execute(queries, (begindate, enddate))
#rows = cur.fetchall()

#for row in rows:
        #print(row)


df = pd.read_sql(queries, db, params=(begindate, enddate))
# pd.set_option('display.max_columns', 7)
df = df[(~df['protocol'].isin(exclude)) & (~df['study'].isin(exclude))]
# df = df[~df['protocol'].isin(exclude)]
# print(df.head(40))

# Original, non-filter version.
# queries = ('''SELECT acquisition_protocol as protocol, mean_ctdivol as ctdi, irradiation_event_uid as uid
# FROM remapp_ctirradiationeventdata ;''')

'''
# Notes on adding specific reference to tabl.column in the select portion.
# https://stackoverflow.com/questions/7478645/sqlite3-select-from-multiple-tables-where-stuff


# Original, non-filter version.
SELECT acquisition_protocol as protocol, mean_ctdivol as ctdi, irradiation_event_uid as uid
FROM remapp_ctirradiationeventdata ;

# Filtered version. This works!
queries = ("""SELECT remapp_ctirradiationeventdata.acquisition_protocol as protocol, remapp_ctirradiationeventdata.mean_ctdivol as ctdi, remapp_ctirradiationeventdata.irradiation_event_uid as uid, remapp_ctradiationdose.start_of_xray_irradiation as day FROM remapp_ctirradiationeventdata INNER JOIN remapp_ctradiationdose on remapp_ctirradiationeventdata.ct_radiation_dose_id = remapp_ctradiationdose.id  WHERE remapp_ctradiationdose.start_of_xray_irradiation > date('now', '-300 days') LIMIT 10""")

# This works too.  Shortened version where I don't call the specific table along with the column.
# ex. remapp_ctirradiationeventdata.acquisition_protocol as protocol vs. acquisition_protocol as protocol
queries = ("""SELECT acquisition_protocol as protocol, mean_ctdivol as ctdi, irradiation_event_uid as uid, start_of_xray_irradiation as day FROM remapp_ctirradiationeventdata INNER JOIN remapp_ctradiationdose on remapp_ctirradiationeventdata.ct_radiation_dose_id = remapp_ctradiationdose.id  WHERE remapp_ctradiationdose.start_of_xray_irradiation > date('now', '-300 days') LIMIT 10""")


# This works as an alternative to inner join.
queries = ("""SELECT acquisition_protocol as protocol, mean_ctdivol as ctdi, irradiation_event_uid as uid, start_of_xray_irradiation as day FROM remapp_ctirradiationeventdata, remapp_ctradiationdose WHERE remapp_ctirradiationeventdata.ct_radiation_dose_id = remapp_ctradiationdose.id AND remapp_ctradiationdose.start_of_xray_irradiation > datetime('now', '-300 days') LIMIT 10""")
'''

#
# # pandas dataframe
# # pd.set_option('display.max_columns', 5)
# #df = pd.read_sql_query(queries, db)
# # df['protocol'] = df['protocol'].astype(str)
# # trying to set dat column to datetime format
# #df['day'] = pd.to_datetime(df['day'], format="%Y/%m/%d")
#
#
# #begindate = input("please enter begin date as yyyy-mm-dd: ")
# #enddate = input("please enter end date as yyyy-mm-dd: ")
# #begindate = datetime.strptime(begindate, '%Y-%m-%d')
# #enddate = datetime.strptime(enddate, '%Y-%m-%d')
# # create mask of just date specified by user
# # mask = (df['day'] > begindate) & (df['day'] <= enddate)
# # df = df.loc[mask]
# # mask dataframe to exclude protocols in list exclude



# function that takes the uid and finds exam accession number.
# def get_accession(uid):
#     uidrow = db.cursor().execute(f"SELECT ct_radiation_dose_id "
#                                 f"FROM remapp_ctirradiationeventdata "
#                                  f"WHERE irradiation_event_uid=?", (uid,)).fetchone()[0]
#     ctdoseid = db.cursor().execute(f"SELECT general_study_module_attributes_id "
#                                    f"FROM remapp_ctradiationdose "
#                                    f"WHERE id=?", (uidrow,)).fetchone()[0]
#     accnum = db.cursor().execute(f"SELECT accession_number "
#                                  f"FROM remapp_generalstudymoduleattr "
#                                  f"WHERE id=?", (ctdoseid,)).fetchone()[0]
#     return accnum
#
# def get_studydescription(uid):
#     uidrow = db.cursor().execute(f"SELECT ct_radiation_dose_id "
#                                  f"FROM remapp_ctirradiationeventdata "
#                                  f"WHERE irradiation_event_uid=?", (uid,)).fetchone()[0]
#     ctdoseid = db.cursor().execute(f"SELECT general_study_module_attributes_id "
#                                    f"FROM remapp_ctradiationdose "
#                                    f"WHERE id=?", (uidrow,)).fetchone()[0]
#     studydescription = db.cursor().execute(f"SELECT study_description "
#                                  f"FROM remapp_generalstudymoduleattr "
#                                  f"WHERE id=?", (ctdoseid,)).fetchone()[0]
#     return studydescription
#
# def get_examdate(uid):
#     uidrow = db.cursor().execute(f"SELECT ct_radiation_dose_id "
#                                  f"FROM remapp_ctirradiationeventdata "
#                                  f"WHERE irradiation_event_uid=?", (uid,)).fetchone()[0]
#     raddate = db.cursor().execute(f"SELECT start_of_xray_irradiation "
#                                   f"FROM remapp_ctradiationdose "
#                                   f"WHERE id=?", (uidrow,)).fetchone()[0]
#     return raddate
#
# # function that takes the uid and finds site location.
#
#
# def get_site(uid):
#     uidrow = db.cursor().execute(f"SELECT ct_radiation_dose_id "
#                                  f"FROM remapp_ctirradiationeventdata "
#                                  f"WHERE irradiation_event_uid=?", (uid,)).fetchone()[0]
#     ctdoseid = db.cursor().execute(f"SELECT general_study_module_attributes_id "
#                                    f"FROM remapp_ctradiationdose "
#                                    f"WHERE id=?", (uidrow,)).fetchone()[0]
#     site = db.cursor().execute(f"SELECT institution_name "
#                                f"FROM remapp_generalequipmentmoduleattr "
#                                f"WHERE general_study_module_attributes_id=?", (ctdoseid,)).fetchone()[0]
#     return site
#
# # function that takes the uid and finds station name.
#
#
# def get_station(uid):
#     uidrow = db.cursor().execute(f"SELECT ct_radiation_dose_id "
#                                  f"FROM remapp_ctirradiationeventdata "
#                                  f"WHERE irradiation_event_uid=?", (uid,)).fetchone()[0]
#     ctdoseid = db.cursor().execute(f"SELECT general_study_module_attributes_id "
#                                    f"FROM remapp_ctradiationdose "
#                                    f"WHERE id=?", (uidrow,)).fetchone()[0]
#     station = db.cursor().execute(f"SELECT station_name "
#                                   f"FROM remapp_generalequipmentmoduleattr "
#                                   f"WHERE general_study_module_attributes_id=?", (ctdoseid,)).fetchone()[0]
#     return station
#
#
# def scanner_alert_limit(uid):
#     try:
#         uidrow = db.cursor().execute(f"SELECT id "
#                                      f"FROM remapp_ctirradiationeventdata "
#                                      f"WHERE irradiation_event_uid=?", (uid,)).fetchone()[0]
#         scanalert = db.cursor().execute(f"SELECT ctdivol_notification_value "
#                                         f"FROM remapp_ctdosecheckdetails "
#                                         f"WHERE ct_irradiation_event_data_id=?", (uidrow,)).fetchone()[0]
#         return scanalert
#     # was getting a type error.  wasn't able to grab [0] on some rows????
#     except TypeError:
#         return "Unknown"
#
# def get_ptage(uid):
#     uidrow = db.cursor().execute(f"SELECT ct_radiation_dose_id "
#                                  f"FROM remapp_ctirradiationeventdata "
#                                  f"WHERE irradiation_event_uid=?", (uid,)).fetchone()[0]
#     ctdoseid = db.cursor().execute(f"SELECT general_study_module_attributes_id "
#                                    f"FROM remapp_ctradiationdose "
#                                    f"WHERE id=?", (uidrow,)).fetchone()[0]
#     ptage = db.cursor().execute(f"SELECT patient_age_decimal "
#                                   f"FROM remapp_patientstudymoduleattr "
#                                   f"WHERE general_study_module_attributes_id=?", (ctdoseid,)).fetchone()[0]
#     return ptage


def create_report():
    filepath = (r"W:\SHARE8 Physics\Software\python\scripts\clahn\Open REM Reports\Open REM Reports.xlsx")
    wb = openpyxl.Workbook()
    sheet = wb.active
    # header row titles
    header = ["Protocol", "uid", "Study Description", "ctdi", "Patient Age", "Accession #", "Study Date",
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
        study = str(row.at['study'])
        nt.append(study)
        # studydescription = get_studydescription(uid)
        # nt.append(studydescription)
        ctdi = str(row.at['ctdi'])
        nt.append(ctdi)
        ptage = str(row.at['ptage'])
        nt.append(ptage)
        # ptage = get_ptage(uid)
        # nt.append(ptage)
        # alert_limit = str(limit)
        # nt.append(alert_limit)
        #scanalert = scanner_alert_limit(uid)
        #nt.append(scanalert)
        # calls function that matches up uid with accession # in database.
        acc = str(row.at['acc'])
        nt.append(acc)
        # acc = get_accession(uid)
        # nt.append(acc)
        day = str(row.at["day"])
        nt.append(day)
        # calls function that matches up uid with beginning of radiation event (study date) in database.
        #studydate = get_examdate(uid)
        #nt.append(studydate)
        # calls function that matches up uid with Site name in database.
        site = str(row.at["site"])
        nt.append(site)
        # siteadd = get_site(uid)
        # nt.append(siteadd)
        # calls function that matches up uid with station name in database.
        station = str(row.at["station"])
        nt.append(station)
        # stationname = get_station(uid)
        # nt.append(stationname)
        sheet.append(nt)
    # this will overwrite the old file with same name.
    wb.save(filepath)
    wb.close()

create_report()
db.close()