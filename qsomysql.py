"""
qsoviz

Convert ADIF file from N3FJP Field Day Log to a MySQL database for visualization
by Grafana.

Copyright 2018 by Michael R. McPherson, Charlottesville, VA
mailto:mcpherson@acm.org
http://www.kq9p.us

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

__author__ = 'Michael R. McPherson <mcpherson@acm.org>'


import time
import configparser
import logging
import json
from datetime import datetime,timedelta,timezone
import mysql.connector
from hamutils.adif import ADIReader, ADIWriter
from qrz import QRZ
import geohash2
import pandas_access as mdb
import os
import subprocess


def newest_file(path):
    files = os.listdir(path)
    paths = [os.path.join(path, basename) for basename in files]
    return max(paths, key=os.path.getctime)

def geo_info(qrz, a_callsign, a_state, states_coords):
    try:
        qrz_result = qrz.callsign(a_callsign)
    except:
        try:
            a_latitude = states_coords[a_state][0]
            a_longitude = states_coords[a_state][1]
        except:
            a_latitude = 0.0
            a_longitude = 0.0
    else:
        a_latitude = float(qrz_result['lat'])
        a_longitude = float(qrz_result['lon'])
    a_geohash = geohash2.encode(a_latitude, a_longitude, precision=5)
    return(a_latitude, a_longitude, a_geohash)


def main():

    program_name = 'qsomysql'
    program_version = '1.2'

    qrz = QRZ(cfg='./qrz.ini')

    config = configparser.ConfigParser()
    config.read(['qsoviz.ini', 'qrz.ini'])
    use_n3fjp_mdb = config['general'].getboolean('use_n3fjp_mdb')
    reset_mysql_db = config['general'].getboolean('reset_mysql_db')
    db_user = config['mysql']['db_user']
    db_password = config['mysql']['db_password']
    db_name = config['mysql']['db_name']

    logging.basicConfig(filename='/home/qsoviz/qsomysql.log', level=logging.INFO, format='%(asctime)s %(message)s')
    logging.info('%s %s: Run started', program_name, program_version)

    states_fp = open('states.json', 'r')
    states_coords = json.load(states_fp)
    states_fp.close()

    mysql_cnx = mysql.connector.connect(user=db_user, password=db_password, database=db_name)
    mysql_cursor = mysql_cnx.cursor()
    if reset_mysql_db:
        mysql_cursor.execute("DELETE FROM aarc_fd")
    mysql_cnx.commit()
    add_qso = ("INSERT INTO aarc_fd "
               "(datetime_on, callsign, n3fjp_modecontest, band, state, arrl_sect, country, "
               + "n3fjp_initials, operator, class, computer_name, latitude, longitude, geohash) "
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    query_qso = "SELECT datetime_on, callsign FROM aarc_fd WHERE datetime_on = %s AND callsign = %s"
    logfile_filename = newest_file('/home/qsoviz/logfiles')

    records_added = 0
    if use_n3fjp_mdb:
        fields = ['fldCall', 'fldModeContest', 'fldBand', 'fldState', 'fldSection', 'fldCountryWorked',
                  'fldInitials', 'fldOperator', 'fldClass', 'fldComputerName']
        df = mdb.read_table(logfile_filename, 'tblContacts')
        for idx, row in df.iterrows():
            fldDateStr = row.loc['fldDateStr']
            fldTimeOnStr = row.loc['fldTimeOnStr']
            year, month, day = fldDateStr.split('/')
            hour, minute, second = fldTimeOnStr.split(':')
            a_datetime_on = datetime(int(year), int(month), int(day), hour=int(hour),
                                              minute=int(minute), second=int(second), tzinfo=timezone.utc)
            a_callsign = row.loc['fldCall']
            query_qso_data = (a_datetime_on, a_callsign)
            mysql_cursor.execute(query_qso, query_qso_data)
            result = mysql_cursor.fetchall()
            if len(result) == 0:
                add_qso_data = [a_datetime_on]
                for field in fields:
                    temp = row.loc[field]
                    if temp != temp:
                        temp = ''
                    temp = temp.upper()
                    add_qso_data.append(temp)
                a_latitude, a_longitude, a_geohash = geo_info(qrz, add_qso_data[1], add_qso_data[4], states_coords)
                add_qso_data.append(a_latitude)
                add_qso_data.append(a_longitude)
                add_qso_data.append(a_geohash)
                add_qso_data = tuple(add_qso_data)
                mysql_cursor.execute(add_qso, add_qso_data)
                mysql_cnx.commit()
                records_added = records_added + 1
    else:
        adif_fp = open(logfile_filename, 'r')
        qsos = ADIReader(adif_fp)
        for qso in qsos:
            a_datetime_on = qso['datetime_on']
            fldCall = qso['call'].upper()
            query_qso_data = (a_datetime_on, fldCall)
            result = mysql_cursor(query_qso, query_qso_data)
            if not result:
                fldModeContest = qso.get('mode', 'NONE').upper()
                fldBand = qso.get('band', 'NONE').upper()
                fldState = qso.get('state', 'NONE').upper()
                fldSection = qso.get('arrl_sect', 'NONE').upper()
                fldCountryWorked = qso.get('country', 'NONE').upper()
                fldInitials = qso.get('app_n3fjp_initials', 'NONE').upper()
                fldOperator = qso.get('operator', 'NONE').upper()
                fldClass = qso.get('class', 'NONE').upper()
                fldComputerName = qso.get('n3fjp_stationid', 'NONE').upper()
                a_latitude, a_longitude, a_geohash = geo_info(qrz, fldCall, fldState, states_coords)
                add_qso_data = (a_datetime_on, fldCall, fldModeContest, fldBand, fldState, fldSection, fldCountryWorked,
                                fldInitials, fldOperator, fldClass, fldComputerName, a_latitude, a_longitude, a_geohash)
                mysql_cursor.execute(add_qso, add_qso_data)
                mysql_cnx.commit()
                records_added = records_added + 1

    if not use_n3fjp_mdb:
        adif_fp.close()
    mysql_cursor.close()
    mysql_cnx.close()
    logging.info('%s %s: records_added = %s', program_name, program_version, records_added)
    logging.info('%s %s: Run ended', program_name, program_version)

if __name__ == "__main__":
    # execute only if run as a script
    main()
