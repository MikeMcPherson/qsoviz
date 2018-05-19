"""
qsoviz

Convert ADIF file from N3FJP Field Day Log to a MySQL database for visualization
by Grafana.

Copyright 2018 by Michael R. McPherson, Charlottesville, VA
mailto:mcpherson@acm.org
http://www.kq9p.us

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2 of the License, or (at your option) any
later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
details.

You should have received a copy of the GNU General Public License along with
this program. If not, see <http://www.gnu.org/licenses/>.
"""

__author__ = 'Michael R. McPherson <mcpherson@acm.org>'


import json
import random
from datetime import datetime, timezone, tzinfo, timedelta
import mysql.connector
from hamutils.adif import ADIReader
from qrz import QRZ


def main():

    program_name = 'qsomysql'
    program_version = '1.0'
    db_name = 'aarc'
    db_user = 'root'
    qrz_user = 'KQ9P'
    qrz = QRZ(cfg='./qrz.cfg')

    key_fp = open('passwords.json', "r")
    json_return = json.load(key_fp)
    key_fp.close()
    db_password = json_return['db_password'].encode()
    qrz_password = json_return['qrz_password'].encode()

    states_fp = open('states.json', "r")
    states_coords = json.load(states_fp)
    states_fp.close()

    cnx = mysql.connector.connect(user=db_user, password=db_password, database=db_name)
    cursor = cnx.cursor()
    cursor.execute("DELETE FROM aarc_fd")
    cnx.commit()
    add_qso = ("INSERT INTO aarc_fd "
               "(datetime_on, callsign, n3fjp_modecontest, band, state, arrl_sect, country, "
               + "n3fjp_initials, operator, class, latitude, longitude, geohash) "
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

    adif_fp = open('aarcfd2016.adi', 'r')
    aarc_adi = ADIReader(adif_fp)
    rsecs = []
    for qso in aarc_adi:
        rsec = random.random()
        while rsec in rsecs:
            rsec = random.random()
            print(rsec)
        rsecs.append(rsec)
        a_datetime_on = qso['datetime_on'] + timedelta(seconds=rsec)
        a_callsign = qso['call'].upper()
        a_n3fjp_modecontest = qso.get('n3fjp_modecontest', 'NONE').upper()
        a_band = qso.get('band', 'NONE').upper()
        a_state = qso.get('state', 'NONE').upper()
        a_arrl_sect =  qso.get('arrl_sect', 'NONE').upper()
        a_country =  qso.get('country', 'NONE').upper()
        a_n3fjp_initials =  qso.get('n3fjp_initials', 'NONE').upper()
        a_operator =  qso.get('operator', 'NONE').upper()
        a_class =  qso.get('class', 'NONE').upper()
        try:
            qrz_result = qrz.callsign(a_callsign)
        except:
            print("Not found ", a_callsign)
        if (('lat' in qrz_result.keys()) and ('lon' in qrz_result.keys())):
            a_latitude = qrz_result['lat']
            a_longitude = qrz_result['lon']
        else:
            a_latitude = states_coords[a_state][0]
            a_longitude = states_coords[a_state][1]
        a_geohash = 'aaaaa'
        data_qso = (a_datetime_on, a_callsign, a_n3fjp_modecontest, a_band, a_state, a_arrl_sect, a_country,
                    a_n3fjp_initials, a_operator, a_class, a_latitude, a_longitude, a_geohash)
        print(data_qso)
        cursor.execute(add_qso, data_qso)
    adif_fp.close()
    cnx.commit()
    cursor.close()
    cnx.close()


if __name__ == "__main__":
    # execute only if run as a script
    main()