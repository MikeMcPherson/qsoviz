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
from datetime import datetime, timezone, tzinfo
import mysql.connector
from hamutils.adif import ADIReader

program_name = 'qsoviz'
program_version = '1.0'
db_name = 'aarcfd'
db_user = 'root'
demo_mode = True

if demo_mode:
    time_start = datetime(2016, 6, 25, 18, 0, 0, tzinfo=timezone.utc).timestamp()
    time_end = datetime(2016, 6, 26, 18, 0, 0, tzinfo=timezone.utc).timestamp()
else:
    time_start = datetime(2018, 6, 23, 18, 0, 0, tzinfo=timezone.utc).timestamp()
    time_end = datetime(2018, 6, 24, 18, 0, 0, tzinfo=timezone.utc).timestamp()

key_fp = open('db_password.json', "r")
json_return = json.load(key_fp)
key_fp.close()
db_password = json_return['db_password'].encode()

adif_fp = open('aarcfd2016.adi', 'r', encoding='ascii')
aarc_adi = ADIReader(adif_fp)
for qso[:10] in aarc_adi:
    print(qso)
#cnx = mysql.connector.connect(user=db_user, password=db_password, database=db_name)
#cursor = cnx.cursor()

sql_command = ('INSERT INTO ' + db_name + ' '
               '(timeUTC, '
               'panelTempT1, '
               'panelTempT2, '
               'panelTempT3, '
               'panelTempT4, '
               'panelTempT5, '
               'panelVoltageV1, '
               'panelVoltageV2, '
               'panelVoltageV3, '
               'panelVoltageV4, '
               'panelVoltageV5, '
               'batteryVoltageB1, '
               'batteryVoltageB2, '
               'batteryVoltageB3, '
               'batteryVoltageB4, '
               'motherboardTemp, '
               'uncontrolledResets, '
               'mcuResets, '
               'readWriteResets, '
               'sdErrors, '
               'antennaStatus) '
               'VALUES '
               '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)')

"""
sql_data = ("{:s}".format(time_utc),
            "{:.2f}".format(panel_temp_px_t1),
            "{:.2f}".format(panel_temp_mx_t2),
            "{:.2f}".format(panel_temp_py_t3),
            "{:.2f}".format(panel_temp_my_t4),
            "{:.2f}".format(panel_temp_mz_t5),
            "{:.2f}".format(panel_voltage_px_v1),
            "{:.2f}".format(panel_voltage_mx_v2),
            "{:.2f}".format(panel_voltage_py_v3),
            "{:.2f}".format(panel_voltage_my_v4),
            "{:.2f}".format(panel_voltage_mz_v5),
            "{:.2f}".format(battery_voltage_b1),
            "{:.2f}".format(battery_voltage_b2),
            "{:.2f}".format(battery_voltage_b3),
            "{:.2f}".format(battery_voltage_b4),
            "{:.2f}".format(motherboard_temp),
            "{:d}".format(uncontrolled_resets),
            "{:d}".format(mcu_resets),
            "{:d}".format(read_write_resets),
            "{:d}".format(sd_errors),
            "{:d}".format(antenna_status))

cursor.execute(sql_command, sql_data)
cnx.commit()
cursor.close()
cnx.close()
"""