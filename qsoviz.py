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
import time
from datetime import datetime, timezone, tzinfo
import mysql.connector
from hamutils.adif import ADIReader


def main():

    program_name = 'qsoviz'
    program_version = '1.0'
    db_name = 'aarcfd'
    db_user = 'root'
    demo_mode = True
    demo_timescale = (24.0 * 60.0) / 2.0

    key_fp = open('db_password.json', "r")
    json_return = json.load(key_fp)
    key_fp.close()
    db_password = json_return['db_password'].encode()

    if demo_mode:
        time_start = datetime(2016, 6, 25, 18, 0, 0, tzinfo=timezone.utc)
        time_end = datetime(2016, 6, 26, 18, 0, 0, tzinfo=timezone.utc)
    else:
        time_start = datetime(2018, 6, 23, 18, 0, 0, tzinfo=timezone.utc)
        time_end = datetime(2018, 6, 24, 18, 0, 0, tzinfo=timezone.utc)
    run_start_time = datetime.utcnow()
    last_loop_time = run_start_time

    while True:
        current_time = datetime.utcnow()
        time_elapsed = current_time - run_start_time
        if demo_mode:
            current_time = (time_start + (time_elapsed * demo_timescale))

        adif_fp = open('aarcfd2016.adi', 'r')
        aarc_adi = ADIReader(adif_fp)
        for qso in aarc_adi:
            print(qso)
        adif_fp.close()
        break
        time.sleep(5)
"""
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


if __name__ == "__main__":
    # execute only if run as a script
    main()