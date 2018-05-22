"""
qsoviz

Convert MySQL database of Field Day contacts to InfluxDB for visualization by Grafana.

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


import json
import time
from datetime import datetime, timezone, timedelta
import mysql.connector
from influxdb import InfluxDBClient


def main():

    program_name = 'qsoviz'
    program_version = '1.0'
    db_host = 'localhost'
    db_port = 8086
    db_name = 'aarc'
    db_user = 'root'
    mysql_query = ('SELECT datetime_on, callsign, n3fjp_modecontest, band, state, country, operator, '
                   + 'latitude, longitude, geohash '
                     'FROM aarc_fd WHERE datetime_on BETWEEN %s AND %s ORDER BY datetime_on ASC')
    json_qso_location = [
        {
            "measurement": "qso_location",
            "tags": {
                "Operator": "Text",
                "Band": "Text",
                "Mode": "Text",
                "State": "Text",
                "geohash": "Text"
            },
            "time": "2009-11-10T23:00:00Z",
            "fields": {
                "Callsign": "Text",
                "Band": "Text",
                "Mode": "Text",
                "State": "Text"
            }
        }
    ]
    json_op_rates = [
        {
            "measurement": "op_rate",
            "tags": {
                "Callsign": "Text",
                "Operator": "Text"
            },
            "time": "2009-11-10T23:00:00Z",
            "fields": {
                "Rate": 1.0,
                "Contacts": 1.0
            }
        }
    ]
    json_mode_rates = [
        {
            "measurement": "mode_rate",
            "tags": {
                "Callsign": "Text",
                "Mode": "Text"
            },
            "time": "2009-11-10T23:00:00Z",
            "fields": {
                "Rate": 1.0
            }
        }
    ]
    json_qso_points = [
        {
            "measurement": "qso_points",
            "tags": {
                "Mode": "Text"
            },
            "time": "2009-11-10T23:00:00Z",
            "fields": {
                "Points": 1.0,
                "Contacts": 1.0
            }
        }
    ]
    demo_mode = True
    demo_timescale = 3600.0

    print(program_name, program_version)

    key_fp = open('passwords.json', "r")
    json_return = json.load(key_fp)
    key_fp.close()
    db_password = json_return['db_password'].encode()

    if demo_mode:
        time_start = datetime(2016, 6, 25, 18, 0, 0, tzinfo=timezone.utc)
        time_end = datetime(2016, 6, 26, 18, 0, 0, tzinfo=timezone.utc)
    else:
        time_start = datetime(2018, 6, 23, 18, 0, 0, tzinfo=timezone.utc)
        time_end = datetime(2018, 6, 24, 18, 0, 0, tzinfo=timezone.utc)
    last_loop_time = datetime.now(tz=timezone.utc)
    time_query_end = time_start

    cnx = mysql.connector.connect(user=db_user, password=db_password, database=db_name)
    cursor = cnx.cursor()

    client = InfluxDBClient(db_host, db_port, db_user, db_password, db_name)
    client.drop_database(db_name)
    client.create_database(db_name)
    client.create_retention_policy('default_policy', 'INF', '3', default=True)

    op_counts = {}
    op_totals = {}
    op_rates = {}
    mode_counts = {'TOTAL': 0, 'PH': 0, 'CW': 0, 'DIG': 0}
    mode_totals = {'TOTAL': 0, 'PH': 0, 'CW': 0, 'DIG': 0}
    mode_points = {'TOTAL': 0, 'PH': 0, 'CW': 0, 'DIG': 0}
    mode_rates = {'TOTAL': 0.0, 'PH': 0.0, 'CW': 0.0, 'DIG': 0.0}
    rate_last_datetime = -1

    while time_query_end < time_end:
        time_clock = datetime.now(tz=timezone.utc)
        time_elapsed = (time_clock - last_loop_time)
        time_query_start = time_query_end
        time_query_end = (time_query_start + (time_elapsed * demo_timescale))
        last_loop_time = time_clock
        mysql_data = (time_query_start, time_query_end)
        cursor.execute(mysql_query, mysql_data)

        for (datetime_on, callsign, n3fjp_modecontest, band, state, country, operator,
             latitude, longitude, geohash) in cursor:
            if rate_last_datetime == -1:
                rate_last_datetime = datetime_on
            rate_diff_datetime = datetime_on - rate_last_datetime

            if rate_diff_datetime >= timedelta(minutes=5):
                for modes in mode_points:
                    json_qso_points[0].update({'time': datetime_on})
                    json_qso_points[0]['fields'].update({'Points': mode_points[modes]})
                    json_qso_points[0]['fields'].update({'Contacts': mode_totals[modes]})
                    json_qso_points[0]['tags'].update({'Mode': modes})
                    client.write_points(json_qso_points)
                for op_key in op_counts.keys():
                    if op_counts[op_key] > 0:
                        op_rates.update({op_key: ((op_counts[op_key] / rate_diff_datetime.seconds) * 3600.0)})
                        json_op_rates[0].update({'time': datetime_on})
                        json_op_rates[0]['fields'].update({'Rate': op_rates[op_key]})
                        json_op_rates[0]['fields'].update({'Contacts': op_totals[op_key]})
                        json_op_rates[0]['tags'].update({'Callsign': callsign})
                        json_op_rates[0]['tags'].update({'Operator': op_key})
                        client.write_points(json_op_rates)
                    op_counts.update({op_key: 0})
                for mode_key in mode_counts.keys():
                    if mode_counts[mode_key] > 0:
                        mode_rates.update({mode_key: ((mode_counts[mode_key] / rate_diff_datetime.seconds) * 3600.0)})
                        json_mode_rates[0].update({'time': datetime_on})
                        json_mode_rates[0]['fields'].update({'Rate': mode_rates[mode_key]})
                        json_mode_rates[0]['tags'].update({'Callsign': callsign})
                        json_mode_rates[0]['tags'].update({'Mode': mode_key})
                        client.write_points(json_mode_rates)
                    mode_counts.update({mode_key: 0})
                rate_last_datetime = datetime_on

            if operator in op_counts:
                op_counts.update({operator: (op_counts[operator] + 1)})
                op_totals.update({operator: (op_totals[operator] + 1)})
            else:
                op_counts.update({operator: 1})
                op_totals.update({operator: 1})
            mode_counts.update({'TOTAL': (mode_counts['TOTAL'] + 1)})
            mode_totals.update({'TOTAL': (mode_totals['TOTAL'] + 1)})
            if n3fjp_modecontest == 'PH':
                mode_counts.update({'PH': (mode_counts['PH'] + 1)})
                mode_totals.update({'PH': (mode_totals['PH'] + 1)})
                mode_points.update({'PH': (mode_points['PH'] + 1)})
            elif n3fjp_modecontest == 'CW':
                mode_counts.update({'CW': (mode_counts['CW'] + 1)})
                mode_totals.update({'CW': (mode_totals['CW'] + 1)})
                mode_points.update({'CW': (mode_points['CW'] + 2)})
            elif n3fjp_modecontest == 'DIG':
                mode_counts.update({'DIG': (mode_counts['DIG'] + 1)})
                mode_totals.update({'DIG': (mode_totals['DIG'] + 1)})
                mode_points.update({'DIG': (mode_points['DIG'] + 2)})
            else:
                print('What mode is this? ', n3fjp_modecontest)
            mode_points.update({'TOTAL': (mode_points['PH'] + mode_points['CW'] + mode_points['DIG'])})

            json_qso_location[0].update({'time': datetime_on})
            json_qso_location[0]['fields'].update({'Callsign': callsign})
            json_qso_location[0]['fields'].update({'State': state})
            json_qso_location[0]['fields'].update({'Band': band})
            json_qso_location[0]['fields'].update({'Mode': n3fjp_modecontest})
            json_qso_location[0]['tags'].update({'Operator': operator})
            json_qso_location[0]['tags'].update({'State': state})
            json_qso_location[0]['tags'].update({'Band': band})
            json_qso_location[0]['tags'].update({'Mode': n3fjp_modecontest})
            json_qso_location[0]['tags'].update({'geohash': geohash})
            client.write_points(json_qso_location)
        time.sleep(2)

    cursor.close()
    cnx.close()


if __name__ == "__main__":
    # execute only if run as a script
    main()
