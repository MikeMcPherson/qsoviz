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


import configparser
import logging
import json
import time
import iso8601
from datetime import datetime, timezone, timedelta
import mysql.connector
from influxdb import InfluxDBClient


def main():

    program_name = 'qsoviz'
    program_version = '1.2'

    mysql_query = ('SELECT datetime_on, callsign, n3fjp_modecontest, band, state, country, operator, computer_name,'
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
    json_station_info = [
        {
            "measurement": "station_info",
            "tags": {
                "Station": "Text",
                "Operator": "Text",
                "Band": "Text",
                "Mode": "Text"
            },
            "time": "2009-11-10T23:00:00Z",
            "fields": {
                "Station": "Text",
                "Operator": "Text",
                "Band": "Text",
                "Mode": "Text"
            }
        }
    ]

    config = configparser.ConfigParser()
    config.read(['qsoviz.ini', 'qrz.ini'])
    debug = config['general'].getboolean('debug')
    reset_influxdb_db = config['general'].getboolean('reset_influxdb_db')
    db_host = config['mysql']['db_host']
    db_port = config['mysql']['db_port']
    db_user = config['mysql']['db_user']
    db_password = config['mysql']['db_password']
    db_name = config['mysql']['db_name']

    logging.basicConfig(filename='/home/qsoviz/qsoviz.log', level=logging.INFO, format='%(asctime)s %(message)s')
    logging.info('%s %s: Run started', program_name, program_version)

    time_start = datetime(2018, 6, 23, 18, 0, 0, tzinfo=timezone.utc)
    time_end = datetime(2018, 6, 24, 18, 0, 0, tzinfo=timezone.utc)

    cnx = mysql.connector.connect(user=db_user, password=db_password, database=db_name)
    cursor = cnx.cursor()

    client = InfluxDBClient(db_host, db_port, db_user, db_password, db_name)
    if reset_influxdb_db:
        client.drop_database(db_name)
        client.create_database(db_name)
        client.create_retention_policy('default_policy', 'INF', '3', default=True)

    try:
        influxdb_query = 'SELECT LAST("Callsign"),time FROM "qso_location"'
        influxdb_last_qso = client.query(influxdb_query)
        influxdb_last_time = list(influxdb_last_qso.get_points())[0]['time']
        influxdb_last_time = iso8601.parse_date(influxdb_last_time)
    except:
        influxdb_last_time = time_start

    op_counts = {}
    op_totals = {}
    op_rates = {}
    mode_counts = {'TOTAL': 0, 'PH': 0, 'CW': 0, 'DIG': 0}
    mode_totals = {'TOTAL': 0, 'PH': 0, 'CW': 0, 'DIG': 0}
    mode_rates = {'TOTAL': 0.0, 'PH': 0.0, 'CW': 0.0, 'DIG': 0.0}
    mode_points = {'TOTAL': 0, 'PH': 0, 'CW': 0, 'DIG': 0}
    rate_last_datetime = -1

    station_last = [['', '', ''], ['', '', ''], ['', '', '']]

    records_added = 0
    records_added_flag = False

    mysql_data = (time_start, time_end)
    cursor.execute(mysql_query, mysql_data)

    for (datetime_on, callsign, n3fjp_modecontest, band, state, country, operator, computer_name,
         latitude, longitude, geohash) in cursor:
        if rate_last_datetime == -1:
            rate_last_datetime = datetime_on
        rate_diff_datetime = datetime_on - rate_last_datetime
        datetime_on_aware = datetime_on.replace(tzinfo=timezone.utc)

        for ig_op in ignore_operators:
            if operator == ig_op:
                operator = 'UNK'

        if rate_diff_datetime >= timedelta(minutes=5):
            for mode_key in mode_points.keys():
                json_qso_points[0].update({'time': datetime_on})
                json_qso_points[0]['fields'].update({'Points': mode_points[mode_key]})
                json_qso_points[0]['fields'].update({'Contacts': mode_totals[mode_key]})
                json_qso_points[0]['tags'].update({'Mode': mode_key})
                if datetime_on_aware >= influxdb_last_time:
                    records_added_flag = True
                    client.write_points(json_qso_points)
            for op_key in op_counts.keys():
                if op_counts[op_key] > 0:
                    op_rates.update({op_key: ((op_counts[op_key] / rate_diff_datetime.seconds) * 3600.0)})
                    json_op_rates[0].update({'time': datetime_on})
                    json_op_rates[0]['fields'].update({'Rate': op_rates[op_key]})
                    json_op_rates[0]['fields'].update({'Contacts': op_totals[op_key]})
                    json_op_rates[0]['tags'].update({'Callsign': callsign})
                    json_op_rates[0]['tags'].update({'Operator': op_key})
                    if datetime_on_aware >= influxdb_last_time:
                        records_added_flag = True
                        client.write_points(json_op_rates)
                op_counts.update({op_key: 0})

            for mode_key in mode_counts.keys():
                if mode_counts[mode_key] > 0:
                    mode_rates.update({mode_key: ((mode_counts[mode_key] / rate_diff_datetime.seconds) * 3600.0)})
                    json_mode_rates[0].update({'time': datetime_on})
                    json_mode_rates[0]['fields'].update({'Rate': mode_rates[mode_key]})
                    json_mode_rates[0]['tags'].update({'Callsign': callsign})
                    json_mode_rates[0]['tags'].update({'Mode': mode_key})
                    if datetime_on_aware >= influxdb_last_time:
                        records_added_flag = True
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
        if datetime_on_aware >= influxdb_last_time:
            records_added_flag = True
            client.write_points(json_qso_location)

        if computer_name == 'CP0':
            station_last[0][0] = operator
            station_last[0][1] = n3fjp_modecontest
            station_last[0][2] = band
        elif computer_name == 'TENTEC':
            station_last[1][0] = operator
            station_last[1][1] = n3fjp_modecontest
            station_last[1][2] = band
        elif computer_name == 'YAESU':
            station_last[2][0] = operator
            station_last[2][1] = n3fjp_modecontest
            station_last[2][2] = band
        else:
            pass

        for idx, s in enumerate(station_last):
            json_station_info[0].update({'time': datetime_on})
            json_station_info[0]['tags'].update({'Station': stations[idx]})
            json_station_info[0]['tags'].update({'Operator': s[0]})
            json_station_info[0]['tags'].update({'Band': s[1]})
            json_station_info[0]['tags'].update({'Mode': s[2]})
            json_station_info[0]['fields'].update({'Station': stations[idx]})
            json_station_info[0]['fields'].update({'Operator': s[0]})
            json_station_info[0]['fields'].update({'Band': s[1]})
            json_station_info[0]['fields'].update({'Mode': s[2]})
            if datetime_on_aware >= influxdb_last_time:
                records_added_flag = True
                client.write_points(json_station_info)

        if records_added_flag:
            records_added = records_added + 1

    logging.info('%s %s: records_added = %s', program_name, program_version, records_added)
    logging.info('%s %s: Run ended', program_name, program_version)

    cursor.close()
    cnx.close()


if __name__ == "__main__":
    # execute only if run as a script
    main()
