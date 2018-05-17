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
from datetime import datetime, timezone, tzinfo, timedelta
import mysql.connector
from hamutils.adif import ADIReader
from hamutils.qrz import Qrz
from influxdb import InfluxDBClient


def main():

    program_name = 'qsoviz'
    program_version = '1.0'
    db_host = 'localhost'
    db_port = 8086
    db_name = 'aarc'
    db_user = 'root'
    qrz_user = 'KQ9P'
    json_qso_location = [
        {
            "measurement": "qso_location",
            "tags": {
                "call": 'Text',
                "operator": "Text"
    },
            "time": "2009-11-10T23:00:00Z",
            "fields": {
                "state": 'Text'
            }
        }
    ]
    json_qso_rate = [
        {
            "measurement": "qso_rate",
            "tags": {
                "call": 'Text',
                "operator": "Text"
            },
            "time": "2009-11-10T23:00:00Z",
            "fields": {
                "rate": 1.0
            }
        }
    ]
    demo_mode = True
    demo_timescale = (24.0 * 60.0) / 2.0

    key_fp = open('passwords.json', "r")
    json_return = json.load(key_fp)
    key_fp.close()
    db_password = json_return['db_password'].encode()
    qrz_password = json_return['qrz_password'].encode()

    if demo_mode:
        time_start = datetime(2016, 6, 25, 18, 0, 0, tzinfo=timezone.utc)
        time_end = datetime(2016, 6, 26, 18, 0, 0, tzinfo=timezone.utc)
    else:
        time_start = datetime(2018, 6, 23, 18, 0, 0, tzinfo=timezone.utc)
        time_end = datetime(2018, 6, 24, 18, 0, 0, tzinfo=timezone.utc)
    run_start_time = datetime.utcnow()
    last_loop_time = run_start_time

    client = InfluxDBClient(db_host, db_port, db_user, db_password, db_name)
    client.drop_database(db_name)
    client.create_database(db_name)
    client.create_retention_policy('default_policy', 'INF', 3, default=True)

    while True:
        current_time = datetime.utcnow()
        time_elapsed = current_time - run_start_time
        if demo_mode:
            current_time = (time_start + (time_elapsed * demo_timescale))

        adif_fp = open('aarcfd2016.adi', 'r')
        aarc_adi = ADIReader(adif_fp)
        aarc_adi_sortable = {}
        for qso in aarc_adi:
            aarc_adi_sortable.update({qso['datetime_on'] : qso})

        rate_counts = {'total' : 0}
        rate_rates = {'total' : 0.0}
        rate_last_datetime = -1
        for key in sorted(aarc_adi_sortable.keys()):
            a_datetime_on = aarc_adi_sortable[key]['datetime_on'].strftime("2018-04-%dT%H:%M:%SZ")
            a_call = aarc_adi_sortable[key]['call'].upper()
            a_n3fjp_modecontest = aarc_adi_sortable[key].get('n3fjp_modecontest', 'NONE').upper()
            a_band = aarc_adi_sortable[key].get('band', 'NONE').upper()
            a_state = aarc_adi_sortable[key].get('state', 'NONE').upper()
            a_arrl_sect =  aarc_adi_sortable[key].get('arrl_sect', 'NONE').upper()
            a_country =  aarc_adi_sortable[key].get('country', 'NONE').upper()
            a_n3fjp_initials =  aarc_adi_sortable[key].get('n3fjp_initials', 'NONE').upper()
            a_operator =  aarc_adi_sortable[key].get('operator', 'NONE').upper()
            a_class =  aarc_adi_sortable[key].get('class', 'NONE').upper()

            if rate_last_datetime == -1:
                rate_last_datetime = aarc_adi_sortable[key]['datetime_on']
            rate_diff_datetime = (aarc_adi_sortable[key]['datetime_on'] - rate_last_datetime)
            if (rate_diff_datetime >= timedelta(minutes=5)):
                for rate_key in rate_counts.keys():
                    if rate_counts[rate_key] == 0:
                        rate_rates.update({rate_key : 0.0})
                    else:
                        rate_rates[rate_key] = ((rate_counts[rate_key] / rate_diff_datetime.seconds) * 3600.0)
                    json_qso_rate[0].update({'time' : a_datetime_on})
                    json_qso_rate[0]['fields'].update({'rate' : rate_rates[rate_key]})
                    json_qso_rate[0]['tags'].update({'call' : a_call})
                    json_qso_rate[0]['tags'].update({'operator' : rate_key})
                    rate_counts.update({rate_key : 0})
                    rate_last_datetime = aarc_adi_sortable[key]['datetime_on']
                    client.write_points(json_qso_rate)
            rate_counts.update({'total' : (rate_counts['total'] + 1)})
            if a_operator in rate_counts:
                rate_counts.update({a_operator : (rate_counts[a_operator] + 1)})
            else:
                rate_counts.update({a_operator : 1})

            json_qso_location[0].update({'time' : a_datetime_on})
            json_qso_location[0]['tags'].update({'call' : a_call})
            json_qso_location[0]['fields'].update({'state' : a_state})
            json_qso_location[0]['tags'].update({'operator' : a_operator})
            client.write_points(json_qso_location)

        adif_fp.close()
        break


if __name__ == "__main__":
    # execute only if run as a script
    main()