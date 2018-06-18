import pyodbc
import datetime
from hamutils.adif import ADIReader, ADIWriter
from paramiko import SSHClient
from scp import SCPClient

ssh = SSHClient()
ssh.load_system_host_keys()
ssh.connect('gs-s-0.w4uva.org', port=8900, username='qsoviz', key_filename='./qsoviz.id_rsa')
scp = SCPClient(ssh.get_transport())

conn_str = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    r'DBQ=C:\Users\kq9p\Documents\Affirmatech\N3FJP Software\ARRL-Field-Day\LogData.mdb;'
)
sql_query = ('SELECT fldCall,fldDateStr,fldTimeOnStr,fldBand,fldCountryWorked,fldClass,fldInitials,'
             'fldModeContest,fldOperator,fldSection,fldState FROM tblContacts ORDER BY fldTimeOnStr ASC')
cnx = pyodbc.connect(conn_str)
cursor = cnx.cursor()
cursor.execute(sql_query)
rows = cursor.fetchall()
fp = open('./aarcfd.adi', 'wb')
adi = ADIWriter(fp)
for row in rows:
    qso_date = row[1].replace('/','')
    time_on = row[2].replace(':','')
    dt_on = qso_date + time_on
    adi.add_qso(call=row[0], datetime_on=datetime.datetime.strptime(dt_on,'%Y%m%d%H%M%S'), band=row[3], Country=row[4],
                Class=row[5], app_N3FJP_Initials=row[6], app_N3FJP_ModeContest=row[7], mode=row[7], OPERATOR=row[8],
                ARRL_Sect=row[9], State=row[10])
adi.close()
cursor.close()
cnx.close()
scp.put('./aarcfd.adi', 'qsoviz/aarcfd.adi')
scp.close()