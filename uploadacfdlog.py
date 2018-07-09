import datetime
import paramiko
from scp import SCPClient
from pathlib import Path


def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""


def main():
    run_time = datetime.datetime.utcnow().isoformat(timespec='seconds') + 'Z'
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
    ssh.load_system_host_keys()
    ssh.connect('gs-s-0.w4uva.org', port=8900, username='qsoviz', key_filename='./qsoviz.id_rsa')
    scp = SCPClient(ssh.get_transport())
    srcfile = Path('C:/Users/AARC/Documents/Affirmatech/N3FJP Software/ARRL-Field-Day/LogData.mdb')
    destfile = 'logfiles/aarcfd' + run_time + '.mdb'
    scp.put(srcfile, destfile)
    scp.close()


main()
