#!/usr/bin/env python3
import re
import time
import random
import threading
from pathlib import Path
from configparser import ConfigParser
from socketserver import (TCPServer, BaseRequestHandler)

import subprocess as sp
SHELL_RUN = lambda x: sp.run(x, stdout=sp.PIPE, stderr=sp.PIPE, check=True, shell=True)
REMOTE_EXE = lambda x,y: SHELL_RUN(f'ssh -J student@student93 hduser@{x}-x1 "{y}"').stdout.decode()

KEYS = ['student97', 'student101', 'student105', 'student109']
CLUSTER = {
    'student97' : ['group01', 'group02', 'group03', 'group04'],
    'student101': ['group05', 'group06', 'group07', 'group08'],
    'student105': ['group09', 'group10', 'group11', 'group12'],
    'student109': ['group13', 'group14', 'group15', 'group16'],
}
LIMIT  = 15*60 #15min

#
status = ConfigParser()
status_file = Path('~/.booking_status').expanduser()
if status_file.exists():
    status.read( status_file )
else:
    for k in KEYS: status[k]={'name':'', 'timestamp':time.time()}
    with open(status_file,'w') as fd: status.write(fd)
#

def jps_patroller():
    JPS_FILTER = re.compile('(?P<name>\d+) SparkSubmit')
    JPS_PARSER = lambda x: [item.group('name') for item in JPS_FILTER.finditer(x)]

    while True:
        try:
            for node in KEYS:
                _group, _time = status[node]['name'], float(status[node]['timestamp'])
                if _group=='' and time.time()-_time>=60*2.0:
                    _pid = JPS_PARSER( REMOTE_EXE(node,'jps') )
                    if len(_pid) > 0:
                        _pid = ' '.join(_pid)
                        try:
                            REMOTE_EXE(node, f'kill -9 {_pid}')
                        except: pass
                pass
        except Exception as e:
            print(f'jps patroller failure: {e}')
        time.sleep(60)
    pass

def hdfs_patroller():
    WHITE_LIST = ['/terainput20g-32m', '/terainput20g-64m', '/terainput20g-128m', '/terainput20g-256m', '/terainput20g-512m', '/tmp', '/user', '/user/hduser', '/user/hduser/.sparkStaging']
    HDFS_FILTER = re.compile('(.+) (?P<name>\/.*)')
    HDFS_PARSER = lambda x: [item.group('name') for item in HDFS_FILTER.finditer(x)]

    while True:
        try:
            for node in KEYS:
                _group, _time = status[node]['name'], float(status[node]['timestamp'])
                # only cleanup others' temporary files
                if _group!='' and time.time()-_time<=10:
                    _group_folder = '/user/%s'%( _group )
                    _folders = HDFS_PARSER( REMOTE_EXE(node,'/opt/hadoop-2.7.5/bin/hdfs dfs -ls / /user /user/hduser') )
                    _folders = list( filter(lambda x:x not in WHITE_LIST, _folders) )
                    try:
                        _folders.remove(_group_folder)
                    except: pass
                    #
                    _removals = ' '.join(_folders)

                    if len(_removals) > 0:
                        try:
                            REMOTE_EXE(node, f'/opt/hadoop-2.7.5/bin/hdfs dfs -rm -r {_removals}')
                        except: pass
                pass
        except Exception as e:
            print(f'hdfs patroller failure: {e}')
        time.sleep(5)
    pass

def get_status():
    _msg = list()
    for k in KEYS:
        _name, _time = status[k]['name'], float(status[k]['timestamp'])
        _timing = time.time()-_time
        if _timing >= LIMIT:
            _name = ''
            status[k]['name'] = ''
        _msg.append( f'["{k}": {_name}]' )
    return ' '.join(_msg)

class TCPHandler(BaseRequestHandler):
    def handle(self):
        # _group = self.request.recv(1024).strip().decode()
        _tmp = self.request.recv(1024).strip().decode().split()
        if len(_tmp)==2:
            _group, _off_flag = _tmp[0], _tmp[1]=='off'
        else:
            _group, _off_flag = _tmp[0], False
        _cluster = ''

        for x in KEYS:
            if _group in CLUSTER[x]:
                _cluster = x
                break

        try:
            _name, _time = status[_cluster]['name'], float(status[_cluster]['timestamp'])
            _timing = time.time()-_time

            if _off_flag:
                if status[_cluster]['name'] == _group:
                    status[_cluster]['name'] = ''
                    with open(status_file,'w') as fd: status.write(fd)
                    _msg = f'[{_group}] Your booking is off.'
                    _msg += '\n' + get_status()
                    # _msg = 'The booking off function is temporarily disabled.'
                else:
                    _msg = get_status()
            elif _name=='' or _timing >= LIMIT:
                # bookin
                status[_cluster] = {'name':_group, 'timestamp':str(time.time())}
                with open(status_file,'w') as fd: status.write(fd)
                _msg = f'[{_group}] Your booking is on. 15 minutes left.'
                _msg += '\n' + get_status()
            else:
                # wait
                _min = int((LIMIT-_timing) / 60)
                _sec = int((LIMIT-_timing) % 60)
                _msg = f'{_name} is on {_cluster}. Please wait for [{_min} min {_sec} sec].'
                _msg += '\n' + get_status()
                pass
        except Exception as e:
            if _group in ['', 'status']:
                _msg = get_status()
            else:
                _msg = 'Wrong Group Name. Please use "groupXX", e.g., group01.'
        finally:
            print( get_status() )
            _msg = _msg.encode()
            self.request.sendall(_msg)
            time.sleep( 0.5 + random.random() ) #~0.5-second silence
    pass

if __name__ == '__main__':
    threading.Thread(target=hdfs_patroller).start()
    threading.Thread(target=jps_patroller).start()

    with TCPServer( ('0.0.0.0',8301), TCPHandler ) as server:
        server.serve_forever()
