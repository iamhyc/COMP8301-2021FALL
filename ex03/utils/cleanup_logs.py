#!/usr/bin/env python3
import os
import subprocess as sp
SHELL_RUN = lambda x: sp.run(x, stdout=sp.PIPE, stderr=sp.PIPE, check=True, shell=True)

workers = range(1,12)
_username = 'student'
_password = 'student'

hadoop_home = os.environ['HADOOP_HOME']
spark_home  = os.environ['SPARK_HOME']

# cleanup hadoop logs
SHELL_RUN(f'rm -rf {hadoop_home}/logs')
for node in workers:
    _hostname = f'worker{node}'
    SHELL_RUN(f'ssh {_username}@{_hostname} "rm -rf {hadoop_home}/logs"')
