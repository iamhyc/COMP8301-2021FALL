#!/usr/bin/env python3
import os
import subprocess as sp
SHELL_RUN = lambda x: sp.run(x, stdout=sp.PIPE, stderr=sp.PIPE, check=True, shell=True)

hadoop_home = os.environ['HADOOP_HOME']
spark_home  = os.environ['SPARK_HOME']

workers = [x.strip() for x in open(f'{hadoop_home}/etc/hadoop/slaves').readlines()]
_username = 'hduser'
_password = 'student'

# cleanup hadoop logs
SHELL_RUN(f'rm -rf {hadoop_home}/logs')
for _hostname in workers:
    SHELL_RUN(f'ssh {_username}@{_hostname} "rm -rf {hadoop_home}/logs"')
