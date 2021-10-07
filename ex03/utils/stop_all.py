#!/usr/bin/env python3
import os
import subprocess as sp
SHELL_RUN = lambda x: sp.run(x, stdout=sp.PIPE, stderr=sp.PIPE, check=True, shell=True)

hadoop_home = os.environ['HADOOP_HOME']
spark_home  = os.environ['SPARK_HOME']

workers = [x.strip() for x in open(f'{hadoop_home}/etc/hadoop/slaves').readlines()]
_username = 'hduser'
_password = 'student'

# stop YARN
print('Hadoop YARN stopping ...')
os.system( 'stop-yarn.sh' )

# start HDFS
print('Hadoop HDFS stopping ...')
os.system( 'stop-dfs.sh' )

# start history servers
print('History Servers stopping ...')
os.system( 'mr-jobhistory-daemon.sh stop historyserver' )
os.system( 'stop-history-server.sh' )
