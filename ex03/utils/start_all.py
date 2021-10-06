#!/usr/bin/env python3
import os
import subprocess as sp
SHELL_RUN = lambda x: sp.run(x, stdout=sp.PIPE, stderr=sp.PIPE, check=True, shell=True)

workers = [f'worker{i}' for i in range(1,12)]
_username = 'student'
_password = 'student'

hadoop_home = os.environ['HADOOP_HOME']
spark_home  = os.environ['SPARK_HOME']

# start HDFS
print('Hadoop HDFS starting ...')
os.system( 'start-dfs.sh' )
assert( SHELL_RUN('hdfs dfsadmin -report | grep Name | wc -l').stdout == b'11\n' )

# start YARN
print('Hadoop YARN starting ...')
os.system( 'start-yarn.sh' )
assert( b'ResourceManager' in SHELL_RUN('jps | grep ResourceManager').stdout  )
for node in workers:
    assert( b'NodeManager' in SHELL_RUN(f'ssh {_username}@{node} "jps|grep NodeManager"').stdout )

# create spark dir
SHELL_RUN( 'hdfs dfs -mkdir -p /tmp/sparkLog' )
SHELL_RUN( 'hdfs dfs -chmod -R 777 /tmp/sparkLog' )

