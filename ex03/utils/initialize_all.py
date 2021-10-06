#!/usr/bin/env python3
import os
import subprocess as sp
SHELL_RUN = lambda x: sp.run(x, stdout=sp.PIPE, stderr=sp.PIPE, check=True, shell=True)

assert( os.system('which sshpass')==0 )

workers = range(1,12)
_username = 'student'
_password = 'student'

# change ownership of hadoop && spark
hadoop_home = os.environ['HADOOP_HOME']
spark_home  = os.environ['SPARK_HOME']
SHELL_RUN(f'echo {_password} | sudo -S groupadd -f hadoop')
SHELL_RUN(f'echo {_password} | sudo -S usermod -aG hadoop {_username}')
SHELL_RUN(f'echo {_password} | sudo -S chown -R {_username}:hadoop {hadoop_home}')
SHELL_RUN(f'echo {_password} | sudo -S chown -R {_username}:hadoop {spark_home}')
SHELL_RUN(f'echo {_password} | sudo -S chown -R {_username}:hadoop /var/hadoop')

# `ssh-copy-id` for further authentication
os.system('cat /dev/zero | ssh-keygen -q -N "" 2>/dev/null')
_command = f'sshpass -p {_password} ssh-copy-id -o StrictHostKeyChecking=accept-new {_username}@%s'
SHELL_RUN( _command%'0.0.0.0' )
SHELL_RUN( _command%'127.0.0.1' )
SHELL_RUN( _command%'localhost' )
SHELL_RUN( _command%'master' )
for node in workers:
    _hostname = f'worker{node}'
    SHELL_RUN( _command%_hostname )
    print(f'Publich key copied to {_hostname}.')

# hdfs namenode init
os.system('hdfs namenode -format')
for node in workers:
    _hostname = f'worker{node}'
    SHELL_RUN( f'ssh {_username}@{_hostname} "echo {_password}|sudo -S rm -rf /var/hadoop/*"' )
print('HDFS namenode && datanodes initialized.')
