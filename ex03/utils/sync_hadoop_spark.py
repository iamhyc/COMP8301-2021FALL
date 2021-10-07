#!/usr/bin/env python3
import os
import subprocess as sp
SHELL_RUN = lambda x: sp.run(x, stdout=sp.PIPE, stderr=sp.PIPE, check=True, shell=True)

hadoop_folder = os.environ['HADOOP_HOME']
spark_folder  = os.environ['SPARK_HOME']

workers = [x.strip() for x in open(f'{hadoop_folder}/etc/hadoop/slaves').readlines()]
_username = 'hduser'
_password = 'student'

for _hostname in workers:
    # change the folder's owner firstly
    SHELL_RUN(f'ssh {_username}@{_hostname} "echo {_password}|sudo -S groupadd -f hadoop && echo {_password}|sudo -S usermod -aG hadoop {_username}"')
    SHELL_RUN(f'ssh {_username}@{_hostname} "echo {_password}|sudo -S chown -R {_username}:hadoop {hadoop_folder} && echo {_password}|sudo -S chown -R {_username}:hadoop {spark_folder} && echo {_password}|sudo -S chown -R {_username}:hadoop /var/hadoop"')
    # scp the hadoop configs
    _folder = f'{hadoop_folder}/etc/hadoop'
    SHELL_RUN(f'ssh {_username}@{_hostname} "rm -rf {_folder}"')
    SHELL_RUN(f'scp -r {_folder} {_username}@{_hostname}:{_folder}')
    # scp the spark configs
    _folder = f'{spark_folder}/conf'
    SHELL_RUN(f'ssh {_username}@{_hostname} "rm -rf {_folder}"')
    SHELL_RUN(f'scp -r {_folder} {_username}@{_hostname}:{_folder}')
    # scp the hosts file
    #SHELL_RUN(f'scp /etc/hosts {_username}@{_hostname}:/tmp/hosts')
    #SHELL_RUN(f'ssh {_username}@{_hostname} "echo {_password} | sudo -S cp -f /tmp/hosts /etc/hosts"')
    print(f'Hadoop && Spark dispatched to {_hostname}.')
