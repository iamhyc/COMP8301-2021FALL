#!/usr/bin/env python3
import re, os, sys
import itertools
import subprocess as sp
import json

_header = '''
127.0.0.1       localhost
127.0.1.1       %s

# The following lines are desirable for IPv6 capable hosts
::1     localhost ip6-localhost ip6-loopback
fe00::0 ip6-localnet
fe00::0 ip6-mcastprefix
fe00::1 ip6-allnodes
fe00::2 ip6-allrouters
'''
_body = '''\
{x1} {hostname}-x1 {x1a}
{x2} {hostname}-x2 {x2a}
{x3} {hostname}-x3 {x3a}
'''
_filter = re.compile('(\d+.\d+.\d+.\d+) (student\d+)-(.*)') #e.g., "10.244.40.4 student96-x2"
_adminpass = 'comp7305'
_username = 'student'
_userpass = 'student'

SHELL_RUN = lambda x: sp.run(x, stdout=sp.PIPE, stderr=sp.PIPE, check=True, shell=True)
CMD = lambda _cmd,_pass: f'sshpass -p {_pass} {_cmd} -oStrictHostKeyChecking=accept-new'

# load host_file into host_map
host_map = dict()
host_file = open('../hosts').readlines()
for item in host_file:
    _result = _filter.findall(item)
    if len(_result)==1:
        _ip, _name, _id = _result[0]
        if _name in host_map:
            host_map[_name][_id] = _ip
        else:
            host_map.update( {_name: {_id:_ip}} )

#===========================================================
if sys.argv[1]=='ex02':
    # proceed?
    print( json.dumps(host_map, indent=2) )
    if input('Proceed? (y/N)') not in ['Y', 'y']:
        exit(0)
    # upload the hosts file
    for name,item in host_map.items():
        if name in ['student93','student94','student95','student96']:
            _hosts = _header%name + _body.format(hostname=name,
                        x1=item['x1'], x2=item['x2'], x3=item['x3'], x1a='', x2a='', x3a='')
            SHELL_RUN( f'echo "{_hosts}" | {CMD("ssh",_adminpass)} ta@{name} "cat > /tmp/hosts && echo {_adminpass}|sudo -S cp /tmp/hosts /etc/hosts"' )
            print(f'"hosts" file updated on {name}.')
    pass
elif sys.argv[1]=='ex03':
    _masters = [93] #[97, 101, 105, 109]
    # assign master-slave tuples
    master_hosts = dict()
    for node in _masters:
        _file = _header
        _nodes = [f'student{node+i}' for i in range(4)]
        # generate hosts file body
        for i,_name in enumerate(_nodes):
            _item = host_map[_name]
            _x1a, _x2a, _x3a = ('master' if i==0 else f'worker{3*i+0}',
				f'worker{3*i+1}', f'worker{3*i+2}')
            _file += _body.format(hostname=_name,
                        x1=_item['x1'], x2=_item['x2'], x3=_item['x3'], x1a=_x1a, x2a=_x2a, x3a=_x3a)
        # fill-in hosts file header
        _workers = [[f'{x}-x1', f'{x}-x2', f'{x}-x3'] for x in _nodes]
        _workers = list( itertools.chain.from_iterable(_workers) )
        master_hosts.update( {_nodes[0]:[_workers,_file]} )
        pass
    # proceed?
    [print('%s: %s%s'%(k,v[0],v[1])) for k,v in master_hosts.items()]
    if input('Proceed? (y/N)') not in ['y', 'Y']:
        exit(0)
    # update utils and hosts
    for _master,(_nodes,_hosts) in master_hosts.items():
        # upload the hosts file
        hosts = _hosts%_master
        SHELL_RUN( f'echo "{hosts}" | {CMD("ssh",_adminpass)} ta@{_master} -T "cat > /tmp/hosts && echo {_adminpass}|sudo -S mv /tmp/hosts /etc/hosts"' )
        for _node in _nodes:
            hosts = _hosts%_node
            SHELL_RUN( f'echo "{hosts}" | {CMD("ssh",_userpass)} -oProxyCommand="{CMD("ssh",_userpass)} -W %h:%p {_username}@{_master}" {_username}@{_node} -T "cat > /tmp/hosts && echo {_userpass}|sudo -S cp /tmp/hosts /etc/hosts"' )
        print(f'"hosts" file updated for {_nodes}.')
        # upload utils for master container
        SHELL_RUN( f'{CMD("scp",_userpass)} -oProxyCommand="{CMD("ssh",_userpass)} -W %h:%p {_username}@{_master}" -r ./utils ./terasort {_username}@{_nodes[0]}:~/' )
        print(f'"utils" && "terasort" uploaded for master container: {_nodes[0]}.')
    pass
else:
    print('"ex02" or "ex03"')
