"""
Installs Zookeeper on Ubuntu 16.04
"""

import os
import wget

zk_config = {
    'tickTime' : 2000,
    'dataDir' : '/var/lib/zookeeper/',
    'clientPort' : 2181,
    'initLimit' : 5,
    'syncLimit' : 2,
    'server' : [''] # remove this value if its a single instance
}

zk_config_file = [] # will be used as the actual configuration file for
zk_folder = 'zookeeper-3.3.6'
zk_tar = '{}.tar.gz'.format(zk_folder)
zk_download_url = 'http://mirror.metrocast.net/apache/zookeeper/{}/{}'.format(zk_folder, zk_tar)
ezstack_dir = '/opt/ezstack'

cmd = os.system
cmd('mkdir -p {}'.format(ezstack_dir))
wget.download(zk_download_url,
              out='{}/{}'.format(ezstack_dir, zk_tar))
cmd('tar xvzf {}/{} -C {}/'.format(ezstack_dir, zk_tar, ezstack_dir))

for key, value in zk_config.iteritems():
    if key == 'server':
        i = 1
        for host in key:
            zk_config_file.append('server.{}={}'.format(i, host))
            i += 1
    else:
        zk_config_file.append('{}={}'.format(key, value))

with open('{}/{}/conf/zoo.cfg'.format(ezstack_dir, zk_folder), 'w') as f:
    f.writelines(zk_config_file)

if 'server' in zk_config.keys():
    myid = int(raw_input('You Indicated {} node setup.\n'
                         'Enter Current Node Number> '.format(len(zk_config['server']))))
    if myid > len(zk_config['server']) or myid < 1:
        print 'Invalid number please correct value in {}/myid'.format(zk_config['dataDir'])

    cmd('mkdir -p {}/'.format(zk_config['dataDir']))
    with open('{}/myid'.format(zk_config['dataDir']), 'w') as f:
        f.write(str(myid))
