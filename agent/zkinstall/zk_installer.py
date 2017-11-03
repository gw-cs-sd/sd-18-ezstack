"""
Installs Zookeeper on Ubuntu 16.04
"""

import os
import wget

zk_file = 'zookeeper-3.3.6.tar.gz'
ezstack_dir = '/opt/ezstack'

cmd = os.system
cmd('mkdir -p {}'.format(ezstack_dir))
wget.download('http://mirror.metrocast.net/apache/zookeeper/zookeeper-3.3.6/{}'.format(zk_file),
              out='{}/{}'.format(ezstack_dir, zk_file))
cmd('tar xvzf {}/{}'.format(ezstack_dir, zk_file))

