EZStack Agent
===

The following folder contains automation scripts to assist in environment deployment.

### Zookeeper
#### Install
- Prereqs are Ubuntu 16.04 and Java 8
```
$ sudo python2.7 zk_installer.py -config /path/to/config.json
..
100% [...................................................] 11833706 / 11833706
You Indicated 3 node setup.
Enter Current Node Number> 1
```

**The current node number should match the server id specified in the configuration file.**

#### Uninstall
```
$ sudo python2.7 zk_installer.py -u
```

### ezstack_configs
`ezstack_config` folder is ezstack specific environment configurations.