#!/usr/bin/python                                                                            

import sys, getopt

from collections import defaultdict

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel

HOST_NAMES = []
HOST_IPS = []
HOST_IP_PREFIX = '192.168.128.'
IP_CIDR = '/24'

SWITCHES = ['s1', 's2', 's3', 's4']

def generate_host_infos(num):
    global HOST_NAMES
    global HOST_IPS
    for h in range(num):
        HOST_NAMES.append('{}{}'.format('h', h+1))
        HOST_IPS.append('{}{}'.format(HOST_IP_PREFIX, (h+1)))

class MyTopo(Topo):
    def build(self):
        # Init Switches ###########################
        for name in SWITCHES:
            self.addSwitch(name)
        # Set links between switches
        self.addLink(SWITCHES[0], SWITCHES[1])  # TODO: loss and delay !
        self.addLink(SWITCHES[0], SWITCHES[2])  # delay='250ms', loss=5
        self.addLink(SWITCHES[0], SWITCHES[3])
        ###########################################
        # Init Hosts
        switch_idx = 0
        for idx in range(len(HOST_NAMES)):
            host = self.addHost(HOST_NAMES[idx], ip='{}{}'.format(HOST_IPS[idx], IP_CIDR))
            self.addLink(SWITCHES[switch_idx], host)
            switch_idx += 1
            if switch_idx == 4:
                switch_idx = 0

def print_help():
    print 'run.py -n <num_nodes> -w <num_workers> -o <num_operations>'
    sys.exit(2)

def main(argv):
    num_nodes = 0
    num_workers = 0
    num_ops = 0
    try:
        opts, args = getopt.getopt(argv,"hn:w:o:",["nodes=,workers=","ops="])
    except getopt.GetoptError:
        print_help()
    for opt, arg in opts:
        if opt == '-h':
            print_help()
        elif opt in ("-n", "--nodes"):
            num_nodes = int(arg)
        elif opt in ("-w", "--workers"):
            num_workers = int(arg)
        elif opt in ("-o", "--ops"):
            num_ops = int(arg)
    if num_ops == 0 or num_workers == 0 or num_workers == 0:
        print_help()
    generate_host_infos(num_nodes)
    topo = MyTopo()
    net = Mininet(topo, link=TCLink)
    net.start()
    #dumpNodeConnections(net.hosts)
    #net.pingAll()
    # Put all ips into one string
    ips = ""
    for ip in HOST_IPS:
      ips += (ip + " ")
    ips[:-1] # remove last empty char
    # Start benchmark
    command = '{} {} {} {} {}'.format('./crdt_bench', num_nodes, num_workers, num_ops, ips)
    for node in HOST_NAMES:
        net.get(node).sendCmd('{} {}'.format(command, node))
    for node in HOST_NAMES:
        print net.get(node).waitOutput()
    net.stop()

if __name__ == '__main__':
    main(sys.argv[1:])

