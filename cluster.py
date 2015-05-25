#!/usr/bin/env python

import Queue
import argparse
import json
import subprocess as sp
import sys
import threading
import time

import blessings

term = blessings.Terminal()

BUILD_SERVER = 'ec2-52-8-74-150.us-west-1.compute.amazonaws.com'

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-b', '--build', default=False, action='store_true',
            help='Build source code and redistribute server binary.')
    parser.add_argument('-d', '--distrib', default=False, action='store_true',
            help='Distribute the current server binary without rebuilding.')
    parser.add_argument('-c', '--clear', default=False, action='store_true',
            help='Delete persistence engine files on each server.')

    parser.add_argument('-L', '--launch', default=False, action='store_true',
            help='Start each remote server.')
    parser.add_argument('-K', '--killall', default=False, action='store_true',
            help='Kill each remote server.')

    parser.add_argument('-v', '--verbose', default=False, action='store_true',
            help=('When used with --launch, keep ssh sessions alive and display'
                'remote process output'))

    parser.add_argument('config', default=['config.json'], type=str, nargs='?',
            help=('Configuration file used to identify hosts for distribution and '
                'given to each host.'))

    args = parser.parse_args()

    config = json.load(open(args.config[0], 'r'))

    if args.build:
        build()

    if args.killall:
        killall(config)

    if args.clear:
        clearall(config)

    if args.build or args.distrib:
        distrib(config, args.config[0])

    if args.launch:
        launchall(config, args.config[0])


def build():
    print term.yellow('building slug')
    sp.call(['tar', 'czf', 'slug.tar', 'Makefile', 'src'])

    print term.yellow('sending slug to build server'), BUILD_SERVER
    sp.call(['scp', 'build.sh', 'slug.tar', BUILD_SERVER+':~'])

    print term.yellow('remotely building server binary')
    sp.call(['ssh', BUILD_SERVER, 'sh', 'build.sh'])

    print term.yellow('retrieving server binary')
    sp.call(['scp', BUILD_SERVER+':~/bin/server', 'bin/xstream-server'])


def clearall(config):
    print term.yellow(
            'clearing database and index on %s nodes' % len(config['Hosts']))
    for hostname in config['Hosts']:
        host, port = hostname.split(':')
        sp.call(['ssh', host, 'rm', '-f', 'pe*.db{,-index}', '*.log'])


def distrib(config, config_filename):
    print term.yellow('distributing server binary')
    for hostname in config['Hosts']:
        host, port = hostname.split(':')
        print term.yellow('to host'), host
        sp.call(['scp', 'bin/xstream-server', config_filename,
                '%s:~' % host])


def killall(config):
    print term.yellow('killing %s nodes' % len(config['Hosts']))
    for hostname in config['Hosts']:
        host, port = hostname.split(':')
        print term.red('kill'), host
        sp.call(['ssh', host, "pkill", "-15", "xstream-server"])


def launchall(config, config_filename):
    print term.yellow('launching %s nodes' % len(config['Hosts']))
    with open('/dev/null', 'w') as fnull:
        for hostname in config['Hosts']:
            host, port = hostname.split(':')
            print term.blue('launch'), host+':'+port
            sp.call(
                ['ssh', '-f', host, "./xstream-server", config_filename, port, "testgraph", ">", "%s.log" % port, "2>&1"],
                stdout=fnull, stderr=fnull)


if __name__ == '__main__':
    main()
