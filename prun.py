#!/usr/bin/python -O

from __future__ import print_function


import argparse
from functools import partial
import multiprocessing
import sys


parser = argparse.ArgumentParser()
parser.add_argument('-host', action='append',
                    help='remote hosts to run job')
parser.add_argument('command', help='command to run')

args = parser.parse_args()

if args.host is None:
    print('Empty host list')
    sys.exit(1)

if args.command is None:
    print('Empty command')
    sys.exit(2)

hosts = ','.join(args.host).split(',')

def run_cmd(host, cmd):
    print(host)
    os.system(f'rsh {host} "{cmd}"')

pool_size = multiprocessing.cpu_count() * 2

pool = multiprocessing.Pool(processes=pool_size)
pool.map(partial(run_cmd, cmd=args.command), hosts)

pool.close()
pool.join()



