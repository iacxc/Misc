#!/usr/bin/python -O

from __future__ import print_function


import multiprocessing
import os.path
import sys


if __name__ == '__main__':
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option('--host', action='append',
                      help='remote hosts to run job')

    opts, args = parser.parse_args()

    if opts.host is None:
        print('Empty host list')
        sys.exit(1)

    hosts = ','.join(opts.host).split(',')

    if len(args) != 1:
        print('Only one command/script is allowed')
        sys.exit(2)

    script = args[0]

    if os.path.isfile(script):
        os.system('chmod +x %s' % script)

        def command(host):
            print(host)
            os.system('scp %(script)s %(host)s:/root' % {
                'host': host, 'script': script})
            os.system('ssh %(host)s /root/%(script)s' % {
                'host': host, 'script': script})
    else:
        def command(host):
            print(host)
            os.system('ssh %(host)s "%(script)s"' % {
                'host': host, 'script': script})

    pool_size = multiprocessing.cpu_count() * 2

    pool = multiprocessing.Pool(processes=pool_size)
    pool.map(command, hosts)

    pool.close()
    pool.join()



