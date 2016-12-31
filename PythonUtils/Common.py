"""
   provide some common utilities

"""

# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2013-2015 Hewlett-Packard Development Company, L.P.
#
# @@@ END COPYRIGHT @@@


__all__ = ( 'check_module',
            'run_cmd',
            'make_enum',
            'encode_int',
            'decode_int',
            'time_it',
            'which',
            'get_input',
            'gcd',
            'lcm',
            'get_script_path',
            'get_hostid',
            'md5sum')


import base64
from datetime import datetime
import hashlib
import os
import os.path
import struct
from subprocess import Popen, PIPE
import sys
import threading


#constants
SUCCESS = 0
FAILURE = 1

E_TIMEOUT = -124
E_NONEXE  = -126
E_NONEXT  = -127


def check_module(module, version=None):
    """ check whether a module is installed,
       (optional) and with the required version
    """
    try:
        m = __import__(module)
        if version is not None:
            try:
                v = getattr(m, 'version', getattr(m, '__version__'))
                if v < str(version):
                    return False
                else:
                    return True

            except AttributeError:
                return False
        else:
            return True

    except ImportError:
        return False


class Command(object):
    """
       a class to implement running a command with timeout
    """
    cmdstr = None
    process = None
    status = None
    output, error = '', ''

    def __init__(self, cmdstr):
        self.cmdstr = cmdstr

    def run(self, timeout=None, test=False):
        """ Run a command then return: (status, output + error). """
        def target():
            """ a closure which run the command"""
            try:
                if __debug__ or test:
                    print self.cmdstr

                if test:
                    self.status, self.output = SUCCESS, ''
                else:
                    self.process = Popen(self.cmdstr, shell=True,
                                                  stdout=PIPE, stderr=PIPE)
                    self.output, self.error = self.process.communicate()
                    self.status = self.process.returncode
            except Exception as exp:
                self.status = exp.errno
                self.error = exp.strerror

        thread = threading.Thread(target=target)
        thread.start()
        thread.join(timeout)

        if thread.is_alive():
            self.process.terminate()
            thread.join()
            self.status = E_TIMEOUT
            self.error = "TIMEOUT"

        return self.status, self.output


def run_cmd(cmdstr, timeout=None, test=False):
    """
       a wrapper to call Command.run, it performs some checks before invoke
       the command
    """
    cmd = cmdstr.split(' ')[0]
    if cmd.startswith('/'):
        if not os.path.exists(cmd):
            return E_NONEXT, '%s: NO SUCH FILE OR DIRECTORY' % cmd

        elif not os.access(cmd, os.X_OK):
            return E_NONEXE, '%s: PERMISSION DENIED' % cmd

    command = Command(cmdstr)
    status, output = command.run(timeout, test)

    return status, output


def make_enum(enum_type='Enum', base_classes=None, methods=None, **attrs):
    """
    Generates a enumeration with the given attributes.
    """
    # Enumerations can not be initalized as a new instance
    def __init__(_instance, *_args, **_kws):
        raise RuntimeError('%s types can not be initialized.' % enum_type)

    if base_classes is None:
        base_classes = ()

    if methods is None:
        methods = {}

    base_classes = base_classes + (object,)
    for key, val in methods.items():
        methods[key] = classmethod(val)

    attrs['enums'] = attrs.copy()
    methods.update(attrs)
    methods['__init__'] = __init__
    return type(enum_type, base_classes, methods)


def encode_int(n):
    """
       encode an integer to a string using base64
    """
    data = struct.pack('i', n)
    return base64.b64encode(data)


def decode_int(s):
    """
       decode a string to an integer using base64
    """
    data = base64.b64decode(s)
    return struct.unpack('i', data)[0]


def time_it(title_, sub, *args, **kws):
    """Calling a function and show the start time, end time
       and elapsed time for running it"""

    def format_time (time_):
        """format a time value"""
        return time_.strftime('%Y-%m-%d %H:%M:%S.%f')

    start_t = datetime.now()

    ret = sub(*args, **kws)

    end_t = datetime.now()

    delta = end_t - start_t

    if __debug__:
        print '{0} start at {1}, end at {2}'.format(
               title_, format_time(start_t), format_time(end_t))

        print '{0} elapsed: {1:.4f} seconds'.format(
               title_, delta.seconds + 1e-6 * delta.microseconds)

    return (delta, ret)


def which(cmd):
    """ simulate the which shell command """
    for path in os.getenv('PATH').split(":"):
        full_path = os.path.join(path, cmd)
        if os.path.exists(full_path) and os.access(full_path, os.X_OK):
            return full_path

    return None


def get_hostid(buf=[]):
    """ get the host id by running /usr/bin/hostid, and buffer it """
    if len(buf) == 0:
        retval, out = run_cmd('/usr/bin/hostid')
        if retval == SUCCESS:
            buf.append(int(out.split('\n')[0], 16))
        else:
           buf.append(0)

    return buf[0]


def get_input(prompt, hint=None, default=None, convert=None):
    """ get input from stdin """
    if hint:
        prompt += " (" + hint
        if default:
            prompt += ", [%s]" % default

        prompt += ")"

    answer = raw_input(prompt + ": ")

    if answer.strip() == "": answer = default

    return answer if convert is None else convert(answer)


def md5sum(fname):
    def sumfile(fobj):
        m = hashlib.new('md5')

        while True:
            d = fobj.read(8096)
            if not d:
                break

            m.update(d)

        return m.hexdigest()

    with file(fname, 'rb') as f:
        return sumfile(f)


def get_script_path():
    """ get the script's path """
    path = sys.path[0]
    if os.path.isdir(path):
        return path
    elif os.path.isfile(path):
        return os.path.dirname(path)


def gcd(m, n, *args):
    """ get the greatest common divisor """
    if m < n: 
        return gcd(n, m, *args)

    while m % n != 0:
        m, n = n, m % n

    return n if len(args) == 0 else gcd(n, *args)


def lcm(m, n, *args):
    """ get the lowest common multiple """
    g = gcd(m, n)
    l = m * n / g

    return l if len(args) == 0 else lcm(l, *args)
