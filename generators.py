from __future__ import print_function

import fnmatch
import os
import re
import time
import sys


def find(filepat, top):
    for path, dirlist, filelist in os.walk(top):
        for name in fnmatch.filter(filelist, filepat):
            yield os.path.join(path, name)


def opener(filenames):
    for name in filenames:
        if name.endswith('.gz'):
            yield gzip.open(name)
        elif name.endswith('.bz2'):
            yield bz2.BZ2File(name)
        else:
            yield open(name)


def cat(sources):
    for s in sources:
        for item in s:
            yield item


def grep(pat, lines):
    patc = re.compile(pat)
    for line in lines:
        if patc.search(line):
            yield line


def follow(thefile):
    # simulate the 'tail -f' action
    thefile.seek(0, 2) # Go to the end of the file
    while True:
        line = thefile.readline()
        if not line:
            time.sleep(0.1)
            continue
        yield line


if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise SystemError('Missing directory name')

    filenames = find('*.py', sys.argv[1])
    files = opener(filenames)
    lines = cat(files)
    outputs = grep(r'^import', lines)
    for line in outputs:
        print(line, end='')
