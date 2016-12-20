
from __future__ import print_function

import sys

from Common import md5sum

for f in sys.argv[1:]:
    print(md5sum(f), f)