
from __future__ import print_function

import sys

from Common import time_it, md5sum

for f in sys.argv[1:]:
    elapsed, md5 = time_it('md5sum', md5sum, f)
    print(md5, f, elapsed)