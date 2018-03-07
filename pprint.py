from __future__ import print_function

import math


def pretty_print(s, a):
    if a == 0:
        print('Error parameter, a cannot be ZERO')
        return

    maxl = int(max(math.log(n, 10) + 1 for n in s))
    ph = '-' * maxl
    l0 = 0
    while s:
        l = min(len(s), a)
        print('+{}+'.format('+'.join([ph] * max(l, l0))))
        formatstr = '{{:>{}d}}'.format(maxl)
        print('|{}|'.format('|'.join(formatstr.format(n) for n in s[:a])))
        s = s[a:]
        l0 = l

    print('+{}+'.format('+'.join([ph] * l0)))
    print()

s = [234,324,324,34,344535,2,3]
pretty_print(s, 0)
pretty_print(s, 1)
pretty_print(s, 2)
pretty_print(s, 3)
pretty_print(s, 4)
pretty_print(s, 5)
pretty_print(s, 6)
pretty_print(s, 7)
pretty_print(s, 8)