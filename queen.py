#!/usr/bin/python -O

from __future__ import print_function

import sys


def print_queen(pos):
    for col in pos:
        print("| " * col + "|X|" + " |" * (len(pos)-col-1))

    print()


def find_queen(start_row, pos):
    if start_row == len(pos):
        yield pos

    for col,_ in enumerate(pos):
        good_pos = True
        for row in range(start_row):
            if pos[row] in (col, col - start_row + row, col + start_row - row):
                good_pos = False
                break

        if good_pos:
            pos[start_row] = col
            for p in find_queen(start_row+1, pos):
                yield p


if __name__ == '__main__':
    if len(sys.argv) > 1:
        queen_num = int(sys.argv[1])
        if queen_num < 4:
            print("Number of edges cannot less than 4")
   
    queen_pos = [-1] * queen_num
    for pos in find_queen(0, queen_pos):
        print_queen(pos)
