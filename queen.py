#!/usr/bin/python -O


def print_queen(pos):
    for col in pos:
        print "| " * col + "|X|" + " |" * (len(pos)-col-1)

    print


def find_queen(start_row, pos):
    if start_row == len(pos):
        yield pos

    for col in range(len(pos)):
        good_pos = True
        for row in range(start_row):
            if pos[row] in (col, col - start_row + row, col + start_row - row):
                good_pos = False
                break

        if good_pos:
            pos[start_row] = col
            for p in find_queen(start_row+1, pos):
                yield p


queen_pos = [-1] * 8
for pos in find_queen(0, queen_pos):
    print_queen(pos)
