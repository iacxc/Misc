#!/usr/bin/python


__all__ = ('List', 'reversed', 'concat', 'uniq')


class Nil(object):
    @property
    def head(self):
        raise RuntimeError("Empty list")

    @property
    def tail(self):
        raise RuntimeError("Empty list")

    @property
    def first(self):
        raise RuntimeError("Empty list")

    @property
    def rest(self):
        raise RuntimeError("Empty list")

    @property
    def is_empty(self):
        return True

    def nth(self, idx):
        raise RuntimeError("Empty list")
        
    def __str__(self):
        return "()"


class Cons(object):
    def __init__(self, head, tail=None):
        self.__head = head
	if tail is None:
            self.__tail = Nil()
        else:
            self.__tail = tail

    @property
    def head(self):
        return self.__head

    @property
    def tail(self):
        return self.__tail

    @property
    def first(self):
        return self.__head
 
    @property
    def rest(self):
        return self.__tail

    @property
    def is_empty(self):
        return False

    def nth(self, idx):
        if idx < 1:
            raise RuntimeError("index must be positive")

        if idx == 1:
            return self.head
        else:
            return self.tail.nth(idx-1)

    def __str__(self):
        return "({0} {1})".format(self.head, self.tail)


def List(*items):
    """ create a list """
    if not items:
        return Nil()
    else:
        return Cons(items[0], List(*items[1:]))


def reversed(alist):
    """ reverse a list """
    def rec(lst, acc):
        if lst.is_empty:
            return acc
        else:
            return rec(lst.tail, Cons(lst.head, acc))

    return rec(alist, Nil())


def concat(list1, list2):
    """ concat two lists """
    if list1.is_empty:
        return list2
    else:
        return Cons(list1.head, concat(list1.tail, list2))


def uniq(alist):
    """ remove adjcent repeated values """
    if alist.is_empty:
        return Nil()

    head = alist.head
    tail = alist.tail

    if not tail.is_empty and head == tail.head:
        return uniq(Cons(head, tail.tail))
    else:
        return Cons(head, uniq(tail))


def flatten(alist):
    """ flatten a list into a tuple """
    if alist.is_empty:
        return ()

    head = alist.head

    if not isinstance(head, (Nil, Cons)):
        l = (head, )
    else:
        l = flatten(head)

    return l + flatten(alist.tail)


if __name__ == "__main__":
    l1 = List(1, 2, List("hello", "world"), "a", "b", List(List('aa', 'bb')))
    print l1
    print flatten(l1)
    print 'Third item:', l1.nth(3)
    print reversed(l1)

    l2 = List(1, 2, 3)
    l3 = List("a", "b")
    print concat(l2, l3)
    print concat(l3, l2)
    print reversed(concat(l2, l3))


    l4 = List(1, 1, 1, 3, 4, 5, 5, 5, 6, 6, 9, 10, 10)
    l5 = List(1, 3, 4, 5, 5, 5, 6, 6, 9, 10, 10, 13)
    print l4
    print uniq(l4)
    print l5
    print uniq(l5)

