"""
   List for python3
"""

__all__ = ('List', )


def cons(head, tail):
    return List(head, *tail)


class List(list):
    def __init__(self, *args):
        super().__init__(args)

    @property
    def is_empty(self):
        return not self

    @property
    def head(self):
        if self.is_empty:
            raise RuntimeError("Empty list")

        return self[0]

    @property
    def tail(self):
        if self.is_empty:
            raise RuntimeError("Empty list")

        _, *tail = self
        return List(*tail)

    @property
    def first(self):
        return self.head

    @property
    def rest(self):
        return self.tail

    def __add__(self, list2):
        """ concat two lists """
        if self.is_empty:
            return List(*list2)
        else:
            return cons(self.head, self.tail + list2)

    def flatten(self):
        """ return a flattened list """
        if self.is_empty:
            return self

        head = self.head
        if not isinstance(self.head, List):
            l = List(head)
        else:
            l = head.flatten()

        return l + self.tail.flatten()

    def reverse(self):
        """ return a reversed list """
        def rec(lst, acc):
            if lst.is_empty:
                return acc
            else:
                return rec(lst.tail, cons(lst.head, acc))

        return rec(self, List())


if __name__ == "__main__":
    l2 = List(1, 2, 3)
    l3 = List("a", "b")
    v = l2 + [4,5]
    print(type(v), v, v.reverse())
    print(l3 + l2)

    l6 = List()
    print("Empty list:", l6)
    v = l6 + [4,5]
    print(type(v), v)

    l1 = List(1, 2, List("hello", "world"), "a", "b", List(List('aa', 'bb')))
    print(l1)
    print("Head:", l1.head, "\nTail:", l1.tail)
    print('Third item:', l1[2])
    print(l1.flatten())
    print(l1.reverse())
    print(l1.reverse().flatten())
    print(l1.flatten().reverse())


