import pytest
from collections import deque
from thespian.system.utilis import *


@pytest.mark.parametrize("itertype", [list, deque])
class TestPartition(object):

    def test_partition_list_none(self, itertype):
        t = itertype(range(10))
        a, b = partition(lambda x: False, t, itertype)
        assert t == b
        assert not a

    def test_partition_list_all(self, itertype):
        t = itertype(range(10))
        a, b = partition(lambda x: True, t, itertype)
        assert t == a
        assert not b

    def test_partition_list_half(self, itertype):
        t = itertype(range(10))
        a, b = partition(lambda x: x < 5, t, itertype)
        assert a == itertype(range(5))
        assert b == itertype(range(5, 10))

    def test_partition_list_odd_even(self, itertype):
        t = itertype(range(10))
        a, b = partition(lambda x: x % 2, t, itertype)
        assert a == itertype([1, 3, 5, 7, 9])
        assert b == itertype([0, 2, 4, 6, 8])

